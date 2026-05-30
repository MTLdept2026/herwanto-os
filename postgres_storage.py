from __future__ import annotations

import json
import logging
import os
import threading
from contextlib import contextmanager
from datetime import datetime

logger = logging.getLogger(__name__)

_schema_lock = threading.RLock()
_schema_ready = False
_pool_lock = threading.RLock()
_pool = None
_pool_url = ""


class PostgresUnavailable(RuntimeError):
    """Raised when Postgres is configured but cannot be used."""


def enabled() -> bool:
    if os.environ.get("HIRA_POSTGRES_ENABLED", "1").strip().lower() in {"0", "false", "no", "off"}:
        return False
    return bool(os.environ.get("DATABASE_URL", "").strip())


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise PostgresUnavailable("DATABASE_URL is not set")
    return url


def _psycopg_pool():
    try:
        from psycopg_pool import ConnectionPool
    except Exception as exc:
        raise PostgresUnavailable("psycopg_pool is not installed") from exc
    return ConnectionPool


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, str(default)) or default))
    except ValueError:
        return default


def _jsonb(value):
    try:
        from psycopg.types.json import Jsonb
    except Exception as exc:
        raise PostgresUnavailable("psycopg JSON support is unavailable") from exc
    return Jsonb(value)


def _advisory_xact_lock(cur, name: str) -> None:
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (name,))


def _sync_notification_sequence(cur) -> None:
    cur.execute(
        """
        SELECT setval(
            'notification_state_id_seq',
            GREATEST(
                COALESCE((SELECT MAX(id::bigint) FROM notification_state WHERE id ~ '^[0-9]+$'), 0),
                COALESCE((SELECT last_value FROM notification_state_id_seq), 0)
            ),
            true
        )
        """
    )


def _connection_pool():
    global _pool, _pool_url
    url = _database_url()
    with _pool_lock:
        if _pool is not None and _pool_url == url:
            return _pool
        if _pool is not None:
            try:
                _pool.close()
            except Exception:
                pass
            _pool = None
            _pool_url = ""
        ConnectionPool = _psycopg_pool()
        max_size = _env_int("HIRA_POSTGRES_POOL_MAX_SIZE", 8)
        min_size = min(_env_int("HIRA_POSTGRES_POOL_MIN_SIZE", 1, minimum=0), max_size)
        pool = ConnectionPool(
            conninfo=url,
            kwargs={"connect_timeout": 5},
            min_size=min_size,
            max_size=max_size,
            open=True,
        )
        pool.wait(timeout=5)
        _pool = pool
        _pool_url = url
        return _pool


@contextmanager
def connect():
    pool = _connection_pool()
    with pool.connection() as conn:
        yield conn


def ensure_schema() -> None:
    global _schema_ready
    if not enabled():
        raise PostgresUnavailable("Postgres storage is disabled")
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_config (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS assistant_memory (
                        category TEXT PRIMARY KEY,
                        items JSONB NOT NULL DEFAULT '[]'::jsonb,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS memory_log (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        category TEXT NOT NULL,
                        text TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'postgres'
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS web_push_subscriptions (
                        client_id TEXT PRIMARY KEY,
                        endpoint TEXT NOT NULL UNIQUE,
                        subscription JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
                        display_mode TEXT NOT NULL DEFAULT 'unknown',
                        app_version TEXT NOT NULL DEFAULT '',
                        user_agent TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS web_push_delivery_log (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        source TEXT NOT NULL DEFAULT '',
                        kind TEXT NOT NULL DEFAULT '',
                        title TEXT NOT NULL DEFAULT '',
                        attempted INTEGER NOT NULL DEFAULT 0,
                        sent INTEGER NOT NULL DEFAULT 0,
                        expired INTEGER NOT NULL DEFAULT 0,
                        errors JSONB NOT NULL DEFAULT '{}'::jsonb,
                        last_error TEXT NOT NULL DEFAULT '',
                        payload_bytes INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS notification_state (
                        id TEXT PRIMARY KEY,
                        kind TEXT NOT NULL DEFAULT 'notice',
                        title TEXT NOT NULL DEFAULT 'H.I.R.A',
                        body TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT '',
                        seen_by JSONB NOT NULL DEFAULT '[]'::jsonb,
                        archived BOOLEAN NOT NULL DEFAULT false,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute("CREATE SEQUENCE IF NOT EXISTS notification_state_id_seq")
                _sync_notification_sequence(cur)
            conn.commit()
        _schema_ready = True


def get_config(key: str) -> str | None:
    ensure_schema()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app_config WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None


def set_config(key: str, value: str) -> None:
    ensure_schema()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_config (key, value, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                """,
                (key, value),
            )
        conn.commit()


def _coerce_items(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def load_memory(default_memory: dict[str, list]) -> tuple[dict[str, list[str]], bool]:
    ensure_schema()
    memory = {key: list(value) for key, value in default_memory.items()}
    initialized = False
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT category, items FROM assistant_memory")
            rows = cur.fetchall()
            initialized = bool(rows)
            for category, items in rows:
                if category in memory:
                    memory[category] = _coerce_items(items)
            cur.execute("SELECT value FROM app_config WHERE key = 'assistant_memory_initialized'")
            row = cur.fetchone()
            initialized = initialized or (row and str(row[0]).strip() == "1")
    return memory, bool(initialized)


def set_memory(memory: dict[str, list], default_memory: dict[str, list]) -> dict[str, list[str]]:
    ensure_schema()
    clean = {}
    for category in default_memory:
        clean[category] = [str(item).strip() for item in memory.get(category, []) if str(item).strip()]
    with connect() as conn:
        with conn.cursor() as cur:
            for category, items in clean.items():
                cur.execute(
                    """
                    INSERT INTO assistant_memory (category, items, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (category)
                    DO UPDATE SET items = EXCLUDED.items, updated_at = now()
                    """,
                    (category, _jsonb(items)),
                )
            cur.execute(
                """
                INSERT INTO app_config (key, value, updated_at)
                VALUES ('assistant_memory_initialized', '1', now())
                ON CONFLICT (key)
                DO UPDATE SET value = '1', updated_at = now()
                """
            )
        conn.commit()
    return clean


def add_memory(category: str, text: str, default_memory: dict[str, list]) -> dict[str, list[str]]:
    ensure_schema()
    item = str(text or "").strip()
    memory = {key: list(value) for key, value in default_memory.items()}
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT category, items FROM assistant_memory FOR UPDATE")
                for stored_category, stored_items in cur.fetchall():
                    if stored_category in memory:
                        memory[stored_category] = _coerce_items(stored_items)
                bucket = memory.setdefault(category, [])
                if item and item not in bucket:
                    bucket.append(item)
                    cur.execute(
                        "INSERT INTO memory_log (created_at, category, text, source) VALUES (now(), %s, %s, 'postgres')",
                        (category, item),
                    )
                cur.execute(
                    """
                    INSERT INTO assistant_memory (category, items, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (category)
                    DO UPDATE SET items = EXCLUDED.items, updated_at = now()
                    """,
                    (category, _jsonb(bucket)),
                )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('assistant_memory_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )
    return {key: list(memory.get(key, [])) for key in default_memory}


def import_memory(memory: dict[str, list], default_memory: dict[str, list], source: str = "sheets_import") -> dict:
    ensure_schema()
    imported = 0
    clean_source = str(source or "import").strip()[:80]
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT category, items FROM assistant_memory FOR UPDATE")
                current = {key: list(value) for key, value in default_memory.items()}
                for category, items in cur.fetchall():
                    if category in current:
                        current[category] = _coerce_items(items)
                for category in default_memory:
                    bucket = current[category]
                    for item in memory.get(category, []) if isinstance(memory, dict) else []:
                        text = str(item).strip()
                        if text and text not in bucket:
                            bucket.append(text)
                            imported += 1
                            cur.execute(
                                "INSERT INTO memory_log (created_at, category, text, source) VALUES (now(), %s, %s, %s)",
                                (category, text, clean_source),
                            )
                    cur.execute(
                        """
                        INSERT INTO assistant_memory (category, items, updated_at)
                        VALUES (%s, %s, now())
                        ON CONFLICT (category)
                        DO UPDATE SET items = EXCLUDED.items, updated_at = now()
                        """,
                        (category, _jsonb(bucket)),
                    )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('assistant_memory_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )
    return {"ok": True, "imported": imported, "source": clean_source, "at": datetime.utcnow().isoformat() + "Z"}


def load_web_push_subscriptions() -> list[dict]:
    ensure_schema()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT client_id, subscription, created_at, last_seen, display_mode, app_version, user_agent
                FROM web_push_subscriptions
                ORDER BY last_seen ASC
                """
            )
            rows = cur.fetchall()
    return [
        {
            "client_id": client_id,
            "subscription": subscription,
            "created": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or ""),
            "last_seen": last_seen.isoformat() if hasattr(last_seen, "isoformat") else str(last_seen or ""),
            "display_mode": display_mode or "unknown",
            "app_version": app_version or "",
            "user_agent": user_agent or "",
        }
        for client_id, subscription, created_at, last_seen, display_mode, app_version, user_agent in rows
    ]


def upsert_web_push_subscription(item: dict) -> None:
    ensure_schema()
    if not isinstance(item, dict):
        return
    client_id = str(item.get("client_id", "")).strip()
    subscription = item.get("subscription") if isinstance(item.get("subscription"), dict) else {}
    endpoint = str(subscription.get("endpoint", "")).strip()
    if not client_id or not endpoint:
        return
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM web_push_subscriptions WHERE client_id = %s OR endpoint = %s",
                    (client_id, endpoint),
                )
                cur.execute(
                    """
                    INSERT INTO web_push_subscriptions (
                        client_id, endpoint, subscription, created_at, last_seen,
                        display_mode, app_version, user_agent
                    )
                    VALUES (
                        %s, %s, %s,
                        COALESCE(NULLIF(%s, '')::timestamptz, now()),
                        COALESCE(NULLIF(%s, '')::timestamptz, now()),
                        %s, %s, %s
                    )
                    """,
                    (
                        client_id,
                        endpoint,
                        _jsonb(subscription),
                        str(item.get("created", "") or ""),
                        str(item.get("last_seen", "") or ""),
                        str(item.get("display_mode", "") or "unknown"),
                        str(item.get("app_version", "") or ""),
                        str(item.get("user_agent", "") or "")[:180],
                    ),
                )
                cur.execute(
                    """
                    DELETE FROM web_push_subscriptions
                    WHERE client_id NOT IN (
                        SELECT client_id
                        FROM web_push_subscriptions
                        ORDER BY last_seen DESC
                        LIMIT 30
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('web_push_subscriptions_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )


def delete_web_push_subscriptions(client_ids: list[str] | None = None, endpoints: list[str] | None = None) -> int:
    ensure_schema()
    clean_ids = [str(item).strip() for item in (client_ids or []) if str(item).strip()]
    clean_endpoints = [str(item).strip() for item in (endpoints or []) if str(item).strip()]
    if not clean_ids and not clean_endpoints:
        return 0
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM web_push_subscriptions
                    WHERE client_id = ANY(%s::text[]) OR endpoint = ANY(%s::text[])
                    """,
                    (clean_ids, clean_endpoints),
                )
                deleted = cur.rowcount or 0
    return int(deleted)


def set_web_push_subscriptions(subscriptions: list[dict]) -> None:
    ensure_schema()
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                client_ids = []
                for item in subscriptions:
                    if not isinstance(item, dict):
                        continue
                    client_id = str(item.get("client_id", "")).strip()
                    subscription = item.get("subscription") if isinstance(item.get("subscription"), dict) else {}
                    endpoint = str(subscription.get("endpoint", "")).strip()
                    if not client_id or not endpoint:
                        continue
                    client_ids.append(client_id)
                    cur.execute(
                        "DELETE FROM web_push_subscriptions WHERE client_id = %s OR endpoint = %s",
                        (client_id, endpoint),
                    )
                    cur.execute(
                        """
                        INSERT INTO web_push_subscriptions (
                            client_id, endpoint, subscription, created_at, last_seen,
                            display_mode, app_version, user_agent
                        )
                        VALUES (
                            %s, %s, %s,
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            %s, %s, %s
                        )
                        """,
                        (
                            client_id,
                            endpoint,
                            _jsonb(subscription),
                            str(item.get("created", "") or ""),
                            str(item.get("last_seen", "") or ""),
                            str(item.get("display_mode", "") or "unknown"),
                            str(item.get("app_version", "") or ""),
                            str(item.get("user_agent", "") or "")[:180],
                        ),
                    )
                if client_ids:
                    cur.execute("DELETE FROM web_push_subscriptions WHERE NOT (client_id = ANY(%s))", (client_ids,))
                else:
                    cur.execute("DELETE FROM web_push_subscriptions")
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('web_push_subscriptions_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )


def import_web_push_subscriptions(subscriptions: list[dict]) -> None:
    ensure_schema()
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                _advisory_xact_lock(cur, "hira:web_push_subscriptions:migration")
                for item in list(subscriptions or [])[-30:]:
                    if not isinstance(item, dict):
                        continue
                    client_id = str(item.get("client_id", "")).strip()
                    subscription = item.get("subscription") if isinstance(item.get("subscription"), dict) else {}
                    endpoint = str(subscription.get("endpoint", "")).strip()
                    if not client_id or not endpoint:
                        continue
                    cur.execute(
                        "DELETE FROM web_push_subscriptions WHERE endpoint = %s AND client_id <> %s",
                        (endpoint, client_id),
                    )
                    cur.execute(
                        """
                        INSERT INTO web_push_subscriptions (
                            client_id, endpoint, subscription, created_at, last_seen,
                            display_mode, app_version, user_agent
                        )
                        VALUES (
                            %s, %s, %s,
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            %s, %s, %s
                        )
                        ON CONFLICT (client_id)
                        DO UPDATE SET
                            endpoint = EXCLUDED.endpoint,
                            subscription = EXCLUDED.subscription,
                            last_seen = GREATEST(web_push_subscriptions.last_seen, EXCLUDED.last_seen),
                            display_mode = EXCLUDED.display_mode,
                            app_version = EXCLUDED.app_version,
                            user_agent = EXCLUDED.user_agent
                        """,
                        (
                            client_id,
                            endpoint,
                            _jsonb(subscription),
                            str(item.get("created", "") or ""),
                            str(item.get("last_seen", "") or ""),
                            str(item.get("display_mode", "") or "unknown"),
                            str(item.get("app_version", "") or ""),
                            str(item.get("user_agent", "") or "")[:180],
                        ),
                    )
                cur.execute(
                    """
                    DELETE FROM web_push_subscriptions
                    WHERE client_id NOT IN (
                        SELECT client_id
                        FROM web_push_subscriptions
                        ORDER BY last_seen DESC
                        LIMIT 30
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('web_push_subscriptions_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )


def load_web_push_delivery_log(limit: int = 80) -> list[dict]:
    ensure_schema()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT created_at, source, kind, title, attempted, sent, expired, errors, last_error, payload_bytes
                FROM web_push_delivery_log
                ORDER BY id DESC
                LIMIT %s
                """,
                (max(1, int(limit or 80)),),
            )
            rows = list(reversed(cur.fetchall()))
    return [
        {
            "created": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or ""),
            "source": source or "",
            "kind": kind or "",
            "title": title or "",
            "attempted": int(attempted or 0),
            "sent": int(sent or 0),
            "expired": int(expired or 0),
            "errors": errors if isinstance(errors, dict) else {},
            "last_error": last_error or "",
            "payload_bytes": int(payload_bytes or 0),
        }
        for created_at, source, kind, title, attempted, sent, expired, errors, last_error, payload_bytes in rows
    ]


def set_web_push_delivery_log(entries: list[dict]) -> None:
    ensure_schema()
    kept = list(entries or [])[-80:]
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("DELETE FROM web_push_delivery_log")
                for item in kept:
                    if not isinstance(item, dict):
                        continue
                    cur.execute(
                        """
                        INSERT INTO web_push_delivery_log (
                            created_at, source, kind, title, attempted, sent, expired,
                            errors, last_error, payload_bytes
                        )
                        VALUES (
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            str(item.get("created", "") or ""),
                            str(item.get("source", "") or "")[:240],
                            str(item.get("kind", "") or "")[:40],
                            str(item.get("title", "") or "")[:240],
                            int(item.get("attempted", 0) or 0),
                            int(item.get("sent", 0) or 0),
                            int(item.get("expired", 0) or 0),
                            _jsonb(item.get("errors", {}) if isinstance(item.get("errors"), dict) else {}),
                            str(item.get("last_error", "") or "")[:300],
                            int(item.get("payload_bytes", 0) or 0),
                        ),
                    )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('web_push_delivery_log_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )


def import_web_push_delivery_log(entries: list[dict]) -> None:
    ensure_schema()
    kept = list(entries or [])[-80:]
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                _advisory_xact_lock(cur, "hira:web_push_delivery_log:migration")
                for item in kept:
                    if not isinstance(item, dict):
                        continue
                    cur.execute(
                        """
                        INSERT INTO web_push_delivery_log (
                            created_at, source, kind, title, attempted, sent, expired,
                            errors, last_error, payload_bytes
                        )
                        VALUES (
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            str(item.get("created", "") or ""),
                            str(item.get("source", "") or "")[:240],
                            str(item.get("kind", "") or "")[:40],
                            str(item.get("title", "") or "")[:240],
                            int(item.get("attempted", 0) or 0),
                            int(item.get("sent", 0) or 0),
                            int(item.get("expired", 0) or 0),
                            _jsonb(item.get("errors", {}) if isinstance(item.get("errors"), dict) else {}),
                            str(item.get("last_error", "") or "")[:300],
                            int(item.get("payload_bytes", 0) or 0),
                        ),
                    )
                cur.execute(
                    """
                    DELETE FROM web_push_delivery_log
                    WHERE id NOT IN (
                        SELECT id
                        FROM web_push_delivery_log
                        ORDER BY id DESC
                        LIMIT 80
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('web_push_delivery_log_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )


def append_web_push_delivery_log(item: dict) -> None:
    ensure_schema()
    if not isinstance(item, dict):
        return
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO web_push_delivery_log (
                        created_at, source, kind, title, attempted, sent, expired,
                        errors, last_error, payload_bytes
                    )
                    VALUES (
                        COALESCE(NULLIF(%s, '')::timestamptz, now()),
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        str(item.get("created", "") or ""),
                        str(item.get("source", "") or "")[:240],
                        str(item.get("kind", "") or "")[:40],
                        str(item.get("title", "") or "")[:240],
                        int(item.get("attempted", 0) or 0),
                        int(item.get("sent", 0) or 0),
                        int(item.get("expired", 0) or 0),
                        _jsonb(item.get("errors", {}) if isinstance(item.get("errors"), dict) else {}),
                        str(item.get("last_error", "") or "")[:300],
                        int(item.get("payload_bytes", 0) or 0),
                    ),
                )
                cur.execute(
                    """
                    DELETE FROM web_push_delivery_log
                    WHERE id NOT IN (
                        SELECT id
                        FROM web_push_delivery_log
                        ORDER BY id DESC
                        LIMIT 80
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('web_push_delivery_log_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )


def load_app_notifications(include_archived: bool = False) -> list[dict]:
    ensure_schema()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, kind, title, body, created_at, source, seen_by, archived
                FROM notification_state
                WHERE (%s OR archived = false)
                ORDER BY created_at ASC
                """,
                (include_archived,),
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(notification_id),
            "kind": kind or "notice",
            "title": title or "H.I.R.A",
            "body": body or "",
            "created": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or ""),
            "source": source or "",
            "seen_by": seen_by if isinstance(seen_by, list) else [],
            "archived": bool(archived),
        }
        for notification_id, kind, title, body, created_at, source, seen_by, archived in rows
    ]


def enqueue_app_notification(kind: str, title: str, body: str, source: str = "") -> dict:
    ensure_schema()
    clean_kind = str(kind or "notice").strip()[:40] or "notice"
    clean_title = str(title or "H.I.R.A").strip()[:240] or "H.I.R.A"
    clean_body = str(body or "").strip()
    clean_source = str(source or "").strip()[:240]
    if not clean_body:
        return {
            "id": "",
            "kind": clean_kind,
            "title": clean_title,
            "body": clean_body,
            "created": datetime.utcnow().isoformat() + "Z",
            "source": clean_source,
            "seen_by": [],
            "archived": False,
        }
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                if clean_source:
                    cur.execute(
                        """
                        SELECT id
                        FROM notification_state
                        WHERE archived = false AND source = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (clean_source,),
                    )
                    row = cur.fetchone()
                    if row:
                        notification_id = str(row[0])
                        cur.execute(
                            """
                            UPDATE notification_state
                            SET kind = %s, title = %s, body = %s, created_at = now(), updated_at = now()
                            WHERE id = %s
                            RETURNING id, kind, title, body, created_at, source, seen_by, archived
                            """,
                            (clean_kind, clean_title, clean_body, notification_id),
                        )
                        return _notification_row_to_dict(cur.fetchone(), duplicate=True)
                else:
                    cur.execute(
                        """
                        SELECT id, kind, title, body, created_at, source, seen_by, archived
                        FROM notification_state
                        WHERE archived = false AND kind = %s AND title = %s AND body = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (clean_kind, clean_title, clean_body),
                    )
                    row = cur.fetchone()
                    if row:
                        return _notification_row_to_dict(row, duplicate=True)
                cur.execute("SELECT nextval('notification_state_id_seq')")
                notification_id = str(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO notification_state (
                        id, kind, title, body, source, seen_by, archived, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, '[]'::jsonb, false, now(), now())
                    RETURNING id, kind, title, body, created_at, source, seen_by, archived
                    """,
                    (notification_id, clean_kind, clean_title, clean_body, clean_source),
                )
                inserted = cur.fetchone()
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('notification_state_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )
                return _notification_row_to_dict(inserted)


def _notification_row_to_dict(row, duplicate: bool = False) -> dict:
    notification_id, kind, title, body, created_at, source, seen_by, archived = row
    item = {
        "id": str(notification_id),
        "kind": kind or "notice",
        "title": title or "H.I.R.A",
        "body": body or "",
        "created": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or ""),
        "source": source or "",
        "seen_by": seen_by if isinstance(seen_by, list) else [],
        "archived": bool(archived),
    }
    if duplicate:
        item["_duplicate"] = True
    return item


def mark_app_notifications_seen(client_id: str, notification_ids: list[str]) -> int:
    ensure_schema()
    clean_client = str(client_id or "default").strip() or "default"
    ids = [str(item_id).strip() for item_id in notification_ids if str(item_id).strip()]
    if not ids:
        return 0
    changed = 0
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, seen_by
                    FROM notification_state
                    WHERE id = ANY(%s::text[])
                    FOR UPDATE
                    """,
                    (ids,),
                )
                for notification_id, seen_by in cur.fetchall():
                    current = seen_by if isinstance(seen_by, list) else []
                    if clean_client in current:
                        continue
                    current.append(clean_client)
                    cur.execute(
                        """
                        UPDATE notification_state
                        SET seen_by = %s, updated_at = now()
                        WHERE id = %s
                        """,
                        (_jsonb(current[-20:]), notification_id),
                    )
                    changed += 1
    return changed


def archive_app_notifications(notification_ids: list[str]) -> int:
    ensure_schema()
    ids = [str(item_id).strip() for item_id in notification_ids if str(item_id).strip()]
    if not ids:
        return 0
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE notification_state
                    SET archived = true, updated_at = now()
                    WHERE id = ANY(%s::text[]) AND archived = false
                    """,
                    (ids,),
                )
                changed = cur.rowcount or 0
    return int(changed)


def set_app_notifications(notifications: list[dict]) -> None:
    ensure_schema()
    kept = list(notifications or [])[-80:]
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("DELETE FROM notification_state")
                for item in kept:
                    if not isinstance(item, dict):
                        continue
                    notification_id = str(item.get("id", "")).strip()
                    body = str(item.get("body", "") or "").strip()
                    if not notification_id or not body:
                        continue
                    cur.execute(
                        """
                        INSERT INTO notification_state (
                            id, kind, title, body, source, seen_by, archived,
                            created_at, updated_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            now()
                        )
                        """,
                        (
                            notification_id,
                            str(item.get("kind", "") or "notice")[:40],
                            str(item.get("title", "") or "H.I.R.A")[:240],
                            body,
                            str(item.get("source", "") or "")[:240],
                            _jsonb(item.get("seen_by", []) if isinstance(item.get("seen_by"), list) else []),
                            bool(item.get("archived", False)),
                            str(item.get("created", "") or ""),
                        ),
                    )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('notification_state_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                        """
                    )
                _sync_notification_sequence(cur)


def import_app_notifications(notifications: list[dict]) -> None:
    ensure_schema()
    kept = list(notifications or [])[-80:]
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                _advisory_xact_lock(cur, "hira:notification_state:migration")
                for item in kept:
                    if not isinstance(item, dict):
                        continue
                    notification_id = str(item.get("id", "")).strip()
                    body = str(item.get("body", "") or "").strip()
                    if not notification_id or not body:
                        continue
                    cur.execute(
                        """
                        INSERT INTO notification_state (
                            id, kind, title, body, source, seen_by, archived,
                            created_at, updated_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            COALESCE(NULLIF(%s, '')::timestamptz, now()),
                            now()
                        )
                        ON CONFLICT (id)
                        DO UPDATE SET
                            kind = EXCLUDED.kind,
                            title = EXCLUDED.title,
                            body = EXCLUDED.body,
                            source = EXCLUDED.source,
                            seen_by = EXCLUDED.seen_by,
                            archived = EXCLUDED.archived,
                            updated_at = now()
                        """,
                        (
                            notification_id,
                            str(item.get("kind", "") or "notice")[:40],
                            str(item.get("title", "") or "H.I.R.A")[:240],
                            body,
                            str(item.get("source", "") or "")[:240],
                            _jsonb(item.get("seen_by", []) if isinstance(item.get("seen_by"), list) else []),
                            bool(item.get("archived", False)),
                            str(item.get("created", "") or ""),
                        ),
                    )
                cur.execute(
                    """
                    DELETE FROM notification_state
                    WHERE id NOT IN (
                        SELECT id
                        FROM notification_state
                        ORDER BY created_at DESC
                        LIMIT 80
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO app_config (key, value, updated_at)
                    VALUES ('notification_state_initialized', '1', now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = '1', updated_at = now()
                    """
                )
                _sync_notification_sequence(cur)


def storage_status() -> dict:
    if not enabled():
        return {"enabled": False, "connected": False, "source": "sheets"}
    try:
        ensure_schema()
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"enabled": True, "connected": True, "source": "postgres"}
    except Exception as exc:
        logger.warning("Postgres storage unavailable: %s", exc)
        return {"enabled": True, "connected": False, "source": "sheets_fallback", "error": str(exc)}
