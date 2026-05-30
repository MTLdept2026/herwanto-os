import asyncio
import base64
import json
import os
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

import dropbox_service
import google_services
import postgres_storage
import web_app


class _ChunkedUpload:
    def __init__(self, chunks):
        self._chunks = list(chunks)
        self.reads = 0

    async def read(self, _size):
        self.reads += 1
        if self._chunks:
            return self._chunks.pop(0)
        return b""


class _FakeRedis:
    def __init__(self):
        self.entries = []
        self.expired = False

    def eval(self, _script, _numkeys, _key, now, window, max_requests, member):
        cutoff = float(now) - int(window)
        self.entries = [(item, score) for item, score in self.entries if score > cutoff]
        if len(self.entries) >= int(max_requests):
            self.expired = True
            return 0
        self.entries.append((member, float(now)))
        self.expired = True
        return 1


class _DropboxTokenResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"access_token": "dropbox-token", "expires_in": 3600}


class CodeReviewFixesTests(unittest.TestCase):
    def test_upload_reader_stops_when_limit_is_exceeded(self):
        upload = _ChunkedUpload([b"aaaa", b"bbbb", b"cccc", b"dddd"])

        with self.assertRaises(HTTPException) as raised:
            asyncio.run(web_app._read_upload_bytes(upload, max_bytes=8))

        self.assertEqual(raised.exception.status_code, 413)
        self.assertEqual(upload.reads, 3)

    def test_redis_rate_limiter_allows_exact_limit_only(self):
        limiter = web_app._SlidingWindowRateLimiter("unit", max_requests=2, window_seconds=60)
        redis = _FakeRedis()

        self.assertTrue(limiter._redis_is_allowed(redis, "client"))
        self.assertTrue(limiter._redis_is_allowed(redis, "client"))
        self.assertFalse(limiter._redis_is_allowed(redis, "client"))
        self.assertEqual(len(redis.entries), 2)
        self.assertTrue(redis.expired)

    def test_cookie_secure_only_trusts_forwarded_proto_when_enabled(self):
        request = SimpleNamespace(
            headers={"x-forwarded-proto": "https"},
            url=SimpleNamespace(scheme="http"),
        )
        env = {
            "HIRA_TRUST_PROXY_HEADERS": "",
            "RAILWAY_ENVIRONMENT": "",
            "RAILWAY_SERVICE_NAME": "",
        }
        with patch.dict(os.environ, env, clear=False):
            self.assertFalse(web_app._cookie_secure_for_request(request))

        with patch.dict(os.environ, {**env, "HIRA_TRUST_PROXY_HEADERS": "1"}, clear=False):
            self.assertTrue(web_app._cookie_secure_for_request(request))

    def test_service_account_info_validates_required_fields(self):
        info = {
            "type": "service_account",
            "project_id": "project",
            "private_key": "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----\n",
            "client_email": "svc@example.iam.gserviceaccount.com",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        raw = base64.b64encode(json.dumps(info).encode("utf-8")).decode("ascii")

        self.assertEqual(google_services._service_account_info(raw)["client_email"], info["client_email"])

        bad = base64.b64encode(json.dumps({"type": "service_account"}).encode("utf-8")).decode("ascii")
        with self.assertRaisesRegex(EnvironmentError, "missing required field"):
            google_services._service_account_info(bad)

    def test_initialized_marker_is_written_once_per_process(self):
        class FakeCursor:
            def __init__(self):
                self.calls = 0

            def execute(self, *_args):
                self.calls += 1

        postgres_storage._initialized_markers_written.discard("unit_initialized")
        cur = FakeCursor()

        postgres_storage._mark_initialized(cur, "unit_initialized")
        postgres_storage._mark_initialized(cur, "unit_initialized")

        self.assertEqual(cur.calls, 1)
        postgres_storage._initialized_markers_written.discard("unit_initialized")

    def test_dropbox_token_cache_refreshes_once_for_concurrent_callers(self):
        env = {
            "DROPBOX_APP_KEY": "app-key",
            "DROPBOX_APP_SECRET": "app-secret",
            "DROPBOX_REFRESH_TOKEN": "refresh-token",
        }
        calls = []

        def post(*_args, **_kwargs):
            calls.append(1)
            time.sleep(0.02)
            return _DropboxTokenResponse()

        dropbox_service._TOKEN_CACHE.clear()
        with patch.dict(os.environ, env, clear=False), patch.object(dropbox_service.requests, "post", side_effect=post):
            with ThreadPoolExecutor(max_workers=4) as executor:
                tokens = list(executor.map(lambda _idx: dropbox_service._access_token(), range(4)))

        self.assertEqual(tokens, ["dropbox-token"] * 4)
        self.assertEqual(len(calls), 1)
        dropbox_service._TOKEN_CACHE.clear()

    def test_dropbox_title_cache_is_bounded_lru(self):
        dropbox_service._TITLE_CACHE.clear()
        with patch.object(dropbox_service, "TITLE_CACHE_MAX", 2):
            dropbox_service._title_cache_set("/a.html", "A")
            dropbox_service._title_cache_set("/b.html", "B")
            self.assertEqual(dropbox_service._title_cache_get("/a.html"), "A")
            dropbox_service._title_cache_set("/c.html", "C")

        self.assertEqual(set(dropbox_service._TITLE_CACHE.keys()), {"/a.html", "/c.html"})
        dropbox_service._TITLE_CACHE.clear()


if __name__ == "__main__":
    unittest.main()
