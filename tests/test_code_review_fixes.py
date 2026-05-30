import asyncio
import base64
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

import google_services
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


if __name__ == "__main__":
    unittest.main()
