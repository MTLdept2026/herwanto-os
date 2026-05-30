import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

import web_app


class _ChunkedUpload:
    def __init__(self, chunks, filename="voice.webm", content_type="audio/webm", size=None):
        self._chunks = list(chunks)
        self.filename = filename
        self.content_type = content_type
        if size is not None:
            self.size = size

    async def read(self, _size):
        if self._chunks:
            return self._chunks.pop(0)
        return b""


class _FakeSpeech:
    def __init__(self, payload=b"audio-bytes"):
        self.payload = payload
        self.kwargs = None

    async def create(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(content=self.payload)


class _FakeOpenAIClient:
    def __init__(self, speech):
        self.audio = SimpleNamespace(speech=speech)


def _request():
    return SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))


async def _response_body(response):
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk)
    return b"".join(chunks)


class VoiceTierATests(unittest.TestCase):
    def test_security_headers_allow_blob_audio_for_tts_playback(self):
        response = web_app._apply_security_headers(web_app.JSONResponse({"ok": True}))
        csp = response.headers["Content-Security-Policy"]
        self.assertIn("media-src 'self' blob:", csp)

    def test_tts_enforces_token_and_streams_audio(self):
        speech = _FakeSpeech()
        with patch.object(web_app, "_require_token") as require_token, \
             patch.object(web_app._TTS_RATE_LIMITER, "is_allowed", new=AsyncMock(return_value=True)), \
             patch.object(web_app.bot, "async_openai_client", _FakeOpenAIClient(speech)), \
             patch.dict(web_app.os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
            response = asyncio.run(web_app.text_to_speech(_request(), web_app.TTSRequest(text="hello"), x_hira_token="token"))
            body = asyncio.run(_response_body(response))

        require_token.assert_called_once_with("token")
        self.assertEqual(response.media_type, "audio/mpeg")
        self.assertEqual(body, b"audio-bytes")
        self.assertEqual(speech.kwargs["model"], "gpt-4o-mini-tts")
        self.assertEqual(speech.kwargs["input"], "hello")

    def test_tts_rejects_empty_text_missing_key_and_rate_limit(self):
        with patch.object(web_app, "_require_token"), \
             patch.object(web_app._TTS_RATE_LIMITER, "is_allowed", new=AsyncMock(return_value=True)), \
             patch.dict(web_app.os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(web_app.text_to_speech(_request(), web_app.TTSRequest(text="   "), x_hira_token="token"))
        self.assertEqual(raised.exception.status_code, 400)

        with patch.object(web_app, "_require_token"), \
             patch.object(web_app._TTS_RATE_LIMITER, "is_allowed", new=AsyncMock(return_value=True)), \
             patch.dict(web_app.os.environ, {"OPENAI_API_KEY": ""}, clear=False):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(web_app.text_to_speech(_request(), web_app.TTSRequest(text="hello"), x_hira_token="token"))
        self.assertEqual(raised.exception.status_code, 400)

        with patch.object(web_app, "_require_token"), \
             patch.object(web_app._TTS_RATE_LIMITER, "is_allowed", new=AsyncMock(return_value=False)):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(web_app.text_to_speech(_request(), web_app.TTSRequest(text="hello"), x_hira_token="token"))
        self.assertEqual(raised.exception.status_code, 429)

    def test_tts_truncates_to_configured_cap(self):
        speech = _FakeSpeech()
        with patch.object(web_app, "_require_token"), \
             patch.object(web_app, "_TTS_MAX_CHARS", 5), \
             patch.object(web_app._TTS_RATE_LIMITER, "is_allowed", new=AsyncMock(return_value=True)), \
             patch.object(web_app.bot, "async_openai_client", _FakeOpenAIClient(speech)), \
             patch.dict(web_app.os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
            response = asyncio.run(web_app.text_to_speech(_request(), web_app.TTSRequest(text="123456789"), x_hira_token="token"))
            asyncio.run(_response_body(response))

        self.assertEqual(speech.kwargs["input"], "12345")

    def test_voice_transcribe_returns_text_without_chat_run(self):
        upload = _ChunkedUpload([b"voice"])
        with patch.object(web_app, "_require_token") as require_token, \
             patch.object(web_app._VOICE_RATE_LIMITER, "is_allowed", new=AsyncMock(return_value=True)), \
             patch.object(web_app, "_transcribe_audio_bytes", return_value="what is my next class") as transcribe, \
             patch.object(web_app.bot, "_run_agentic_chat", side_effect=AssertionError("chat should not run")), \
             patch.dict(web_app.os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
            result = asyncio.run(web_app.voice_transcribe(_request(), file=upload, x_hira_token="token"))

        require_token.assert_called_once_with("token")
        transcribe.assert_called_once()
        self.assertEqual(result, {"text": "what is my next class"})

    def test_voice_transcribe_oversized_upload_returns_413(self):
        upload = _ChunkedUpload([b"aaaa", b"bbbb"])
        with patch.object(web_app, "_require_token"), \
             patch.object(web_app, "_MAX_VOICE_BYTES", 4), \
             patch.object(web_app._VOICE_RATE_LIMITER, "is_allowed", new=AsyncMock(return_value=True)), \
             patch.object(web_app, "_transcribe_audio_bytes", side_effect=AssertionError("should not transcribe")), \
             patch.dict(web_app.os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(web_app.voice_transcribe(_request(), file=upload, x_hira_token="token"))

        self.assertEqual(raised.exception.status_code, 413)


if __name__ == "__main__":
    unittest.main()
