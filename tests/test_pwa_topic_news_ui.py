import contextlib
import json
import os
import threading
import unittest
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return


@contextlib.contextmanager
def _static_server():
    handler = lambda *args, **kwargs: _QuietHandler(*args, directory=str(REPO_ROOT), **kwargs)
    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    except PermissionError as exc:
        raise unittest.SkipTest(f"Local HTTP server is blocked in this sandbox: {exc}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _sse(*events):
    return "".join(f"data: {json.dumps(event)}\n\n" for event in events)


class PwaTopicNewsUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise unittest.SkipTest(f"Playwright is not installed: {exc}")
        cls._sync_playwright = staticmethod(sync_playwright)

    def _run_page(self, chat_body, prompt="How about Nothing, Teenage Engineering and Android stuff?"):
        with self._sync_playwright() as p:
            executable = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            launch_kwargs = {"headless": True}
            if os.path.exists(executable):
                launch_kwargs["executable_path"] = executable
            try:
                browser = p.chromium.launch(**launch_kwargs)
            except Exception as exc:
                raise unittest.SkipTest(f"Chromium is not available for UI tests: {exc}")
            try:
                page = browser.new_page(viewport={"width": 390, "height": 844})
                page.add_init_script(
                    """
                    localStorage.setItem("hira_session_unlocked", "1");
                    localStorage.setItem("hira_client_id", "ui-topic-news-test");
                    localStorage.setItem("hira_pwa_chat", "[]");
                    """
                )

                def route_api(route):
                    url = route.request.url
                    if "/api/chat" in url:
                        route.fulfill(
                            status=200,
                            headers={"content-type": "text/event-stream"},
                            body=chat_body,
                        )
                        return
                    if "/api/home" in url:
                        route.fulfill(
                            status=200,
                            content_type="application/json",
                            body=json.dumps({
                                "agenda_structured": {"days": []},
                                "daily_load": {"today": {"score": 0, "tone": "green"}, "days": []},
                                "tasks_structured": {"items": []},
                                "marking": {"tasks": []},
                                "connections": {},
                                "notifications": [],
                                "sync_timings": [],
                            }),
                        )
                        return
                    route.fulfill(status=200, content_type="application/json", body="{}")

                page.route("**/api/**", route_api)
                page.goto(f"{self.base_url}/pwa/index.html", wait_until="domcontentloaded")
                page.locator("#messageInput").fill(prompt)
                page.locator("#chatForm").evaluate("form => form.requestSubmit()")
                page.locator("#messages").get_by_text("Nothing OS update reaches Phone users").wait_for(timeout=6000)
                return page.locator("#messages").inner_text()
            finally:
                browser.close()

    def test_topic_news_sse_renders_in_chat_without_backend_snag(self):
        with _static_server() as base_url:
            self.base_url = base_url
            body = _sse(
                {"type": "route", "name": "topic_news"},
                {"type": "tool", "name": "get_latest_news"},
                {
                    "type": "text",
                    "text": (
                        "Quick live check on the non-sports topics:\n\n"
                        "*Teenage Engineering*\n- OP-XY firmware update spotted\n\n"
                        "*Android*\n- Android feature drop starts rolling out\n\n"
                        "*Nothing*\n- Nothing OS update reaches Phone users"
                    ),
                },
                {
                    "type": "done",
                    "text": (
                        "Quick live check on the non-sports topics:\n\n"
                        "*Teenage Engineering*\n- OP-XY firmware update spotted\n\n"
                        "*Android*\n- Android feature drop starts rolling out\n\n"
                        "*Nothing*\n- Nothing OS update reaches Phone users"
                    ),
                },
                {"type": "saved"},
            )
            messages = self._run_page(body)

        self.assertIn("Teenage Engineering", messages)
        self.assertIn("Android feature drop", messages)
        self.assertIn("Nothing OS update", messages)
        self.assertNotIn("backend snag", messages.lower())

    def test_topic_news_partial_failure_copy_renders_in_chat(self):
        with _static_server() as base_url:
            self.base_url = base_url
            body = _sse(
                {"type": "route", "name": "topic_news"},
                {"type": "tool", "name": "get_latest_news"},
                {
                    "type": "text",
                    "text": (
                        "Quick live check on the non-sports topics:\n\n"
                        "*Teenage Engineering*\n- Live news check failed, so I'm not going to pad this with memory.\n\n"
                        "*Nothing*\n- Nothing OS update reaches Phone users"
                    ),
                },
                {
                    "type": "done",
                    "text": (
                        "Quick live check on the non-sports topics:\n\n"
                        "*Teenage Engineering*\n- Live news check failed, so I'm not going to pad this with memory.\n\n"
                        "*Nothing*\n- Nothing OS update reaches Phone users"
                    ),
                },
                {"type": "saved"},
            )
            messages = self._run_page(body)

        self.assertIn("Live news check failed", messages)
        self.assertIn("Nothing OS update", messages)
        self.assertNotIn("backend snag", messages.lower())

    def test_generic_shortlist_topic_news_renders_in_chat(self):
        with _static_server() as base_url:
            self.base_url = base_url
            body = _sse(
                {"type": "route", "name": "topic_news"},
                {"type": "tool", "name": "get_latest_news"},
                {
                    "type": "text",
                    "text": (
                        "Quick live check on the non-sports topics:\n\n"
                        "*Latest from your shortlist*\n"
                        "- AI model update lands\n"
                        "- Android feature drop starts rolling out\n"
                        "- Nothing OS update reaches Phone users"
                    ),
                },
                {
                    "type": "done",
                    "text": (
                        "Quick live check on the non-sports topics:\n\n"
                        "*Latest from your shortlist*\n"
                        "- AI model update lands\n"
                        "- Android feature drop starts rolling out\n"
                        "- Nothing OS update reaches Phone users"
                    ),
                },
                {"type": "saved"},
            )
            messages = self._run_page(body, prompt="Any recent news on my favourite topics?")

        self.assertIn("Latest from your shortlist", messages)
        self.assertIn("AI model update", messages)
        self.assertIn("Nothing OS update", messages)
        self.assertNotIn("backend snag", messages.lower())
