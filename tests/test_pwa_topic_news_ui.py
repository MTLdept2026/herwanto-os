import contextlib
import json
import os
import threading
import unittest
from html.parser import HTMLParser
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


class _ButtonParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.buttons = []
        self._form_stack = []

    def handle_starttag(self, tag, attrs):
        attr_map = dict(attrs)
        if tag == "form":
            self._form_stack.append(attr_map.get("id", ""))
        if tag == "button":
            attr_map["_form_id"] = self._form_stack[-1] if self._form_stack else ""
            self.buttons.append(attr_map)

    def handle_endtag(self, tag):
        if tag == "form" and self._form_stack:
            self._form_stack.pop()


class PwaButtonWiringTests(unittest.TestCase):
    def test_static_buttons_have_declared_handlers(self):
        html = (REPO_ROOT / "pwa" / "index.html").read_text()
        app_js = (REPO_ROOT / "pwa" / "app.js").read_text()
        parser = _ButtonParser()
        parser.feed(html)

        indirectly_handled_ids = {
            "notificationReaderDoneBtn",
            "notificationReaderSnoozeBtn",
            "notificationReaderDismissBtn",
        }
        missing = []
        for button in parser.buttons:
            button_id = button.get("id", "")
            button_type = button.get("type", "submit")
            classes = set((button.get("class", "") or "").split())
            data_keys = {key for key in button if key.startswith("data-")}
            handled = bool(
                (button_id and f'$("#{button_id}").addEventListener' in app_js)
                or (button_id and f'$("#{button_id}")?.addEventListener' in app_js)
                or button_id in indirectly_handled_ids
                or button_type == "submit"
                or "nav-tab" in classes
                or "theme-btn" in classes
                or data_keys & {
                    "data-command-action",
                    "data-command-prompt",
                    "data-reader-close",
                    "data-theme-choice",
                    "data-home-dismiss",
                    "data-gmail-preset",
                    "data-quick-close",
                    "data-quick-view",
                }
            )
            if not handled:
                missing.append(button_id or f"button in form {button.get('_form_id', '')}")

        self.assertEqual(missing, [])


class PwaTopicNewsUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise unittest.SkipTest(f"Playwright is not installed: {exc}")
        cls._sync_playwright = staticmethod(sync_playwright)

    def _run_page(self, chat_body, prompt="How about Nothing, Teenage Engineering and Android stuff?", wait_for_text="Nothing OS update reaches Phone users"):
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
                page.locator("#messages").get_by_text(wait_for_text).wait_for(timeout=6000)
                return page.locator("#messages").inner_text()
            finally:
                browser.close()

    def _route_standard_api(self, route):
        url = route.request.url
        if "/api/chat" in url:
            route.fulfill(status=200, headers={"content-type": "text/event-stream"}, body=_sse(
                {"type": "route", "name": "agentic"},
                {"type": "text", "text": "Button smoke reply."},
                {"type": "done", "text": "Button smoke reply."},
                {"type": "saved"},
            ))
            return
        if "/api/home" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({
                "agenda_structured": {"days": []},
                "daily_load": {"today": {"score": 0, "tone": "green"}, "days": []},
                "tasks_structured": {"items": [{"id": "1", "description": "Smoke task", "due": "2026-05-22"}]},
                "marking": {"total_scripts": 0, "marked_scripts": 0, "unmarked_scripts": 0, "sets": []},
                "proactive": {"top": []},
                "digest": {},
                "intelligence": {},
                "classops": {},
                "services": {},
                "files": "",
                "briefing_delivery": {"overall": "ok", "summary": "Ready", "slots": []},
                "prayers": {},
                "sync_timings": [],
            }))
            return
        if "/api/agenda" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"text": "Agenda", "structured": {"days": []}}))
            return
        if "/api/tasks/1/done" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"ok": True, "synced_marking": {}}))
            return
        if "/api/tasks" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"text": "Task brief", "structured": {"items": [{"id": "1", "description": "Smoke task", "due": "2026-05-22"}]}}))
            return
        if "/api/gmail/draft" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"account": "personal"}))
            return
        if "/api/gmail" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"account": "personal", "messages": [{"subject": "Smoke mail", "from": "a@example.com", "date": "Today", "snippet": "Hello"}]}))
            return
        if "/api/files" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"text": "Files ready"}))
            return
        if "/api/notifications/n1" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"notification": {"id": "n1", "kind": "reminder", "title": "Smoke", "body": "Smoke body", "source": "nudge:1"}}))
            return
        if "/api/notifications/health" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"prayers": {"prayers": []}, "recent_delivery_log": [], "outcome_actions": {}, "push_recovery": {}, "briefing_delivery": {}}))
            return
        if "/api/notifications/test" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"sent": False, "notification": {"id": "n1", "kind": "test", "title": "Smoke", "body": "Smoke body", "source": "test"}}))
            return
        if "/api/notifications" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"notifications": [{"id": "n1", "kind": "reminder", "title": "Smoke", "body": "Smoke body", "source": "nudge:1"}], "ok": True}))
            return
        if "/api/action-ledger/a1/review" in url or "/api/action-ledger/a1/undo" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"ok": True, "result": "ok", "entry": {"id": "a1", "action": "task.done", "status": "done", "subject": "Smoke", "reviewed": True}}))
            return
        if "/api/action-ledger" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"entries": [{"id": "a1", "action": "task.done", "status": "done", "subject": "Smoke"}]}))
            return
        if "/api/app/version" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"git_commit": "test", "server_time": "2026-05-21T13:00:00+08:00"}))
            return
        route.fulfill(status=200, content_type="application/json", body=json.dumps({"ok": True}))

    def test_static_buttons_click_without_frontend_errors(self):
        with _static_server() as base_url, self._sync_playwright() as p:
            executable = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            launch_kwargs = {"headless": True}
            if os.path.exists(executable):
                launch_kwargs["executable_path"] = executable
            try:
                browser = p.chromium.launch(**launch_kwargs)
            except Exception as exc:
                raise unittest.SkipTest(f"Chromium is not available for UI tests: {exc}")
            errors = []
            try:
                page = browser.new_page(viewport={"width": 390, "height": 844})
                page.on("pageerror", lambda exc: errors.append(str(exc)))
                page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
                page.add_init_script(
                    """
                    localStorage.setItem("hira_session_unlocked", "1");
                    localStorage.setItem("hira_client_id", "ui-button-smoke-test");
                    localStorage.setItem("hira_pwa_chat", "[]");
                    window.Notification = window.Notification || function(){};
                    window.Notification.permission = "denied";
                    window.Notification.requestPermission = async () => "denied";
                    """
                )
                page.route("**/api/**", self._route_standard_api)
                page.goto(f"{base_url}/pwa/index.html", wait_until="domcontentloaded")
                page.locator("#settingsBtn").click()
                page.locator("#notificationsBtn").click()
                page.locator("#quickActionFab").click()
                page.evaluate(
                    """
                    async () => {
                      const skip = new Set(["installBtn", "attachBtn"]);
                      for (const button of Array.from(document.querySelectorAll("button"))) {
                        if (skip.has(button.id) || button.type === "submit") continue;
                        button.click();
                        await new Promise((resolve) => setTimeout(resolve, 15));
                      }
                    }
                    """
                )
                page.wait_for_timeout(500)
            finally:
                browser.close()

        self.assertEqual(errors, [])

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

    def test_openai_citation_markers_are_hidden_in_chat(self):
        with _static_server() as base_url:
            self.base_url = base_url
            reply = (
                "\ue200cite\ue202turn0search0\ue202turn0search9\ue201\n"
                "- Then Russell went and took **Canadian GP pole**, with Antonelli P2.\n"
                "\n\ufffdcite\ufffdturn1search0\ufffdturn1search4\ufffd\n"
                "Tonight is going to hurt a bit for Liverpool fans."
            )
            body = _sse(
                {"type": "route", "name": "agentic"},
                {"type": "text", "text": reply},
                {"type": "done", "text": reply},
                {"type": "saved"},
            )
            messages = self._run_page(
                body,
                prompt="Great news for mercedes yesterday and liverpool tonight?",
                wait_for_text="Canadian GP pole",
            )

        self.assertIn("Canadian GP pole", messages)
        self.assertIn("Liverpool fans", messages)
        self.assertNotIn("cite", messages)
        self.assertNotIn("turn0search", messages)
        self.assertNotIn("turn1search", messages)
