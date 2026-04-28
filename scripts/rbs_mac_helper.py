#!/usr/bin/env python3
"""
Local Mac helper for Hira's RBS availability checks.

Run this on the Mac that has access to your Chrome/MIMS session:

    python scripts/rbs_mac_helper.py --once
    python scripts/rbs_mac_helper.py --poll

Install local-only browser dependency first:

    python -m pip install playwright
    python -m playwright install chromium

This script shares jobs through the existing Google Sheet Config store.
It performs dry-run availability checks only; it does not submit bookings.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import google_services as gs  # noqa: E402

RBS_URL = os.environ.get("RBS_URL", "https://rbs.avero-tech.com/login.html")
PROFILE_DIR = Path(os.environ.get("RBS_CHROME_PROFILE_DIR", "~/.hira-rbs-chrome")).expanduser()
SCREENSHOT_DIR = Path(os.environ.get("RBS_SCREENSHOT_DIR", str(ROOT / "files" / "rbs"))).expanduser()
POLL_SECONDS = int(os.environ.get("RBS_HELPER_POLL_SECONDS", "15"))
LOGIN_WAIT_SECONDS = int(os.environ.get("RBS_LOGIN_WAIT_SECONDS", "180"))


def _log(message: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def _period_number(value: str) -> int:
    match = re.search(r"(\d{1,2})", str(value or ""))
    if not match:
        raise ValueError(f"Invalid period: {value}")
    return int(match.group(1))


def _period_labels(start: str, end: str) -> list[str]:
    first = _period_number(start)
    last = _period_number(end)
    if first > last:
        first, last = last, first
    return [f"P{i}" for i in range(first, last + 1)]


def _click_if_visible(page, selector: str, timeout: int = 1500) -> bool:
    try:
        locator = page.locator(selector).first
        if locator.is_visible(timeout=timeout):
            locator.click()
            return True
    except Exception:
        return False
    return False


def _open_rbs(page):
    page.goto(RBS_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1500)

    _click_if_visible(page, "text=Sign in via MIMS", timeout=3000)
    page.wait_for_timeout(1500)

    # If credentials are prefilled on the MIMS page, this is usually enough.
    _click_if_visible(page, "button:has-text('Sign in')", timeout=2500)
    page.wait_for_load_state("domcontentloaded", timeout=60000)
    page.wait_for_timeout(2500)

    if _looks_like_mims_login(page):
        _log(
            "MIMS login is still showing. Please sign in manually in the Chrome window; "
            f"I will wait up to {LOGIN_WAIT_SECONDS}s."
        )
        _wait_for_manual_login(page)


def _looks_like_mims_login(page) -> bool:
    try:
        text = page.locator("body").inner_text(timeout=1500).lower()
    except Exception:
        text = ""
    url = page.url.lower()
    return (
        "mims portal" in text
        or "forgot password" in text
        or "please enter values for the user name and password" in text
        or "mims" in url
    ) and "resource booking" not in text


def _wait_for_manual_login(page):
    deadline = time.time() + LOGIN_WAIT_SECONDS
    while time.time() < deadline:
        if not _looks_like_mims_login(page):
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    raise RuntimeError("Still on MIMS login page after waiting for manual sign-in.")


def _go_to_make_booking(page):
    # RBS sometimes lands on Home after auth; these clicks are intentionally broad.
    for selector in [
        "text=Resource Booking",
        "a:has-text('Resource Booking')",
        "button:has-text('Resource Booking')",
    ]:
        if _click_if_visible(page, selector, timeout=2500):
            page.wait_for_timeout(800)
            break

    for selector in [
        "text=Make New Booking",
        "a:has-text('Make New Booking')",
        "button:has-text('Make New Booking')",
    ]:
        if _click_if_visible(page, selector, timeout=2500):
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(1500)
            return


def _set_date(page, iso_date: str):
    parsed = datetime.strptime(iso_date, "%Y-%m-%d")
    rbs_date = parsed.strftime("%d/%m/%Y")
    candidates = [
        "input[placeholder*='day' i]",
        "input[placeholder*='date' i]",
        "input[type='text']",
        "input",
    ]
    for selector in candidates:
        try:
            locator = page.locator(selector).first
            if locator.is_visible(timeout=1500):
                locator.click()
                locator.fill(rbs_date)
                locator.press("Enter")
                page.wait_for_timeout(1200)
                return
        except Exception:
            continue


def _select_period_dropdown(page, label_text: str, period: str):
    # Works for native selects if the site uses them.
    try:
        index = 0 if label_text.lower().startswith("from") else 1
        select = page.locator("select").nth(index)
        if select.is_visible(timeout=1000):
            select.select_option(label=period)
            page.wait_for_timeout(500)
            return
    except Exception:
        pass

    # Fallback for Bootstrap/select2-style controls.
    for selector in [
        f"text={label_text}",
        f"[aria-label*='{label_text}' i]",
        f".dropdown-toggle:has-text('{label_text}')",
    ]:
        if _click_if_visible(page, selector, timeout=1200):
            page.wait_for_timeout(500)
            break
    _click_if_visible(page, f"text={period}", timeout=2500)
    page.wait_for_timeout(700)


def _set_periods(page, from_period: str, till_period: str):
    _select_period_dropdown(page, "From Period", from_period)
    _select_period_dropdown(page, "Till Period", till_period)


def _search_resources(page, resources: list[str]):
    search_text = ", ".join(resources)
    for selector in [
        "input[placeholder*='resource' i]",
        "input[placeholder*='search' i]",
    ]:
        try:
            locator = page.locator(selector).first
            if locator.is_visible(timeout=1500):
                locator.fill(search_text)
                locator.press("Enter")
                page.wait_for_timeout(1500)
                return
        except Exception:
            continue


def _read_grid(page, resources: list[str], periods: list[str]) -> dict:
    """Best-effort grid reader. Empty cells are available; green/grey icons are booked."""
    script = """
    ({resources, periods}) => {
      const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
      const visible = (el) => {
        const r = el.getBoundingClientRect();
        const style = window.getComputedStyle(el);
        return r.width > 0 && r.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
      };
      const all = Array.from(document.querySelectorAll('body *')).filter(visible);

      const periodHeaders = {};
      for (const period of periods) {
        const found = all
          .map(el => ({el, text: norm(el.innerText), rect: el.getBoundingClientRect()}))
          .filter(item => item.text.startsWith(norm(period)) && item.text.includes(':'))
          .sort((a, b) => (a.rect.width * a.rect.height) - (b.rect.width * b.rect.height))[0];
        if (found) periodHeaders[period] = found.rect;
      }
      if (Object.keys(periodHeaders).length === 0) {
        return {available: [], unavailable: resources, notes: 'Could not find period headers in the RBS grid.'};
      }

      const resourceRects = {};
      for (const resource of resources) {
        const found = all
          .map(el => ({el, text: norm(el.innerText), rect: el.getBoundingClientRect()}))
          .filter(item => item.text === norm(resource))
          .sort((a, b) => (a.rect.width * a.rect.height) - (b.rect.width * b.rect.height))[0];
        if (found) resourceRects[resource] = found.rect;
      }

      const isGreen = (color) => {
        const nums = (color.match(/\\d+/g) || []).map(Number);
        if (nums.length < 3) return false;
        const [r, g, b] = nums;
        return g > 120 && g > r * 1.25 && g > b * 1.25;
      };
      const isGrey = (color) => {
        const nums = (color.match(/\\d+/g) || []).map(Number);
        if (nums.length < 3) return false;
        const [r, g, b] = nums;
        return Math.abs(r - g) < 20 && Math.abs(g - b) < 20 && r > 70 && r < 180;
      };
      const elementBookingState = (el) => {
        if (!el) return 'empty';
        const chain = [el, ...Array.from(el.querySelectorAll('*'))];
        for (const node of chain) {
          const style = window.getComputedStyle(node);
          if (isGreen(style.color) || isGreen(style.backgroundColor) || isGreen(style.fill)) return 'mine';
        }
        for (const node of chain) {
          const style = window.getComputedStyle(node);
          const cls = String(node.className || '').toLowerCase();
          if (isGrey(style.color) || isGrey(style.backgroundColor) || cls.includes('book')) return 'blocked';
        }
        return norm(el.innerText) ? 'blocked' : 'empty';
      };

      const rows = Array.from(document.querySelectorAll('tr')).filter(visible);
      const available = [];
      const unavailable = [];
      for (const resource of resources) {
        const resourceRect = resourceRects[resource];
        if (!resourceRect) {
          unavailable.push(`${resource} (row not found)`);
          continue;
        }
        const y = resourceRect.top + resourceRect.height / 2;
        const row = rows.find(r => {
          const rr = r.getBoundingClientRect();
          return y >= rr.top && y <= rr.bottom;
        });
        const blocked = [];
        const mine = [];
        for (const period of periods) {
          const header = periodHeaders[period];
          if (!header) {
            blocked.push(`${period}: header not found`);
            continue;
          }
          const x = header.left + header.width / 2;
          let state = 'empty';
          if (row) {
            const cells = Array.from(row.children).filter(visible);
            const cell = cells.find(c => {
              const r = c.getBoundingClientRect();
              return x >= r.left && x <= r.right;
            });
            state = elementBookingState(cell);
          } else {
            const element = document.elementFromPoint(x, y);
            state = elementBookingState(element);
          }
          if (state === 'blocked') blocked.push(period);
          if (state === 'mine') mine.push(period);
        }
        if (blocked.length || mine.length) {
          const bits = [];
          if (blocked.length) bits.push(`${blocked.join(', ')} booked`);
          if (mine.length) bits.push(`${mine.join(', ')} already booked by you`);
          unavailable.push(`${resource} (${bits.join('; ')})`);
        } else {
          available.push(resource);
        }
      }
      return {available, unavailable, notes: ''};
    }
    """
    return page.evaluate(script, {"resources": resources, "periods": periods})


def check_availability(page, job: dict) -> dict:
    resources = job.get("resources") or ["1.com", "2.com", "3.com", "4.com"]
    periods = _period_labels(job["from_period"], job["till_period"])

    _open_rbs(page)
    _go_to_make_booking(page)
    _set_date(page, job["date"])
    _set_periods(page, job["from_period"], job["till_period"])
    _search_resources(page, resources)

    page.wait_for_timeout(1200)
    grid_result = _read_grid(page, resources, periods)

    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    screenshot = SCREENSHOT_DIR / f"rbs-job-{job['id']}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.png"
    page.screenshot(path=str(screenshot), full_page=True)

    notes = grid_result.get("notes") or "Availability inferred from the visible RBS grid. Please verify screenshot on first few runs."
    return {
        "available": grid_result.get("available", []),
        "unavailable": grid_result.get("unavailable", []),
        "periods": periods,
        "screenshot": str(screenshot),
        "notes": notes,
    }


def process_once() -> bool:
    job = gs.claim_next_rbs_job()
    if not job:
        _log("No queued RBS jobs.")
        return False

    _log(f"Claimed RBS job #{job['id']}: {json.dumps(job, ensure_ascii=False)}")
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        gs.update_rbs_job(job["id"], "failed", error=f"Playwright is not installed locally: {exc}")
        raise

    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                channel=os.environ.get("RBS_BROWSER_CHANNEL", "chrome"),
                headless=False,
                viewport={"width": 1600, "height": 1000},
                args=["--start-maximized"],
            )
            page = context.pages[0] if context.pages else context.new_page()
            result = check_availability(page, job)
            context.close()
        gs.update_rbs_job(job["id"], "done", result=result)
        _log(f"Completed RBS job #{job['id']}: {json.dumps(result, ensure_ascii=False)}")
        return True
    except Exception as exc:
        _log(f"RBS job #{job['id']} failed: {exc}")
        try:
            gs.update_rbs_job(job["id"], "failed", error=str(exc))
        except Exception:
            pass
        return True


def poll_forever():
    _log(f"RBS helper polling every {POLL_SECONDS}s. Profile: {PROFILE_DIR}")
    while True:
        try:
            process_once()
        except Exception as exc:
            _log(f"Helper loop error: {exc}")
        time.sleep(POLL_SECONDS)


def main():
    parser = argparse.ArgumentParser(description="Hira local Mac helper for RBS dry-run availability checks.")
    parser.add_argument("--once", action="store_true", help="Process one queued job then exit.")
    parser.add_argument("--poll", action="store_true", help="Poll for queued jobs until stopped.")
    args = parser.parse_args()

    if args.poll:
        poll_forever()
    else:
        process_once()


if __name__ == "__main__":
    main()
