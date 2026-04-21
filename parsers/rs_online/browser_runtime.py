"""Browser lifecycle helpers for the RS Online browser parser."""

import socket
import subprocess
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright


class BrowserRuntimeMixin:
    """Owns Chrome/CDP startup, recovery, and shutdown behavior."""

    def _ensure_browser(self) -> None:
        if self.playwright and self.context and self.listing_page and self.detail_page:
            return

        self.playwright = sync_playwright().start()

        if not self._is_port_open(self.cdp_port):
            browser_path = self._resolve_browser_path()
            if not browser_path:
                raise RuntimeError("No local Chrome/Edge browser installation was found.")

            self.remote_debug_profile_dir.mkdir(exist_ok=True)
            command = [
                browser_path,
                f"--remote-debugging-port={self.cdp_port}",
                f"--user-data-dir={self.remote_debug_profile_dir}",
                "--no-first-run",
                "--no-default-browser-check",
                "about:blank",
            ]
            self.browser_process = subprocess.Popen(command)
            self._wait_for_cdp_port()

        self.browser = self.playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{self.cdp_port}")
        contexts = self.browser.contexts
        self.context = contexts[0] if contexts else self.browser.new_context()
        pages = self.context.pages
        self.listing_page = pages[0] if pages else self.context.new_page()
        self.detail_page = self.context.new_page()
        self.listing_page.set_default_timeout(self.timeout * 1000)
        self.detail_page.set_default_timeout(self.timeout * 1000)

    def _ensure_page(self, page_kind: str) -> Page:
        self._ensure_browser()
        if page_kind == "detail":
            return self.detail_page
        return self.listing_page

    @staticmethod
    def _should_reset_browser_connection(exc: Exception) -> bool:
        message = str(exc).lower()
        markers = (
            "frame was detached",
            "frame has been detached",
            "connection closed",
            "target page, context or browser has been closed",
            "list.remove(x): x not in list",
        )
        return any(marker in message for marker in markers)

    def _reset_browser_connection(self) -> None:
        self.listing_page = None
        self.detail_page = None
        self.context = None
        self.browser = None

        self._terminate_browser_process()

        if self.playwright is not None:
            try:
                self.playwright.stop()
            except BaseException:
                pass
            self.playwright = None

    def _terminate_browser_process(self) -> None:
        if self.browser_process and self.browser_process.poll() is None:
            try:
                self.browser_process.terminate()
                self.browser_process.wait(timeout=5)
            except BaseException:
                try:
                    self.browser_process.kill()
                    self.browser_process.wait(timeout=5)
                except BaseException:
                    pass
        self.browser_process = None

    def _handle_cookie_popup(self, page: Page) -> None:
        selectors = [
            "button:has-text('Accept All Cookies')",
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
            "#onetrust-accept-btn-handler",
        ]
        for selector in selectors:
            try:
                button = page.locator(selector).first
                if button.is_visible(timeout=500):
                    button.click(timeout=1000)
                    page.wait_for_timeout(500)
                    return
            except Exception:
                continue

    def _resolve_browser_path(self) -> str:
        if self.browser_path_override:
            return self.browser_path_override
        candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ]
        for candidate in candidates:
            if Path(candidate).exists():
                return candidate
        return ""

    @staticmethod
    def _is_port_open(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex(("127.0.0.1", port)) == 0

    def _wait_for_cdp_port(self) -> None:
        deadline = time.time() + self.timeout
        while time.time() < deadline:
            if self._is_port_open(self.cdp_port):
                return
            time.sleep(0.25)
        raise RuntimeError(f"Could not open CDP port {self.cdp_port}")

    def close(self) -> None:
        if self.keep_browser_open:
            return
        self._reset_browser_connection()
