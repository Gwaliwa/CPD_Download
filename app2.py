# app2.py — UNICEF CPD English PDF downloader
# Robust fetch pipeline against 403:
# 1) Playwright Browser  2) Playwright Request API  3) Hardened requests  4) Jina mirror fallback
# Run locally:  python -m streamlit run app2.py

import io, os, re, time, zipfile, random, tempfile, platform, sys, subprocess
from typing import List, Tuple
from urllib.parse import urljoin, urlparse

import streamlit as st
st.set_page_config(page_title="UNICEF CPD • English PDF Downloader", page_icon="📄", layout="wide")

from bs4 import BeautifulSoup
import requests

DEFAULT_URL = "https://www.unicef.org/executiveboard/country-programme-documents"

# ----------------- Headers / UAs -----------------
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    # Hints help a few CDNs:
    "sec-ch-ua": '"Chromium";v="124", "Not=A?Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def _in_cloud() -> bool:
    return os.path.expanduser("~").startswith("/home/appuser")

# ----------------- Optional Playwright -----------------
PLAYWRIGHT_AVAILABLE = True
try:
    from playwright.sync_api import (
        sync_playwright, Playwright, Browser, BrowserContext, Page, APIResponse
    )
except Exception:
    PLAYWRIGHT_AVAILABLE = False

@st.cache_resource(show_spinner=False)
def ensure_playwright_browser() -> bool:
    """Ensure Chromium for full browser mode. If it fails, we’ll still have other fallbacks."""
    if not PLAYWRIGHT_AVAILABLE:
        return False
    os.environ.setdefault(
        "PLAYWRIGHT_BROWSERS_PATH",
        "/home/appuser/.cache/ms-playwright" if _in_cloud() else os.path.expanduser("~/.cache/ms-playwright")
    )
    try:
        with sync_playwright() as p:
            b = p.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu","--single-process","--no-zygote"],
            )
            b.close()
            return True
    except Exception:
        if _in_cloud():
            try:
                subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
                with sync_playwright() as p:
                    b = p.chromium.launch(
                        headless=True,
                        args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu","--single-process","--no-zygote"],
                    )
                    b.close()
                return True
            except Exception:
                return False
        return False

PLAYWRIGHT_READY = ensure_playwright_browser()

# ----------------- Utils -----------------
def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def guess_is_english(href: str, anchor_text: str) -> bool:
    s_low = ((href or "") + " " + (anchor_text or "")).lower()
    fn = os.path.basename(urlparse(href).path).lower()
    if any(sfx in fn for sfx in ("-en.pdf", "_en.pdf", "-eng.pdf", "_eng.pdf")):
        return True
    if any(tok in s_low for tok in ("/en/", "lang=en", "language=en", "locale=en", "english")):
        return True
    if re.search(r"[\(\[\s]en[\)\]\s]", s_low):
        return True
    return False

def safe_filename_from_url(pdf_url: str) -> str:
    name = os.path.basename(urlparse(pdf_url).path) or "document.pdf"
    return name if name.lower().endswith(".pdf") else f"{name}.pdf"

def make_zip(file_tuples: List[Tuple[str, bytes]]) -> bytes:
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for fname, data in file_tuples:
            base, i, fn = fname, 2, fname
            while fn in z.namelist():
                fn = re.sub(r"(\.pdf)$", f"_{i}\\1", base, flags=re.I)
                i += 1
            z.writestr(fn, data)
    mem.seek(0)
    return mem.read()

# ----------------- Fetch strategies -----------------
def make_session(user_agent: str, referer: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(BASE_HEADERS | {
        "User-Agent": user_agent,
        "Referer": referer,
        "Origin": "https://www.unicef.org",
    })
    return s

def fetch_html_via_playwright_request_api(url: str, referer: str) -> str | None:
    if not PLAYWRIGHT_AVAILABLE:
        return None
    try:
        with sync_playwright() as p:
            ctx = p.request.new_context(
                base_url=None,
                extra_http_headers=BASE_HEADERS | {"Referer": referer},
                user_agent=UA_POOL[0],
            )
            r = ctx.get(url, timeout=30_000)
            if r.ok:
                return r.text()
    except Exception:
        return None
    return None

def fetch_html_hardened(url: str, referer: str, tries: int = 4, warm_paths: list[str] | None = None) -> str | None:
    warm_paths = warm_paths or [
        "https://www.unicef.org/",
        "https://www.unicef.org/executiveboard",
    ]
    last = None
    for attempt in range(tries):
        ua = UA_POOL[attempt % len(UA_POOL)]
        sess = make_session(ua, referer)
        # warm cookies/security
        for wp in warm_paths:
            try:
                sess.get(wp, timeout=20, allow_redirects=True)
                time.sleep(0.3 + random.random()*0.4)
            except Exception:
                pass
        try:
            resp = sess.get(url, timeout=35, allow_redirects=True)
            if resp.status_code == 200 and resp.text and len(resp.text) > 2000:
                return resp.text
            last = f"HTTP {resp.status_code}"
        except Exception as e:
            last = repr(e)
        time.sleep(0.7 + attempt * 0.5)
    return None

def fetch_html_via_mirror(url: str) -> str | None:
    """
    Last resort: fetch a readable copy via r.jina.ai mirror.
    We only use it to *extract PDF links*, not for downloading.
    """
    # Normalize to http form for mirror
    no_scheme = re.sub(r"^https?://", "", url.strip())
    mirror = f"https://r.jina.ai/http://{no_scheme}"
    try:
        r = requests.get(mirror, timeout=35, headers={"User-Agent": UA_POOL[0]})
        if r.status_code == 200 and len(r.text) > 1000:
            return r.text
    except Exception:
        pass
    return None

# ----------------- Playwright Browser session (scan + download) -----------------
class BrowserSession:
    def __init__(self, referer: str, timeout_ms: int = 30000, headless: bool = True):
        self._p = None
        self.browser = None
        self.ctx = None
        self.page = None
        self.referer = referer
        self.timeout_ms = timeout_ms
        self.headless = headless

    def __enter__(self):
        if not (PLAYWRIGHT_AVAILABLE and PLAYWRIGHT_READY):
            raise RuntimeError("Playwright browser not available")
        self._p = sync_playwright().start()
        self.browser = self._p.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu","--single-process","--no-zygote"],
        )
        self.ctx = self.browser.new_context(
            accept_downloads=True,
            user_agent=UA_POOL[0],
            locale="en-US",
            java_script_enabled=True,
        )
        self.ctx.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9", "DNT": "1"})
        self.page = self.ctx.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.ctx: self.ctx.close()
        finally:
            try:
                if self.browser: self.browser.close()
            finally:
                if self._p: self._p.stop()

    def render_html(self, url: str) -> str:
        self.page.goto(url, wait_until="domcontentloaded")
        # Accept common cookie banners (best-effort)
        for sel in [
            "#onetrust-accept-btn-handler",
            "button#onetrust-accept-btn-handler",
            "button[aria-label='Accept all cookies']",
            "button:has-text('Accept')",
            "button:has-text('I Accept')",
        ]:
            try:
                self.page.locator(sel).first.click(timeout=1200)
                break
            except Exception:
                pass
        try:
            self.page.wait_for_load_state("networkidle", timeout=9000)
        except Exception:
            pass
        return self.page.content()

    def request_pdf(self, pdf_url: str) -> bytes | None:
        headers = {
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
            "Referer": self.referer,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-site",
            "Upgrade-Insecure-Requests": "1",
        }
        resp: APIResponse = self.ctx.request.get(pdf_url, headers=headers)
        if resp.ok:
            ct = (resp.headers.get("content-type") or "").lower()
            if "application/pdf" in ct or "application/octet-stream" in ct or ct == "":
                return resp.body()
        return None

    def click_download(self, pdf_url: str) -> bytes:
        with self.page.expect_download(timeout=self.timeout_ms) as dl_info:
            self.page.evaluate(
                """(url) => { const a=document.createElement('a'); a.href=url; a.download=''; a.rel='noopener';
                              a.target='_self'; document.body.appendChild(a); a.click(); a.remove(); }""",
                pdf_url,
            )
        dl = dl_info.value
        tmpdir = tempfile.mkdtemp(prefix="dl_")
        name = (dl.suggested_filename or safe_filename_from_url(pdf_url)).replace("/", "_")
        path = os.path.join(tmpdir, name)
        dl.save_as(path)
        with open(path, "rb") as f:
            data = f.read()
        try:
            os.remove(path); os.rmdir(tmpdir)
        except Exception:
            pass
        return data

# ----------------- Scan & Parse -----------------
def parse_pdf_links(html: str, page_url: str) -> List[Tuple[str, str]]:
    # Try DOM parsing first
    try:
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().endswith(".pdf"):
                links.append((urljoin(page_url, href), normalize_space(a.get_text(" "))))
        if links:
            # de-dup
            seen, dedup = set(), []
            for u, t in links:
                if u not in seen:
                    seen.add(u); dedup.append((u, t))
            return dedup
    except Exception:
        pass
    # If mirror returned plain text, fallback to regex for .pdf URLs
    urls = set(re.findall(r"https?://[^\s\"'<>]+?\.pdf", html, flags=re.I))
    return [(u, "") for u in sorted(urls)]

def collect_pdf_links(page_url: str) -> List[Tuple[str, str]]:
    # 1) Full Browser
    if PLAYWRIGHT_READY:
        with BrowserSession(referer=page_url) as b:
            html = b.render_html(page_url)
            return parse_pdf_links(html, page_url)
    # 2) Playwright Request API (no Chromium)
    html = fetch_html_via_playwright_request_api(page_url, referer=page_url)
    if html:
        return parse_pdf_links(html, page_url)
    # 3) Hardened requests
    html = fetch_html_hardened(page_url, referer=page_url, warm_paths=[
        "https://www.unicef.org/",
        "https://www.unicef.org/executiveboard",
        "https://www.unicef.org/executiveboard/country-programme-documents",
    ])
    if html:
        return parse_pdf_links(html, page_url)
    # 4) Mirror fallback (read-only)
    html = fetch_html_via_mirror(page_url)
    if html:
        return parse_pdf_links(html, page_url)
    raise RuntimeError("All fetch strategies failed (403/blocked)")

# ----------------- Download -----------------
def download_pdf_smart(pdf_url: str, referer: str, delay_between: float = 0.0) -> bytes:
    # Prefer browser
    if PLAYWRIGHT_READY:
        with BrowserSession(referer=referer) as b:
            x = b.request_pdf(pdf_url)
            if x: return x
            time.sleep(0.2)
            return b.click_download(pdf_url)
    # Fallback direct GET with hardened headers
    for attempt in range(3):
        ua = UA_POOL[attempt % len(UA_POOL)]
        sess = make_session(ua, referer)
        r = sess.get(pdf_url, timeout=60, allow_redirects=True)
        if r.status_code == 200:
            ct = (r.headers.get("content-type") or "").lower()
            if "application/pdf" in ct or "application/octet-stream" in ct or ct == "":
                return r.content
        time.sleep(delay_between + random.uniform(0.1, 0.3))
    raise RuntimeError(f"Download blocked or not a PDF: {pdf_url}")

# ----------------- UI -----------------
st.sidebar.info(f"Mode: {'Playwright browser' if PLAYWRIGHT_READY else 'HTTP/RequestAPI/mirror'}  •  Python {sys.version.split()[0]}")
st.sidebar.caption(f"Exec: {sys.executable}  •  Platform: {platform.system()} {platform.release()}")
st.title("📄 UNICEF CPD — English PDF Downloader")

with st.expander("ℹ️ Notes"):
    st.markdown(
        "- Prefers a real browser (best against 403). If not available, tries multiple HTTP strategies including a read-only mirror to extract .pdf links.\n"
        "- Downloads use the browser when available; otherwise a direct GET with Referer and hardened headers."
    )

url = st.text_input("Page URL", value=DEFAULT_URL)
colA, colB, colC = st.columns([1, 1, 1])
with colA:
    enforce_english = st.checkbox("Only English PDFs", value=True)
with colB:
    show_all = st.checkbox("Show non-English too (for review)", value=False)
with colC:
    add_delay = st.number_input("Delay (sec) between downloads", min_value=0.0, value=0.5, step=0.1)

scan = st.button("🔎 Scan page")

if "scan_results" not in st.session_state:
    st.session_state.scan_results = []

if scan:
    try:
        pdfs = collect_pdf_links(url)
        st.session_state.scan_results = pdfs
        st.success(f"Found {len(pdfs)} PDF link(s) on the page.")
    except Exception as e:
        st.error(f"Error fetching the page: {e}")

pdfs = st.session_state.get("scan_results", [])

if pdfs:
    rows, english_rows = [], []
    for href, text in pdfs:
        is_en = guess_is_english(href, text)
        rows.append({
            "English?": "Yes" if is_en else "No",
            "Link text": text,
            "PDF URL": href,
            "Filename": safe_filename_from_url(href),
        })
        if is_en:
            english_rows.append(rows[-1])

    display_rows = rows if show_all else english_rows
    st.write("### Discovered PDF links")
    st.dataframe(display_rows, use_container_width=True)

    to_download = english_rows if enforce_english else rows
    if len(to_download) == 0:
        st.warning("No PDFs match the English filter. Try showing all to review.")
    else:
        if st.button(f"⬇️ Build ZIP with {len(to_download)} file(s)"):
            file_buffers: List[Tuple[str, bytes]] = []
            progress = st.progress(0)
            status = st.empty()
            try:
                for i, r in enumerate(to_download, start=1):
                    pdf_url, fname = r["PDF URL"], r["Filename"]
                    try:
                        status.text(f"Downloading {fname} …")
                        content = download_pdf_smart(pdf_url, referer=url, delay_between=add_delay)
                        file_buffers.append((fname, content))
                    except Exception as e:
                        st.warning(f"Failed: {fname} — {e}")
                    finally:
                        progress.progress(i / len(to_download)); time.sleep(random.uniform(0.05, 0.15))
            finally:
                status.empty(); progress.empty()

            if file_buffers:
                zip_bytes = make_zip(file_buffers)
                st.download_button(
                    label="📦 Download ZIP",
                    data=zip_bytes,
                    file_name="unicef_cpd_english_pdfs.zip",
                    mime="application/zip",
                )
                st.success(f"ZIP is ready with {len(file_buffers)}/{len(to_download)} files.")
