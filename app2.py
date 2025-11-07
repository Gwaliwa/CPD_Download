# app.py — Streamlit + Playwright (with requests fallback) to collect English PDFs
# Run: python -m streamlit run app.py

import io, os, re, time, zipfile, random, tempfile, platform, sys
from typing import List, Tuple
from urllib.parse import urljoin, urlparse

import subprocess

import streamlit as st
from bs4 import BeautifulSoup

# Try to import Playwright early
PLAYWRIGHT_AVAILABLE = True
try:
    from playwright.sync_api import (
        sync_playwright, Playwright, Browser, BrowserContext, Page, APIResponse
    )
except Exception:
    PLAYWRIGHT_AVAILABLE = False

import requests  # fallback for direct .pdf downloads

DEFAULT_URL = "https://www.unicef.org/executiveboard/country-programme-documents"

# --- Show interpreter info up top (helps verify the right Python) ---
st.sidebar.info(f"Python: {sys.version.split()[0]}  |  Exec: {sys.executable}")
st.sidebar.caption(f"Platform: {platform.system()} {platform.release()}")

def _in_streamlit_cloud() -> bool:
    return os.path.expanduser("~").startswith("/home/appuser")

# ----------------------------------------------------------------------
# Ensure Chromium for Playwright (no --with-deps). If not possible, keep fallback.
# ----------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _ensure_playwright_browser_once() -> bool:
    if not PLAYWRIGHT_AVAILABLE:
        return False

    os.environ.setdefault(
        "PLAYWRIGHT_BROWSERS_PATH",
        "/home/appuser/.cache/ms-playwright" if _in_streamlit_cloud()
        else os.path.expanduser("~/.cache/ms-playwright")
    )

    # Fast path: try launching immediately
    try:
        with sync_playwright() as p:
            b = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process", "--no-zygote"],
            )
            b.close()
            return True
    except Exception:
        # If on Cloud, try to download Chromium binary once
        if _in_streamlit_cloud():
            try:
                subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
                with sync_playwright() as p:
                    b = p.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process", "--no-zygote"],
                    )
                    b.close()
                return True
            except Exception:
                return False
        else:
            # Local: don’t hard-stop; we’ll run in fallback mode
            return False

PLAYWRIGHT_READY = _ensure_playwright_browser_once()

# ---------------- Helpers ----------------
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

# --------- Unified Browser Session (Playwright path) ----------
class DownloadSession:
    def __init__(self, referer: str, timeout_ms: int = 30000, headless: bool = True):
        self._p = None
        self.browser = None
        self.context = None
        self.page = None
        self.referer = referer
        self.timeout_ms = timeout_ms
        self.headless = headless

    def __enter__(self):
        if not PLAYWRIGHT_READY:
            raise RuntimeError("Playwright not available")
        self._p = sync_playwright().start()
        self.browser = self._p.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process", "--no-zygote"],
        )
        self.context = self.browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            java_script_enabled=True,
        )
        self.context.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9", "DNT": "1"})
        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        self._ensure_on_referer()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.context: self.context.close()
        finally:
            try:
                if self.browser: self.browser.close()
            finally:
                if self._p: self._p.stop()

    def _ensure_on_referer(self):
        assert self.page is not None
        try:
            cur = self.page.url
        except Exception:
            cur = "about:blank"
        if not cur or cur == "about:blank" or not cur.startswith(self.referer.split("/executiveboard")[0]):
            self.page.goto(self.referer)

    def fetch_html(self, url: str) -> str:
        assert self.page is not None
        if self.page.url != url:
            self.page.goto(url)
        try:
            self.page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        return self.page.content()

    def _direct_request(self, pdf_url: str) -> bytes | None:
        assert self.context is not None
        headers = {
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
            "Referer": self.referer,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-site",
            "Upgrade-Insecure-Requests": "1",
        }
        resp: APIResponse = self.context.request.get(pdf_url, headers=headers)
        if resp.ok:
            ctype = (resp.headers.get("content-type") or "").lower()
            if "application/pdf" in ctype or "application/octet-stream" in ctype or ctype == "":
                return resp.body()
        return None

    def _download_via_event(self, pdf_url: str) -> bytes:
        assert self.page is not None
        self._ensure_on_referer()
        with self.page.expect_download(timeout=self.timeout_ms) as dl_info:
            self.page.evaluate(
                """(url) => {
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = '';
                    a.rel = 'noopener';
                    a.target = '_self';
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                }""",
                pdf_url,
            )
        download = dl_info.value
        tmpdir = tempfile.mkdtemp(prefix="dl_")
        suggested = (download.suggested_filename or safe_filename_from_url(pdf_url)).replace("/", "_")
        tmp_path = os.path.join(tmpdir, suggested)
        download.save_as(tmp_path)
        with open(tmp_path, "rb") as f:
            data = f.read()
        try:
            os.remove(tmp_path); os.rmdir(tmpdir)
        except Exception:
            pass
        return data

    def download_pdf(self, pdf_url: str, max_attempts: int = 3, delay_between: float = 0.0) -> bytes:
        last_err = None
        for _ in range(max_attempts):
            try:
                data = self._direct_request(pdf_url)
                if data: return data
            except Exception as e:
                last_err = e
            try:
                data = self._download_via_event(pdf_url)
                if data: return data
            except Exception as e2:
                last_err = e2
            time.sleep((delay_between or 0.0) + random.uniform(0.1, 0.4))
        if last_err: raise last_err
        raise RuntimeError("Unknown download failure")


# -------------- Scanning --------------
def collect_pdf_links_requests(page_url: str) -> List[Tuple[str, str]]:
    """Fallback: basic HTML fetch via requests (no JS)."""
    r = requests.get(page_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    results: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            full = urljoin(page_url, href)
            text = normalize_space(a.get_text(" "))
            results.append((full, text))
    # De-dup
    seen, deduped = set(), []
    for u, t in results:
        if u not in seen:
            seen.add(u); deduped.append((u, t))
    return deduped

def collect_pdf_links_with_session(session: DownloadSession, page_url: str) -> List[Tuple[str, str]]:
    html = session.fetch_html(page_url)
    soup = BeautifulSoup(html, "html.parser")
    results: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            full = urljoin(page_url, href)
            text = normalize_space(a.get_text(" "))
            results.append((full, text))
    seen, deduped = set(), []
    for u, t in results:
        if u not in seen:
            seen.add(u); deduped.append((u, t))
    return deduped

# -------------- Streamlit UI --------------
st.set_page_config(page_title="UNICEF CPD • English PDF Downloader", page_icon="📄", layout="wide")
st.title("📄 UNICEF CPD — English PDF Downloader")

with st.expander("ℹ️ Notes"):
    st.markdown(
        "- Uses Playwright when available (first-party Referer, JS, attachment downloads).\n"
        "- Falls back to plain requests if Playwright isn’t available (works for direct .pdf links).\n"
        "- Retries with small jitter to ease rate limits.\n"
        "- Please follow UNICEF’s terms of use and robots.txt."
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
        if PLAYWRIGHT_READY:
            with DownloadSession(referer=url) as sess:
                pdfs = collect_pdf_links_with_session(sess, url)
        else:
            pdfs = collect_pdf_links_requests(url)
        st.session_state.scan_results = pdfs
        st.success(f"Found {len(pdfs)} PDF link(s) on the page.")
    except Exception as e:
        st.error(f"Error fetching the page: {e}")

pdfs = st.session_state.get("scan_results", [])

def requests_download(pdf_url: str, referer: str | None = None) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0"}
    if referer:
        headers["Referer"] = referer
    r = requests.get(pdf_url, headers=headers, timeout=30)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    if "application/pdf" in ct or "application/octet-stream" in ct or ct == "":
        return r.content
    raise RuntimeError(f"Unexpected content-type: {ct}")

if pdfs:
    rows, english_rows = [], []
    for href, text in pdfs:
        is_en = guess_is_english(href, text)
        row = {"English?": "Yes" if is_en else "No", "Link text": text, "PDF URL": href, "Filename": safe_filename_from_url(href)}
        rows.append(row)
        if is_en:
            english_rows.append(row)

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
                if PLAYWRIGHT_READY:
                    with DownloadSession(referer=url) as sess:
                        for i, r in enumerate(to_download, start=1):
                            pdf_url, fname = r["PDF URL"], r["Filename"]
                            try:
                                status.text(f"Downloading {fname} …")
                                content = sess.download_pdf(pdf_url, max_attempts=3, delay_between=add_delay)
                                file_buffers.append((fname, content))
                            except Exception as e:
                                st.warning(f"Failed (PW): {fname} — {e}")
                            finally:
                                progress.progress(i / len(to_download))
                                time.sleep(random.uniform(0.05, 0.15))
                else:
                    # Fallback: direct GET; no JS/attachment handling
                    for i, r in enumerate(to_download, start=1):
                        pdf_url, fname = r["PDF URL"], r["Filename"]
                        try:
                            status.text(f"Downloading {fname} (fallback) …")
                            content = requests_download(pdf_url, referer=url)
                            file_buffers.append((fname, content))
                        except Exception as e:
                            st.warning(f"Failed (fallback): {fname} — {e}")
                        finally:
                            progress.progress(i / len(to_download))
                            time.sleep(random.uniform(0.05, 0.15))
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
