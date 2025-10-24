# app.py — Streamlit + Playwright: collect English PDFs from UNICEF CPD page
# Run: streamlit run app.py

import io, os, re, time, zipfile, random, tempfile
from typing import List, Tuple
from urllib.parse import urljoin, urlparse

import streamlit as st
from bs4 import BeautifulSoup
from playwright.sync_api import (
    sync_playwright, Playwright, Browser, BrowserContext, Page, APIResponse
)

DEFAULT_URL = "https://www.unicef.org/executiveboard/country-programme-documents"

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

# --------- Unified Browser Session ----------
class DownloadSession:
    def __init__(self, referer: str, timeout_ms: int = 30000, headless: bool = True):
        self._p: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self.referer = referer
        self.timeout_ms = timeout_ms
        self.headless = headless

    def __enter__(self):
        self._p = sync_playwright().start()
        self.browser = self._p.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            accept_downloads=True,  # CRITICAL for attachments
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            java_script_enabled=True,
        )
        self.context.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
        })
        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        # Land on referer once so attachment requests are first-party
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
            # Soft navigation; no wait_until=domcontentloaded (some pages are heavy)
            self.page.goto(self.referer)

    # Prep cookies/JS + get HTML for scanning
    def fetch_html(self, url: str) -> str:
        assert self.page is not None
        if self.page.url != url:
            self.page.goto(url)
        try:
            self.page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        return self.page.content()

    # First try an in-context request with Referer
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

    # Then trigger an actual download event via DOM
    def _download_via_event(self, pdf_url: str) -> bytes:
        assert self.page is not None
        self._ensure_on_referer()
        # Wait for the Download while kicking off a DOM click on a temp <a>
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
        suggested = download.suggested_filename or safe_filename_from_url(pdf_url)
        # sanitize suggested filename for weird unicode/slashes
        suggested = suggested.replace("/", "_")
        tmp_path = os.path.join(tmpdir, suggested)
        download.save_as(tmp_path)
        with open(tmp_path, "rb") as f:
            data = f.read()
        try:
            os.remove(tmp_path)
            os.rmdir(tmpdir)
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
    # De-dup
    seen, deduped = set(), []
    for u, t in results:
        if u not in seen:
            seen.add(u)
            deduped.append((u, t))
    return deduped

# -------------- Streamlit UI --------------
st.set_page_config(page_title="UNICEF CPD • English PDF Downloader (Headless Browser)", page_icon="📄", layout="wide")
st.title("📄 UNICEF CPD — English PDF Downloader")

with st.expander("ℹ️ Notes"):
    st.markdown(
        "- Keeps one **Chromium session** (cookies persist) and starts on the CPD page.\n"
        "- Sends a first-party **Referer**.\n"
        "- Handles `Content-Disposition: attachment` via **DOM click + expect_download()** (no goto waiting).\n"
        "- Retries and small jitter to ease rate limits.\n"
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
        with DownloadSession(referer=url) as sess:
            pdfs = collect_pdf_links_with_session(sess, url)
        st.session_state.scan_results = pdfs
        st.success(f"Found {len(pdfs)} PDF link(s) on the page.")
    except Exception as e:
        st.error(f"Error fetching the page: {e}")

pdfs = st.session_state.get("scan_results", [])

if pdfs:
    rows, english_rows = [], []
    for href, text in pdfs:
        is_en = guess_is_english(href, text)
        row = {
            "English?": "Yes" if is_en else "No",
            "Link text": text,
            "PDF URL": href,
            "Filename": safe_filename_from_url(href),
        }
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
                with DownloadSession(referer=url) as sess:
                    # already on CPD page; maintain referer for every attachment
                    for i, r in enumerate(to_download, start=1):
                        pdf_url, fname = r["PDF URL"], r["Filename"]
                        try:
                            status.text(f"Downloading {fname} …")
                            content = sess.download_pdf(
                                pdf_url,
                                max_attempts=3,
                                delay_between=add_delay
                            )
                            file_buffers.append((fname, content))
                        except Exception as e:
                            st.warning(f"Failed: {fname} — {e}")
                        finally:
                            progress.progress(i / len(to_download))
                            time.sleep(random.uniform(0.05, 0.15))
            finally:
                status.empty()
                progress.empty()

            if file_buffers:
                zip_bytes = make_zip(file_buffers)
                st.download_button(
                    label="📦 Download ZIP",
                    data=zip_bytes,
                    file_name="unicef_cpd_english_pdfs.zip",
                    mime="application/zip",
                )
                st.success(f"ZIP is ready with {len(file_buffers)}/{len(to_download)} files.")

st.caption("If Playwright is blocked in your env, I can give you a Selenium (undetected-chromedriver) version.")
