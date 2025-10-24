# app.py — Streamlit + Playwright HTTP client only (no browser): collect English PDFs from UNICEF CPD page
# Run: streamlit run app.py

import io, os, re, time, zipfile, random
from typing import List, Tuple
from urllib.parse import urljoin, urlparse

import streamlit as st
from bs4 import BeautifulSoup

# IMPORTANT: we will NOT launch a browser; only use Playwright's request client.
from playwright.sync_api import sync_playwright, APIResponse

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

# --------- HTTP-only session (no Chromium) ----------
class HttpSession:
    """
    Single Playwright APIRequestContext shared for scanning + downloads.
    No browser is launched, so it works on Streamlit Cloud without chromium.
    """
    def __init__(self, referer: str, timeout_ms: int = 30000):
        self.referer = referer
        self.timeout_ms = timeout_ms
        self.pw = None
        self.ctx = None

    def __enter__(self):
        self.pw = sync_playwright().start()
        # Create a request context with a strong UA and default headers.
        self.ctx = self.pw.request.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "DNT": "1",
            },
            timeout=self.timeout_ms
        )
        # Seed cookies / first-party context by requesting the CPD page.
        self._prime_cookies(self.referer)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.ctx: self.ctx.dispose()
        finally:
            if self.pw: self.pw.stop()

    def _prime_cookies(self, url: str):
        # Just GET the referer page so any cookies are set for the site.
        try:
            _ = self.ctx.get(url)
        except Exception:
            pass

    def fetch_html(self, url: str) -> str:
        resp: APIResponse = self.ctx.get(url, headers={"Referer": self.referer})
        if not resp.ok:
            raise RuntimeError(f"GET {url} -> {resp.status}")
        return resp.text()

    def download_pdf(self, pdf_url: str, max_attempts: int = 3, delay_between: float = 0.0) -> bytes:
        """
        Download a PDF with a first-party Referer and PDF-friendly Accept header.
        Retries with light jitter to handle transient 403/429.
        """
        last_err = None
        for _ in range(max_attempts):
            try:
                resp: APIResponse = self.ctx.get(
                    pdf_url,
                    headers={
                        "Referer": self.referer,
                        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-site",
                        "Upgrade-Insecure-Requests": "1",
                    }
                )
                if resp.ok:
                    ctype = (resp.headers.get("content-type") or "").lower()
                    # Some files return octet-stream; accept both.
                    if ("application/pdf" in ctype) or ("application/octet-stream" in ctype) or (ctype == ""):
                        return resp.body()
                    # If content-type is odd but body is large, accept anyway.
                    body = resp.body()
                    if body and len(body) > 1024:
                        return body
                last_err = RuntimeError(f"HTTP {resp.status} for {pdf_url}")
            except Exception as e:
                last_err = e
            time.sleep((delay_between or 0.0) + random.uniform(0.1, 0.4))
        if last_err:
            raise last_err
        raise RuntimeError("Unknown download failure")

def collect_pdf_links(session: HttpSession, page_url: str) -> List[Tuple[str, str]]:
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
st.set_page_config(page_title="UNICEF CPD • English PDF Downloader (HTTP-only)", page_icon="📄", layout="wide")
st.title("📄 UNICEF CPD — English PDF Downloader (no browser)")

with st.expander("ℹ️ Notes"):
    st.markdown(
        "- Uses **Playwright’s HTTP client only** — **no Chromium** required (works on Streamlit Cloud).\n"
        "- Seeds cookies by first requesting the CPD page, then downloads PDFs with a valid **Referer**.\n"
        "- Heuristically filters **English** documents.\n"
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
        with HttpSession(referer=url) as sess:
            pdfs = collect_pdf_links(sess, url)
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
                with HttpSession(referer=url) as sess:
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

st.caption("If some files still 403, I can add a pure-requests fallback or pre-signed redirects handling.")
