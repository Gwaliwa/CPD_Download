# meta_extractor_fixed.py — UNICEF CPD/COAR enhanced country-year-region detection

import re, unicodedata
from pathlib import Path

# ---- Normalization ----
def _fold_ascii(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[_\-–—/\.]", " ", s)
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"[^\w\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _token_contains(haystack: str, needle: str) -> bool:
    patt = r"\b" + r"\s+".join(map(re.escape, needle.split())) + r"\b"
    return re.search(patt, haystack) is not None

# ---- Alias map ----
COUNTRY_ALIASES = {
    "cote d ivoire": "Côte d’Ivoire", "cote d'ivoire": "Côte d’Ivoire", "ivory coast": "Côte d’Ivoire",
    "viet nam": "Viet Nam", "vietnam": "Viet Nam",
    "lao pdr": "Lao PDR", "laos": "Lao PDR", "lao people s democratic republic": "Lao PDR",
    "timor leste": "Timor-Leste", "east timor": "Timor-Leste",
    "sao tome and principe": "Sao Tome and Principe", "sao tome & principe": "Sao Tome and Principe",
    "cabo verde": "Cape Verde",
    "eswatini": "Eswatini", "swaziland": "Eswatini",
    "turkiye": "Turkey", "tuerkiye": "Turkey", "türkiye": "Turkey",
    "kosova": "Kosovo",
    "state of palestine": "State of Palestine", "palestine": "State of Palestine",
    "dr congo": "Congo", "drc": "Congo", "congo drc": "Congo",
    "car": "Central African Republic",
    "uk": "United Kingdom", "u k": "United Kingdom",
    "usa": "United States", "u s a": "United States", "united states of america": "United States",
}

# ---- Placeholder country/region lists ----
UNICEF_COUNTRIES = {"Nigeria","Niger","Côte d’Ivoire","Turkey","Viet Nam","Lao PDR","Cape Verde",
                    "Timor-Leste","Kosovo","State of Palestine","Central African Republic",
                    "United Kingdom","United States"}
REGION_MAP = {"Nigeria":"WCARO","Niger":"WCARO","Côte d’Ivoire":"WCARO",
              "Turkey":"ECARO","Viet Nam":"EAPRO","Lao PDR":"EAPRO","Cape Verde":"WCARO",
              "Timor-Leste":"EAPRO","Kosovo":"ECARO","State of Palestine":"MENARO",
              "Central African Republic":"WCARO","United Kingdom":"HQ","United States":"HQ"}

# ---- Canonicalization ----
def _canonical_country_from_alias(norm_text: str, unicef_countries: set[str]) -> str | None:
    for alias, canonical in COUNTRY_ALIASES.items():
        if _token_contains(norm_text, _fold_ascii(alias)):
            return canonical if canonical in unicef_countries else None
    for c in sorted(unicef_countries, key=len, reverse=True):
        if c in {"Global","Headquarters","HQ"}: continue
        if _token_contains(norm_text, _fold_ascii(c)):
            return c
    if any(_token_contains(norm_text, k) for k in ["global","headquarters","hq"]):
        return "HQ"
    return None

# ---- First pages text stub (replace with extract_text_hybrid in your app) ----
def first_pages_text(pdf_path: Path | None, ocr_dpi: int = 220, ocr_langs: str = "eng", pages: int = 4) -> str:
    return ""

# ---- Detection ----
def detect_country_smart(filename: str, pdf_first_pages: str, pdf_full_text: str) -> str | None:
    name_norm = _fold_ascii(filename or "")
    c = _canonical_country_from_alias(name_norm, UNICEF_COUNTRIES)
    if c: return c
    fp_norm = _fold_ascii(pdf_first_pages or "")
    c = _canonical_country_from_alias(fp_norm, UNICEF_COUNTRIES)
    if c: return c
    all_norm = _fold_ascii(pdf_full_text or "")
    c = _canonical_country_from_alias(all_norm, UNICEF_COUNTRIES)
    return c

_YEAR_RANGE = re.compile(r"\b(20\d{2}|19\d{2})\s*[–\-/to]{1,3}\s*(20\d{2}|19\d{2})\b")
_YEAR_SINGLE = re.compile(r"\b(20\d{2}|19\d{2})\b")

def _year_from_text_pref_first(text: str):
    if not text: return (None, None)
    m = _YEAR_RANGE.search(text)
    if m: return (m.group(1), m.group(2))
    m = _YEAR_SINGLE.search(text)
    if m: return (m.group(1), None)
    return (None, None)

def extract_year_smart_cover_first(pdf_path: Path | None, first_pages: str, full_text: str, name: str) -> str | None:
    y1,y2 = _year_from_text_pref_first(first_pages or "")
    if y1: return f"{y1}-{y2}" if y2 else y1
    y1,y2 = _year_from_text_pref_first(name or "")
    if y1: return f"{y1}-{y2}" if y2 else y1
    y1,y2 = _year_from_text_pref_first(full_text or "")
    if y1: return f"{y1}-{y2}" if y2 else y1
    return None

def detect_region_from_country_only(country: str | None) -> str | None:
    if not country: return None
    if country in {"HQ","Headquarters","Global"}: return "HQ"
    return REGION_MAP.get(country)

def extract_meta_all(pdf_path: Path | None, full_text: str, name: str):
    fp_txt = first_pages_text(pdf_path)
    year = extract_year_smart_cover_first(pdf_path, fp_txt, full_text or "", name or "")
    country = detect_country_smart(name or "", fp_txt, full_text or "")
    region = detect_region_from_country_only(country)
    return {"year": year, "country": country, "region": region}
