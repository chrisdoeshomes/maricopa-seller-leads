#!/usr/bin/env python3
"""
Maricopa County Motivated Seller Lead Scraper
Scrapes the Maricopa County Recorder's Office for distressed-property document types,
then enriches each lead with parcel/owner data from the Assessor's bulk download.

Sources:
  - Recorder (Playwright): https://recorder.maricopa.gov/recording/document-search.html
  - Assessor bulk CSV:      https://mcassessor.maricopa.gov/page/data_sales/
  - Assessor parcel API:    https://mcassessor.maricopa.gov/mcs.php?q=<APN>
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import unicodedata
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Optional imports (handle gracefully if playwright not installed yet)
# ---------------------------------------------------------------------------
try:
    from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOOKBACK_DAYS: int = int(os.getenv("LOOKBACK_DAYS", "7"))
RECORDER_BASE   = "https://recorder.maricopa.gov"
RECORDER_SEARCH = f"{RECORDER_BASE}/recording/document-search.html"
RECORDER_LEGACY_SEARCH = f"{RECORDER_BASE}/recdocdata/"
ASSESSOR_BASE   = "https://mcassessor.maricopa.gov"
ASSESSOR_PARCEL = f"{ASSESSOR_BASE}/mcs.php"
ASSESSOR_DOWNLOADS = f"{ASSESSOR_BASE}/page/data_sales/"

# Output paths
REPO_ROOT     = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR      = REPO_ROOT / "data"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Document-type catalogue
# ---------------------------------------------------------------------------
DOC_TYPES: dict[str, dict] = {
    # Foreclosure / Lis Pendens
    "LP":       {"label": "Lis Pendens",             "cat": "foreclosure", "weight": 10},
    "NOFC":     {"label": "Notice of Foreclosure",   "cat": "foreclosure", "weight": 10},
    "TAXDEED":  {"label": "Tax Deed",                "cat": "tax",         "weight": 10},
    "RELLP":    {"label": "Release of Lis Pendens",  "cat": "release",     "weight":  5},
    # Judgments
    "JUD":      {"label": "Judgment",                "cat": "judgment",    "weight": 10},
    "CCJ":      {"label": "Certified Judgment",      "cat": "judgment",    "weight": 10},
    "DRJUD":    {"label": "Domestic Judgment",       "cat": "judgment",    "weight": 10},
    # Tax / Federal Liens
    "LNCORPTX": {"label": "Corp Tax Lien",           "cat": "tax_lien",    "weight": 10},
    "LNIRS":    {"label": "IRS Lien",                "cat": "tax_lien",    "weight": 10},
    "LNFED":    {"label": "Federal Lien",            "cat": "tax_lien",    "weight": 10},
    # Property Liens
    "LN":       {"label": "Lien",                    "cat": "lien",        "weight": 10},
    "LNMECH":   {"label": "Mechanic Lien",           "cat": "lien",        "weight": 10},
    "LNHOA":    {"label": "HOA Lien",                "cat": "lien",        "weight": 10},
    "MEDLN":    {"label": "Medicaid Lien",           "cat": "lien",        "weight": 10},
    # Other
    "PRO":      {"label": "Probate",                 "cat": "probate",     "weight": 10},
    "NOC":      {"label": "Notice of Commencement",  "cat": "construction","weight":  5},
}

# Map recorder document-code strings to our keys (handles partial/alias matches)
DOC_CODE_MAP: dict[str, str] = {
    "LIS PENDENS":             "LP",
    "LIS PENDEN":              "LP",
    "NOTICE OF FORECLOSURE":   "NOFC",
    "FORECLOSURE":             "NOFC",
    "TAX DEED":                "TAXDEED",
    "RELEASE LIS PENDENS":     "RELLP",
    "RELEASE OF LIS PENDENS":  "RELLP",
    "JUDGMENT":                "JUD",
    "CERTIFIED JUDGMENT":      "CCJ",
    "DOMESTIC JUDGMENT":       "DRJUD",
    "CORP TAX LIEN":           "LNCORPTX",
    "CORPORATE TAX LIEN":      "LNCORPTX",
    "IRS LIEN":                "LNIRS",
    "FEDERAL LIEN":            "LNFED",
    "LIEN":                    "LN",
    "MECHANIC LIEN":           "LNMECH",
    "MECHANICS LIEN":          "LNMECH",
    "HOA LIEN":                "LNHOA",
    "HOMEOWNERS ASSOCIATION":  "LNHOA",
    "MEDICAID LIEN":           "MEDLN",
    "PROBATE":                 "PRO",
    "NOTICE OF COMMENCEMENT":  "NOC",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fetcher")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    """Normalise to upper-case ASCII, strip punctuation."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9 ]", " ", s.upper()).strip()


def parse_amount(raw: str) -> float | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9.]", "", raw)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def retry(fn, attempts: int = 3, delay: float = 2.0):
    """Synchronous retry wrapper."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    return None


def map_doc_code(raw_type: str) -> str | None:
    """Map a raw recorder doc-type string to our internal code.

    Catalogue keys are sorted longest-first so specific phrases (e.g.
    'HOA LIEN', 'RELEASE LIS PENDENS') are tested before shorter ones
    (e.g. 'LIEN', 'LIS PENDENS').  We only check whether the *key*
    appears inside the *input* — never the reverse — so "Lis Pendens"
    cannot accidentally match "Release of Lis Pendens".
    """
    upper = norm(raw_type)
    if not upper:
        return None
    for k, v in sorted(DOC_CODE_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        if norm(k) in upper:
            return v
    # Exact or prefix match against internal code keys
    for k in sorted(DOC_TYPES.keys(), key=len, reverse=True):
        if upper == k or upper.startswith(k + " "):
            return k
    return None


# ---------------------------------------------------------------------------
# Assessor bulk-data loader
# ---------------------------------------------------------------------------

class ParcelLookup:
    """
    Loads the Assessor's free bulk CSV/DBF and builds owner-name lookup tables.
    Falls back to per-parcel API if bulk download is unavailable.
    """

    def __init__(self):
        self._by_apn:   dict[str, dict] = {}
        self._by_owner: dict[str, list[dict]] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load(self):
        """Try bulk download first, then skip (API fallback used per-record)."""
        log.info("Loading Assessor bulk parcel data …")
        success = self._try_bulk_download()
        if not success:
            log.warning("Bulk parcel download unavailable; will use per-parcel API fallback.")

    def lookup_by_apn(self, apn: str) -> dict | None:
        return self._by_apn.get(apn.replace("-", "").replace(" ", ""))

    def lookup_by_owner(self, owner_raw: str) -> dict | None:
        if not owner_raw:
            return None
        key = norm(owner_raw)
        # Try all name orderings
        for variant in self._name_variants(key):
            results = self._by_owner.get(variant)
            if results:
                return results[0]
        return None

    def fetch_parcel_api(self, q: str) -> dict | None:
        """Fall back to Assessor parcel search API."""
        def _get():
            r = requests.get(
                ASSESSOR_PARCEL,
                params={"q": q},
                headers={"User-Agent": "MotivatedSellerScraper/1.0"},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            if hits:
                src = hits[0].get("_source", {})
                return self._normalise_parcel(src)
            return None
        return retry(_get)

    # ------------------------------------------------------------------
    # Bulk download helpers
    # ------------------------------------------------------------------

    def _try_bulk_download(self) -> bool:
        """Scrape the Assessor's data-downloads page for a CSV/ZIP link."""
        try:
            r = requests.get(
                ASSESSOR_DOWNLOADS,
                headers={"User-Agent": "MotivatedSellerScraper/1.0"},
                timeout=20,
            )
            r.raise_for_status()
        except Exception as exc:
            log.warning("Could not fetch Assessor downloads page: %s", exc)
            return False

        soup = BeautifulSoup(r.text, "lxml")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(ext in href for ext in (".zip", ".csv", ".dbf")):
                full = a["href"] if a["href"].startswith("http") else ASSESSOR_BASE + "/" + a["href"].lstrip("/")
                links.append(full)

        # Prefer the "secured master" / parcel ownership files
        priority_keywords = ["owner", "parcel", "master", "secured", "residential"]
        ranked = sorted(links, key=lambda u: sum(k in u.lower() for k in priority_keywords), reverse=True)

        for url in ranked[:5]:
            log.info("Attempting bulk download: %s", url)
            result = retry(lambda u=url: self._download_and_parse(u))
            if result and len(self._by_apn) > 0:
                log.info("Loaded %d parcels from bulk file.", len(self._by_apn))
                return True

        return False

    def _download_and_parse(self, url: str) -> bool:
        r = requests.get(
            url,
            headers={"User-Agent": "MotivatedSellerScraper/1.0"},
            timeout=120,
            stream=True,
        )
        r.raise_for_status()
        content = r.content

        # ZIP container?
        if url.lower().endswith(".zip") or content[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    ext = name.lower().rsplit(".", 1)[-1]
                    if ext == "csv":
                        with zf.open(name) as f:
                            self._parse_csv(f)
                            return True
                    elif ext == "dbf":
                        with zf.open(name) as f:
                            tmp = Path("/tmp") / name
                            tmp.write_bytes(f.read())
                        self._parse_dbf(tmp)
                        return True
        elif url.lower().endswith(".csv"):
            self._parse_csv(io.BytesIO(content))
            return True
        elif url.lower().endswith(".dbf"):
            tmp = Path("/tmp/parcel_bulk.dbf")
            tmp.write_bytes(content)
            self._parse_dbf(tmp)
            return True

        return False

    def _parse_csv(self, f):
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
        for row in reader:
            try:
                parcel = self._normalise_parcel(row)
                self._index_parcel(parcel)
            except Exception:
                pass

    def _parse_dbf(self, path: Path):
        if not HAS_DBF:
            log.warning("dbfread not installed; skipping DBF parse.")
            return
        try:
            table = DBF(str(path), encoding="latin-1", ignore_missing_memofile=True)
            for row in table:
                try:
                    r = {k: (str(v).strip() if v is not None else "") for k, v in row.items()}
                    parcel = self._normalise_parcel(r)
                    self._index_parcel(parcel)
                except Exception:
                    pass
        except Exception as exc:
            log.warning("DBF parse error: %s", exc)

    # ------------------------------------------------------------------
    # Normalisation & indexing
    # ------------------------------------------------------------------

    _APN_ALIASES    = ("APN", "PARCELNO", "PARCEL_NO", "PARCEL", "ASSESSOR_NO")
    _OWNER_ALIASES  = ("OWNER", "OWN1", "OWNERNAME", "OWNER_NAME", "OWNER1")
    _SADDR_ALIASES  = ("SITE_ADDR", "SITEADDR", "SITE_ADDRESS", "PROP_ADDR", "PROPADDR", "ADDRESS")
    _SCITY_ALIASES  = ("SITE_CITY", "SITECITY", "PROP_CITY", "CITY_NAME")
    _SZIP_ALIASES   = ("SITE_ZIP",  "SITEZIP",  "PROP_ZIP",  "ZIP5")
    _MADDR_ALIASES  = ("ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILADDR", "MAILING_ADDRESS")
    _MCITY_ALIASES  = ("CITY",   "MAILCITY", "MAIL_CITY")
    _MSTATE_ALIASES = ("STATE",  "MAILSTATE","MAIL_STATE")
    _MZIP_ALIASES   = ("ZIP",    "MAILZIP",  "MAIL_ZIP")

    def _get(self, d: dict, aliases: tuple) -> str:
        for a in aliases:
            v = d.get(a, d.get(a.lower(), ""))
            if v:
                return str(v).strip()
        return ""

    def _normalise_parcel(self, d: dict) -> dict:
        # Upper-case all keys for uniform access
        ud = {k.upper(): v for k, v in d.items()}
        return {
            "apn":          self._get(ud, self._APN_ALIASES),
            "owner":        self._get(ud, self._OWNER_ALIASES),
            "prop_address": self._get(ud, self._SADDR_ALIASES),
            "prop_city":    self._get(ud, self._SCITY_ALIASES),
            "prop_state":   ud.get("SITE_STATE", "AZ"),
            "prop_zip":     self._get(ud, self._SZIP_ALIASES),
            "mail_address": self._get(ud, self._MADDR_ALIASES),
            "mail_city":    self._get(ud, self._MCITY_ALIASES),
            "mail_state":   self._get(ud, self._MSTATE_ALIASES),
            "mail_zip":     self._get(ud, self._MZIP_ALIASES),
        }

    def _index_parcel(self, p: dict):
        if p["apn"]:
            self._by_apn[p["apn"].replace("-", "")] = p
        if p["owner"]:
            for variant in self._name_variants(norm(p["owner"])):
                self._by_owner.setdefault(variant, []).append(p)

    @staticmethod
    def _name_variants(name: str) -> list[str]:
        """Generate FIRST LAST / LAST FIRST / LAST, FIRST variants."""
        parts = name.split()
        variants = [name]
        if len(parts) >= 2:
            variants.append(" ".join(parts[1:] + [parts[0]]))   # LAST FIRST
            variants.append(parts[-1] + " " + " ".join(parts[:-1]))  # LAST FIRST
        return list(dict.fromkeys(variants))  # deduplicate, preserve order


# ---------------------------------------------------------------------------
# Recorder Scraper  (Playwright async)
# ---------------------------------------------------------------------------

class RecorderScraper:
    """
    Scrapes the Maricopa County Recorder's Office document-search interface.

    Strategy:
      1. Try the modern React/Angular document search (recorder.maricopa.gov/recording/document-search.html)
         which uses a REST JSON API under the hood.
      2. Fall back to the legacy ASP.NET search (recorder.maricopa.gov/recdocdata/)
         which uses __doPostBack form submissions.
    """

    def __init__(self, start: date, end: date):
        self.start = start
        self.end   = end
        self.results: list[dict] = []

    async def run(self) -> list[dict]:
        if not HAS_PLAYWRIGHT:
            log.error("Playwright is not installed. Run: pip install playwright && playwright install chromium")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = await context.new_page()

            try:
                await self._scrape_modern_api(page)
            except Exception as exc:
                log.warning("Modern API scrape failed (%s); trying legacy form …", exc)
                try:
                    await self._scrape_legacy_form(page)
                except Exception as exc2:
                    log.error("Legacy form scrape also failed: %s", exc2)

            await browser.close()

        log.info("Recorder scrape complete: %d raw records", len(self.results))
        return self.results

    # ------------------------------------------------------------------
    # Strategy 1 – intercept the underlying JSON REST call
    # ------------------------------------------------------------------

    async def _scrape_modern_api(self, page: Page):
        """
        The modern search page fires XHR/fetch requests to an internal API.
        We intercept those responses to harvest JSON data for each doc type.
        """
        captured: list[dict] = []

        async def handle_response(response):
            try:
                ct = response.headers.get("content-type", "")
                if "json" in ct and "recorder.maricopa.gov" in response.url:
                    data = await response.json()
                    if isinstance(data, (list, dict)):
                        records = data if isinstance(data, list) else data.get("results", data.get("documents", []))
                        if isinstance(records, list):
                            captured.extend(records)
            except Exception:
                pass

        page.on("response", handle_response)

        start_str = self.start.strftime("%m/%d/%Y")
        end_str   = self.end.strftime("%m/%d/%Y")

        for code, info in DOC_TYPES.items():
            log.info("  → Querying recorder for %s (%s)", code, info["label"])
            try:
                await page.goto(RECORDER_SEARCH, wait_until="networkidle", timeout=30_000)
                await asyncio.sleep(1)

                # Try to locate a date-range input + doc-type dropdown/input
                # (selectors may need tuning when the UI changes)
                for sel in ['input[placeholder*="Document"]', 'input[id*="type"]', 'input[name*="type"]']:
                    try:
                        await page.fill(sel, info["label"], timeout=3000)
                        break
                    except Exception:
                        pass

                for sel in ['input[placeholder*="From"]', 'input[id*="from"]', 'input[name*="begin"]']:
                    try:
                        await page.fill(sel, start_str, timeout=3000)
                        break
                    except Exception:
                        pass

                for sel in ['input[placeholder*="To"]', 'input[id*="to"]', 'input[name*="end"]']:
                    try:
                        await page.fill(sel, end_str, timeout=3000)
                        break
                    except Exception:
                        pass

                for sel in ['button[type="submit"]', 'button:has-text("Search")', 'input[type="submit"]']:
                    try:
                        await page.click(sel, timeout=3000)
                        break
                    except Exception:
                        pass

                await page.wait_for_load_state("networkidle", timeout=15_000)
                await asyncio.sleep(2)

                # Parse whatever loaded in the DOM
                html = await page.content()
                parsed = self._parse_results_html(html, code)
                self.results.extend(parsed)

            except PWTimeout:
                log.warning("Timeout querying recorder for %s", code)
            except Exception as exc:
                log.warning("Error querying recorder for %s: %s", code, exc)

        # Also ingest any XHR-captured JSON
        for item in captured:
            rec = self._parse_json_record(item)
            if rec:
                self.results.append(rec)

    # ------------------------------------------------------------------
    # Strategy 2 – legacy ASP.NET __doPostBack form
    # ------------------------------------------------------------------

    async def _scrape_legacy_form(self, page: Page):
        """
        The legacy Recorder search uses ASP.NET Web Forms with __doPostBack.
        We submit by document-type code and date range, then parse the HTML table.
        """
        start_str = self.start.strftime("%m/%d/%Y")
        end_str   = self.end.strftime("%m/%d/%Y")

        for code, info in DOC_TYPES.items():
            log.info("  → Legacy form query for %s", code)
            for attempt in range(3):
                try:
                    await page.goto(RECORDER_LEGACY_SEARCH, wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(1)

                    # Fill begin date
                    for sel in ["#BeginDate", 'input[name="BeginDate"]', 'input[id*="Begin"]']:
                        try:
                            await page.fill(sel, start_str, timeout=3000)
                            break
                        except Exception:
                            pass

                    # Fill end date
                    for sel in ["#EndDate", 'input[name="EndDate"]', 'input[id*="End"]']:
                        try:
                            await page.fill(sel, end_str, timeout=3000)
                            break
                        except Exception:
                            pass

                    # Set doc type
                    for sel in ["#DocType", 'select[name="DocType"]', 'select[id*="Doc"]']:
                        try:
                            await page.select_option(sel, label=info["label"], timeout=3000)
                            break
                        except Exception:
                            pass
                    for sel in ["#DocCode", 'input[name="DocCode"]', 'input[id*="Code"]']:
                        try:
                            await page.fill(sel, code, timeout=3000)
                            break
                        except Exception:
                            pass

                    # Submit
                    for sel in ['input[type="submit"]', 'button[type="submit"]', 'button:has-text("Search")']:
                        try:
                            await page.click(sel, timeout=3000)
                            break
                        except Exception:
                            pass

                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    await asyncio.sleep(1)

                    html = await page.content()
                    parsed = self._parse_results_html(html, code)
                    self.results.extend(parsed)
                    log.info("    %s → %d records", code, len(parsed))
                    break  # success

                except PWTimeout:
                    log.warning("  Timeout on %s attempt %d", code, attempt + 1)
                    await asyncio.sleep(3)
                except Exception as exc:
                    log.warning("  Error on %s attempt %d: %s", code, attempt + 1, exc)
                    await asyncio.sleep(3)

    # ------------------------------------------------------------------
    # HTML table parser (handles both modern and legacy DOM)
    # ------------------------------------------------------------------

    def _parse_results_html(self, html: str, hint_code: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        records = []

        # Find the primary results table
        tables = soup.find_all("table")
        if not tables:
            # Try div-based result cards (modern React)
            cards = soup.select("[class*='result'], [class*='record'], [class*='document']")
            for card in cards:
                rec = self._parse_card(card, hint_code)
                if rec:
                    records.append(rec)
            return records

        # Pick the largest table (most rows)
        tbl = max(tables, key=lambda t: len(t.find_all("tr")))
        rows = tbl.find_all("tr")
        if not rows:
            return records

        # Detect headers
        header_row = rows[0]
        headers = [th.get_text(strip=True).upper() for th in header_row.find_all(["th", "td"])]

        def col(cells, *names):
            for n in names:
                for i, h in enumerate(headers):
                    if n in h and i < len(cells):
                        return cells[i].get_text(strip=True)
            return ""

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            try:
                raw_type = col(cells, "DOC TYPE", "DOCTYPE", "TYPE", "DOCUMENT")
                doc_code = map_doc_code(raw_type) or hint_code
                if doc_code not in DOC_TYPES:
                    continue

                # Try to extract a direct link
                link = ""
                for a in row.find_all("a", href=True):
                    href = a["href"]
                    if not href.startswith("javascript"):
                        link = href if href.startswith("http") else RECORDER_BASE + href
                        break

                rec = {
                    "doc_num":  col(cells, "DOC NUM", "DOCNUM", "NUMBER", "RECORDING"),
                    "doc_type": raw_type or DOC_TYPES[doc_code]["label"],
                    "cat":      DOC_TYPES[doc_code]["cat"],
                    "cat_label":DOC_TYPES[doc_code]["label"],
                    "filed":    col(cells, "DATE", "FILED", "RECORDED"),
                    "owner":    col(cells, "GRANTOR", "OWNER", "FROM"),
                    "grantee":  col(cells, "GRANTEE", "TO", "LENDER"),
                    "legal":    col(cells, "LEGAL", "DESCRIPTION", "PARCEL"),
                    "amount":   parse_amount(col(cells, "AMOUNT", "DEBT", "VALUE", "CONSIDERATION")),
                    "clerk_url":link,
                    "_code":    doc_code,
                }
                if rec["doc_num"] or rec["owner"]:
                    records.append(rec)
            except Exception:
                pass

        return records

    def _parse_card(self, card, hint_code: str) -> dict | None:
        """Parse a div/card result (modern React UI)."""
        try:
            text = card.get_text(" ", strip=True)
            link_tag = card.find("a", href=True)
            link = ""
            if link_tag:
                href = link_tag["href"]
                link = href if href.startswith("http") else RECORDER_BASE + href

            # Extract doc number via regex
            doc_num_m = re.search(r"(\d{4}-\d{6,}|\d{9,})", text)
            doc_num = doc_num_m.group(1) if doc_num_m else ""

            return {
                "doc_num":   doc_num,
                "doc_type":  DOC_TYPES[hint_code]["label"],
                "cat":       DOC_TYPES[hint_code]["cat"],
                "cat_label": DOC_TYPES[hint_code]["label"],
                "filed":     "",
                "owner":     "",
                "grantee":   "",
                "legal":     text[:200],
                "amount":    None,
                "clerk_url": link,
                "_code":     hint_code,
            }
        except Exception:
            return None

    def _parse_json_record(self, item: dict) -> dict | None:
        """Parse a record intercepted from an XHR JSON response."""
        try:
            raw_type = (
                item.get("documentType") or item.get("docType") or
                item.get("document_type") or item.get("type", "")
            )
            code = map_doc_code(raw_type)
            if not code:
                return None
            return {
                "doc_num":   str(item.get("documentNumber") or item.get("docNum") or item.get("id", "")),
                "doc_type":  raw_type,
                "cat":       DOC_TYPES[code]["cat"],
                "cat_label": DOC_TYPES[code]["label"],
                "filed":     str(item.get("filedDate") or item.get("recordedDate") or item.get("date", "")),
                "owner":     str(item.get("grantor") or item.get("owner") or ""),
                "grantee":   str(item.get("grantee") or ""),
                "legal":     str(item.get("legalDescription") or item.get("legal") or ""),
                "amount":    parse_amount(str(item.get("amount") or item.get("consideration") or "")),
                "clerk_url": str(item.get("url") or item.get("documentUrl") or ""),
                "_code":     code,
            }
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Static/REST fallback – Requests + BeautifulSoup
# ---------------------------------------------------------------------------

class RecorderRestFallback:
    """
    Tries the Recorder's known REST-ish endpoints via plain requests.
    Used when Playwright is unavailable or returns nothing.
    """

    SEARCH_ENDPOINT = f"{RECORDER_BASE}/recording/api/search"  # speculative; real endpoint discovered at runtime
    DATE_RANGE_URL  = f"{RECORDER_BASE}/recdocdata/results.aspx"

    def __init__(self, start: date, end: date):
        self.start = start
        self.end   = end
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; MotivatedSellerBot/1.0)",
            "Accept": "application/json, text/html, */*",
        })

    def run(self) -> list[dict]:
        results = []
        start_str = self.start.strftime("%m/%d/%Y")
        end_str   = self.end.strftime("%m/%d/%Y")

        for code, info in DOC_TYPES.items():
            log.info("  → REST fallback for %s", code)
            recs = retry(lambda c=code, i=info, s=start_str, e=end_str: self._query(c, i, s, e))
            if recs:
                results.extend(recs)
        return results

    def _query(self, code: str, info: dict, start_str: str, end_str: str) -> list[dict]:
        # Try JSON endpoint first
        try:
            r = self.session.post(
                self.SEARCH_ENDPOINT,
                json={
                    "docType": code,
                    "beginDate": start_str,
                    "endDate": end_str,
                    "pageSize": 500,
                },
                timeout=20,
            )
            if r.ok and "json" in r.headers.get("content-type", ""):
                data = r.json()
                return self._parse_json(data, code, info)
        except Exception:
            pass

        # Try HTML POST (legacy __doPostBack)
        try:
            # First GET to harvest viewstate
            r0 = self.session.get(self.DATE_RANGE_URL, timeout=20)
            soup0 = BeautifulSoup(r0.text, "lxml")
            vs  = soup0.find("input", {"name": "__VIEWSTATE"})
            evv = soup0.find("input", {"name": "__EVENTVALIDATION"})
            payload = {
                "__EVENTTARGET":    "",
                "__EVENTARGUMENT":  "",
                "__VIEWSTATE":      vs["value"] if vs else "",
                "__EVENTVALIDATION":evv["value"] if evv else "",
                "BeginDate":        start_str,
                "EndDate":          end_str,
                "DocCode":          code,
                "btnSearch":        "Search",
            }
            r1 = self.session.post(self.DATE_RANGE_URL, data=payload, timeout=30)
            return self._parse_html(r1.text, code, info)
        except Exception as exc:
            log.debug("REST fallback HTML post failed for %s: %s", code, exc)
            return []

    def _parse_json(self, data: Any, code: str, info: dict) -> list[dict]:
        recs = []
        items = data if isinstance(data, list) else data.get("results", data.get("documents", []))
        for item in (items or []):
            try:
                recs.append({
                    "doc_num":   str(item.get("documentNumber") or item.get("id", "")),
                    "doc_type":  item.get("documentType", info["label"]),
                    "cat":       info["cat"],
                    "cat_label": info["label"],
                    "filed":     str(item.get("filedDate") or item.get("date", "")),
                    "owner":     str(item.get("grantor") or item.get("owner", "")),
                    "grantee":   str(item.get("grantee", "")),
                    "legal":     str(item.get("legalDescription", "")),
                    "amount":    parse_amount(str(item.get("amount", ""))),
                    "clerk_url": str(item.get("url", "")),
                    "_code":     code,
                })
            except Exception:
                pass
        return recs

    def _parse_html(self, html: str, code: str, info: dict) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        tbl = soup.find("table", id=lambda x: x and ("result" in x.lower() or "grid" in x.lower()))
        if not tbl:
            tbls = soup.find_all("table")
            tbl = max(tbls, key=lambda t: len(t.find_all("tr"))) if tbls else None
        if not tbl:
            return []

        rows = tbl.find_all("tr")
        if not rows:
            return []

        headers = [th.get_text(strip=True).upper() for th in rows[0].find_all(["th", "td"])]

        def col(cells, *names):
            for n in names:
                for i, h in enumerate(headers):
                    if n in h and i < len(cells):
                        return cells[i].get_text(strip=True)
            return ""

        results = []
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            try:
                link_tag = row.find("a", href=True)
                link = ""
                if link_tag:
                    href = link_tag["href"]
                    link = href if href.startswith("http") else RECORDER_BASE + href

                results.append({
                    "doc_num":   col(cells, "DOC NUM", "NUMBER", "RECORDING"),
                    "doc_type":  col(cells, "TYPE", "DOCTYPE") or info["label"],
                    "cat":       info["cat"],
                    "cat_label": info["label"],
                    "filed":     col(cells, "DATE", "FILED", "RECORDED"),
                    "owner":     col(cells, "GRANTOR", "OWNER"),
                    "grantee":   col(cells, "GRANTEE"),
                    "legal":     col(cells, "LEGAL", "DESCRIPTION"),
                    "amount":    parse_amount(col(cells, "AMOUNT", "CONSIDERATION")),
                    "clerk_url": link,
                    "_code":     code,
                })
            except Exception:
                pass
        return results


# ---------------------------------------------------------------------------
# Lead Scoring & Flagging
# ---------------------------------------------------------------------------

def score_record(rec: dict) -> tuple[int, list[str]]:
    """Return (score 0-100, flags list)."""
    flags: list[str] = []
    score = 30  # base

    code  = rec.get("_code", "")
    cat   = rec.get("cat", "")
    amount = rec.get("amount")
    filed  = rec.get("filed", "")
    owner  = rec.get("owner", "")
    prop   = rec.get("prop_address", "")

    # Document-type flags
    if code == "LP" or cat == "foreclosure":
        flags.append("Lis pendens")
        score += DOC_TYPES.get(code, {}).get("weight", 10)

    if code == "NOFC":
        flags.append("Pre-foreclosure")
        score += 10

    if cat == "judgment":
        flags.append("Judgment lien")
        score += 10

    if cat == "tax_lien" or code == "TAXDEED":
        flags.append("Tax lien")
        score += 10

    if code == "LNMECH":
        flags.append("Mechanic lien")
        score += 10

    if code == "PRO":
        flags.append("Probate / estate")
        score += 10

    # LP + foreclosure combo
    lp_codes   = {"LP", "RELLP"}
    fc_codes   = {"NOFC", "TAXDEED"}
    if code in lp_codes and any(r.get("_code") in fc_codes and r.get("owner") == owner
                                 for r in [rec]):  # simple self-check; cross-check done later
        score += 20

    # Amount bonuses
    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    # Recency
    try:
        filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d") if "-" in filed else datetime.strptime(filed[:10], "%m/%d/%Y")
        if (date.today() - filed_dt.date()).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    # Has address
    if prop:
        score += 5

    # LLC / corp
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|LP|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")

    # Deduplicate flags preserving order
    seen = set()
    clean_flags = []
    for f in flags:
        if f not in seen:
            seen.add(f)
            clean_flags.append(f)

    return min(score, 100), clean_flags


def apply_lp_fc_combo_bonus(records: list[dict]) -> list[dict]:
    """Apply +20 to records where the same owner has both LP and FC docs."""
    by_owner: dict[str, list[int]] = {}
    for i, r in enumerate(records):
        key = norm(r.get("owner", ""))
        if key:
            by_owner.setdefault(key, []).append(i)

    lp_owners  = {norm(r.get("owner", "")) for r in records if r.get("_code") == "LP"}
    fc_owners  = {norm(r.get("owner", "")) for r in records if r.get("_code") in ("NOFC", "TAXDEED")}
    combo_owners = lp_owners & fc_owners

    for r in records:
        key = norm(r.get("owner", ""))
        if key in combo_owners and "Pre-foreclosure" not in r["flags"]:
            r["flags"].insert(0, "Pre-foreclosure")
            r["score"] = min(r["score"] + 20, 100)

    return records


# ---------------------------------------------------------------------------
# Name parser for GHL CSV
# ---------------------------------------------------------------------------

def split_name(full: str) -> tuple[str, str]:
    """Return (first, last) from an owner name string."""
    if not full:
        return "", ""
    # Remove suffixes
    clean = re.sub(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE|ET AL|ET UX|JR|SR|II|III|IV)\b", "", full.upper()).strip()
    # Handle "LAST, FIRST" format
    if "," in clean:
        parts = clean.split(",", 1)
        last = parts[0].strip().title()
        first = parts[1].strip().title()
        return first, last
    parts = clean.split()
    if len(parts) == 1:
        return "", parts[0].title()
    return parts[0].title(), " ".join(parts[1:]).title()


# ---------------------------------------------------------------------------
# Build clerk URL if missing
# ---------------------------------------------------------------------------

def build_clerk_url(doc_num: str) -> str:
    if not doc_num:
        return ""
    clean = doc_num.replace("-", "").replace(" ", "")
    # Recorder document viewer pattern
    return f"{RECORDER_BASE}/recdocdata/getimage.aspx?rec={clean}"


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main():
    log.info("=" * 60)
    log.info("Maricopa County Motivated Seller Scraper")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    log.info("Date range: %s → %s", start_dt, end_dt)

    # 1. Load parcel data
    parcels = ParcelLookup()
    parcels.load()

    # 2. Scrape recorder
    raw_records: list[dict] = []

    if HAS_PLAYWRIGHT:
        scraper = RecorderScraper(start_dt, end_dt)
        raw_records = await scraper.run()
    else:
        log.warning("Playwright unavailable; using REST fallback.")

    if not raw_records:
        log.info("Playwright returned nothing; trying REST fallback …")
        fallback = RecorderRestFallback(start_dt, end_dt)
        raw_records = fallback.run()

    log.info("Total raw records before dedup: %d", len(raw_records))

    # 3. Deduplicate by doc_num
    seen_docs: set[str] = set()
    unique: list[dict] = []
    for r in raw_records:
        key = r.get("doc_num", "") or id(r)
        if key not in seen_docs:
            seen_docs.add(str(key))
            unique.append(r)

    log.info("Unique records: %d", len(unique))

    # 4. Enrich with parcel data
    enriched: list[dict] = []
    for r in unique:
        try:
            parcel = None
            # Try by APN extracted from legal description
            apn_m = re.search(r"(\d{3}-\d{2}-\d{3})", r.get("legal", ""))
            if apn_m:
                parcel = parcels.lookup_by_apn(apn_m.group(1))
            # Try by owner name
            if not parcel:
                parcel = parcels.lookup_by_owner(r.get("owner", ""))
            # Try Assessor API (if we have an APN)
            if not parcel and apn_m:
                parcel = parcels.fetch_parcel_api(apn_m.group(1))

            if parcel:
                r.update({
                    "prop_address": parcel.get("prop_address", ""),
                    "prop_city":    parcel.get("prop_city", ""),
                    "prop_state":   parcel.get("prop_state", "AZ"),
                    "prop_zip":     parcel.get("prop_zip", ""),
                    "mail_address": parcel.get("mail_address", ""),
                    "mail_city":    parcel.get("mail_city", ""),
                    "mail_state":   parcel.get("mail_state", "AZ"),
                    "mail_zip":     parcel.get("mail_zip", ""),
                })
            else:
                r.setdefault("prop_address", "")
                r.setdefault("prop_city", "")
                r.setdefault("prop_state", "AZ")
                r.setdefault("prop_zip", "")
                r.setdefault("mail_address", "")
                r.setdefault("mail_city", "")
                r.setdefault("mail_state", "AZ")
                r.setdefault("mail_zip", "")

            # Build clerk URL if missing
            if not r.get("clerk_url"):
                r["clerk_url"] = build_clerk_url(r.get("doc_num", ""))

            # Score
            score, flags = score_record(r)
            r["score"] = score
            r["flags"] = flags

            enriched.append(r)
        except Exception as exc:
            log.warning("Enrichment error for record %s: %s", r.get("doc_num"), exc)

    # Apply combo bonus
    enriched = apply_lp_fc_combo_bonus(enriched)

    # Sort by score descending
    enriched.sort(key=lambda x: x.get("score", 0), reverse=True)

    # 5. Build output payload
    with_address = sum(1 for r in enriched if r.get("prop_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Maricopa County Recorder's Office",
        "date_range":   {"start": str(start_dt), "end": str(end_dt)},
        "total":        len(enriched),
        "with_address": with_address,
        "records": [
            {
                "doc_num":      r.get("doc_num", ""),
                "doc_type":     r.get("doc_type", ""),
                "filed":        r.get("filed", ""),
                "cat":          r.get("cat", ""),
                "cat_label":    r.get("cat_label", ""),
                "owner":        r.get("owner", ""),
                "grantee":      r.get("grantee", ""),
                "amount":       r.get("amount"),
                "legal":        r.get("legal", ""),
                "prop_address": r.get("prop_address", ""),
                "prop_city":    r.get("prop_city", ""),
                "prop_state":   r.get("prop_state", "AZ"),
                "prop_zip":     r.get("prop_zip", ""),
                "mail_address": r.get("mail_address", ""),
                "mail_city":    r.get("mail_city", ""),
                "mail_state":   r.get("mail_state", "AZ"),
                "mail_zip":     r.get("mail_zip", ""),
                "clerk_url":    r.get("clerk_url", ""),
                "flags":        r.get("flags", []),
                "score":        r.get("score", 30),
            }
            for r in enriched
        ],
    }

    # 6. Save JSON
    for out_path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log.info("Saved → %s", out_path)

    # 7. GHL CSV export
    ghl_path = DATA_DIR / "ghl_export.csv"
    _write_ghl_csv(payload["records"], ghl_path)
    log.info("GHL CSV → %s", ghl_path)

    log.info("Done. %d leads (%d with address)", payload["total"], payload["with_address"])
    return payload


def _write_ghl_csv(records: list[dict], path: Path):
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            first, last = split_name(r.get("owner", ""))
            writer.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", "AZ"),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", "AZ"),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         r.get("doc_type", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", "") or "",
                "Seller Score":          r.get("score", 30),
                "Motivated Seller Flags":"; ".join(r.get("flags", [])),
                "Source":                "Maricopa County Recorder",
                "Public Records URL":    r.get("clerk_url", ""),
            })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
