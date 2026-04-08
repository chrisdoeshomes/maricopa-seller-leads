#!/usr/bin/env python3
"""
Maricopa County Motivated Seller Lead Scraper  – v2
====================================================
Sources
-------
  Recorder (legacy ASP.NET form):
    https://recorder.maricopa.gov/RecDocData/RecDocData.aspx
  Assessor public search API (no token needed):
    https://mcassessor.maricopa.gov/search/property/?q={name}
  Assessor parcel detail API:
    https://mcassessor.maricopa.gov/parcel/{apn}/owner-details
    https://mcassessor.maricopa.gov/parcel/{apn}/address
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import time
import unicodedata
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

RECORDER_SEARCH_URL = "https://recorder.maricopa.gov/RecDocData/RecDocData.aspx"
RECORDER_BASE       = "https://recorder.maricopa.gov"

ASSESSOR_SEARCH  = "https://mcassessor.maricopa.gov/search/property/"
ASSESSOR_PARCEL  = "https://mcassessor.maricopa.gov/parcel/{apn}"
ASSESSOR_OWNER   = "https://mcassessor.maricopa.gov/parcel/{apn}/owner-details"
ASSESSOR_ADDRESS = "https://mcassessor.maricopa.gov/parcel/{apn}/address"
ASSESSOR_DOWNLOADS = "https://mcassessor.maricopa.gov/page/data_sales/"

REPO_ROOT     = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR      = REPO_ROOT / "data"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Document type catalogue
# ---------------------------------------------------------------------------
DOC_TYPES: dict[str, dict] = {
    "LP":       {"label": "Lis Pendens",            "cat": "foreclosure", "weight": 10},
    "NOFC":     {"label": "Notice of Foreclosure",  "cat": "foreclosure", "weight": 10},
    "TAXDEED":  {"label": "Tax Deed",               "cat": "tax",         "weight": 10},
    "RELLP":    {"label": "Release Lis Pendens",    "cat": "release",     "weight":  5},
    "JUD":      {"label": "Judgment",               "cat": "judgment",    "weight": 10},
    "CCJ":      {"label": "Certified Judgment",     "cat": "judgment",    "weight": 10},
    "DRJUD":    {"label": "Domestic Judgment",      "cat": "judgment",    "weight": 10},
    "LNCORPTX": {"label": "Corp Tax Lien",          "cat": "tax_lien",    "weight": 10},
    "LNIRS":    {"label": "IRS Lien",               "cat": "tax_lien",    "weight": 10},
    "LNFED":    {"label": "Federal Lien",           "cat": "tax_lien",    "weight": 10},
    "LN":       {"label": "Lien",                   "cat": "lien",        "weight": 10},
    "LNMECH":   {"label": "Mechanic Lien",          "cat": "lien",        "weight": 10},
    "LNHOA":    {"label": "HOA Lien",               "cat": "lien",        "weight": 10},
    "MEDLN":    {"label": "Medicaid Lien",          "cat": "lien",        "weight": 10},
    "PRO":      {"label": "Probate",                "cat": "probate",     "weight": 10},
    "NOC":      {"label": "Notice of Commencement", "cat": "construction","weight":  5},
}

# Longest-key-first map so specific phrases win over substrings
DOC_CODE_MAP: dict[str, str] = {
    "RELEASE OF LIS PENDENS":  "RELLP",
    "RELEASE LIS PENDENS":     "RELLP",
    "NOTICE OF FORECLOSURE":   "NOFC",
    "CERTIFIED JUDGMENT":      "CCJ",
    "CORPORATE TAX LIEN":      "LNCORPTX",
    "DOMESTIC JUDGMENT":       "DRJUD",
    "CORP TAX LIEN":           "LNCORPTX",
    "NOTICE OF COMMENCEMENT":  "NOC",
    "HOMEOWNERS ASSOCIATION":  "LNHOA",
    "MECHANICS LIEN":          "LNMECH",
    "MECHANIC LIEN":           "LNMECH",
    "MEDICAID LIEN":           "MEDLN",
    "FEDERAL LIEN":            "LNFED",
    "LIS PENDENS":             "LP",
    "LIS PENDEN":              "LP",
    "FORECLOSURE":             "NOFC",
    "TAX DEED":                "TAXDEED",
    "HOA LIEN":                "LNHOA",
    "IRS LIEN":                "LNIRS",
    "JUDGMENT":                "JUD",
    "PROBATE":                 "PRO",
    "LIEN":                    "LN",
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
# Shared HTTP session with browser-like headers
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
})

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9 ]", " ", s.upper()).strip()


def parse_amount(raw: str) -> float | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9.]", "", str(raw))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def retry(fn, attempts: int = 4, base_delay: float = 3.0):
    for i in range(attempts):
        try:
            result = fn()
            if result is not None:
                return result
        except Exception as exc:
            log.warning("  Attempt %d/%d failed: %s", i + 1, attempts, exc)
        if i < attempts - 1:
            wait = base_delay * (2 ** i)   # exponential back-off: 3, 6, 12 s
            log.info("  Waiting %.0fs before retry…", wait)
            time.sleep(wait)
    return None


def map_doc_code(raw_type: str) -> str | None:
    upper = norm(raw_type)
    if not upper:
        return None
    # Longest key first — already sorted in DOC_CODE_MAP definition above
    for k, v in DOC_CODE_MAP.items():
        if norm(k) in upper:
            return v
    for k in sorted(DOC_TYPES.keys(), key=len, reverse=True):
        if upper == k or upper.startswith(k + " "):
            return k
    return None


def build_clerk_url(doc_num: str) -> str:
    if not doc_num:
        return ""
    clean = re.sub(r"[^0-9]", "", doc_num)
    return f"{RECORDER_BASE}/RecDocData/getimage.aspx?rec={clean}"


def split_name(full: str) -> tuple[str, str]:
    if not full:
        return "", ""
    clean = re.sub(
        r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE|ET AL|ET UX|JR|SR|II|III|IV)\b",
        "", full.upper()
    ).strip()
    if "," in clean:
        parts = clean.split(",", 1)
        return parts[1].strip().title(), parts[0].strip().title()
    parts = clean.split()
    if len(parts) == 1:
        return "", parts[0].title()
    return parts[0].title(), " ".join(parts[1:]).title()


# ---------------------------------------------------------------------------
# Recorder scraper — pure requests + BeautifulSoup
# ---------------------------------------------------------------------------

class RecorderScraper:
    """
    Submits the legacy Recorder ASP.NET form for each document type and
    collects results. No browser needed.
    """

    def __init__(self, start: date, end: date):
        self.start = start
        self.end   = end

    def run(self) -> list[dict]:
        records: list[dict] = []
        start_str = self.start.strftime("%m/%d/%Y")
        end_str   = self.end.strftime("%m/%d/%Y")

        # Warm up session — grab the page first to get cookies + viewstate
        viewstate, evval, evgen = self._get_viewstate()

        for code, info in DOC_TYPES.items():
            log.info("→ Querying Recorder for %s (%s)", code, info["label"])
            recs = retry(
                lambda c=code, i=info, s=start_str, e=end_str,
                       vs=viewstate, ev=evval, eg=evgen:
                    self._query(c, i, s, e, vs, ev, eg),
                attempts=4,
                base_delay=5.0,
            )
            if recs:
                records.extend(recs)
                log.info("  → %d records for %s", len(recs), code)
            else:
                log.warning("  → 0 records for %s (timeout or no results)", code)

            # Polite delay between requests
            time.sleep(2.5)

        return records

    def _get_viewstate(self) -> tuple[str, str, str]:
        """GET the search page and extract ASP.NET hidden fields."""
        try:
            r = SESSION.get(RECORDER_SEARCH_URL, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            vs  = soup.find("input", {"name": "__VIEWSTATE"})
            ev  = soup.find("input", {"name": "__EVENTVALIDATION"})
            eg  = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
            return (
                vs["value"]  if vs  else "",
                ev["value"]  if ev  else "",
                eg["value"]  if eg  else "",
            )
        except Exception as exc:
            log.warning("Could not fetch Recorder viewstate: %s", exc)
            return "", "", ""

    def _query(
        self,
        code: str,
        info: dict,
        start_str: str,
        end_str: str,
        viewstate: str,
        evval: str,
        evgen: str,
    ) -> list[dict]:
        """POST the search form for one document type and parse the results."""

        payload = {
            "__EVENTTARGET":        "",
            "__EVENTARGUMENT":      "",
            "__VIEWSTATE":          viewstate,
            "__VIEWSTATEGENERATOR": evgen,
            "__EVENTVALIDATION":    evval,
            # The actual form fields (names discovered from page source)
            "ctl00$ContentPlaceHolder1$txtBeginDate": start_str,
            "ctl00$ContentPlaceHolder1$txtEndDate":   end_str,
            "ctl00$ContentPlaceHolder1$txtDocCode":   code,
            "ctl00$ContentPlaceHolder1$btnSearch":    "Search",
        }

        r = SESSION.post(
            RECORDER_SEARCH_URL,
            data=payload,
            timeout=45,
            headers={
                "Referer": RECORDER_SEARCH_URL,
                "Origin":  RECORDER_BASE,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        r.raise_for_status()

        results = self._parse_html(r.text, code, info)

        # If we got nothing, also try the newer Elasticsearch-backed endpoint
        if not results:
            results = self._try_elastic(code, info, start_str, end_str)

        return results

    def _try_elastic(
        self, code: str, info: dict, start_str: str, end_str: str
    ) -> list[dict]:
        """
        The new recorder search page fires XHR requests to an internal
        Elasticsearch proxy. Try a few known patterns.
        """
        headers = {
            "Accept": "application/json",
            "Referer": f"{RECORDER_BASE}/recording/document-search.html",
            "X-Requested-With": "XMLHttpRequest",
        }
        candidate_urls = [
            f"{RECORDER_BASE}/recording/api/search",
            f"{RECORDER_BASE}/api/recording/search",
            f"{RECORDER_BASE}/RecDocData/api/search",
        ]
        for url in candidate_urls:
            try:
                r = SESSION.post(
                    url,
                    json={
                        "docType":   code,
                        "beginDate": start_str,
                        "endDate":   end_str,
                        "pageSize":  500,
                    },
                    headers=headers,
                    timeout=20,
                )
                if r.ok and "json" in r.headers.get("content-type", ""):
                    data = r.json()
                    items = (
                        data if isinstance(data, list)
                        else data.get("results", data.get("documents", data.get("hits", [])))
                    )
                    if isinstance(items, list) and items:
                        return [self._parse_json_item(i, code, info) for i in items]
            except Exception:
                pass
        return []

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_html(self, html: str, code: str, info: dict) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")

        # Try to find a results grid
        tbl = (
            soup.find("table", id=lambda x: x and "grid" in x.lower())
            or soup.find("table", id=lambda x: x and "result" in x.lower())
            or soup.find("table", id=lambda x: x and "gv" in x.lower())
        )
        if not tbl:
            all_tbls = soup.find_all("table")
            if not all_tbls:
                return []
            tbl = max(all_tbls, key=lambda t: len(t.find_all("tr")))

        rows = tbl.find_all("tr")
        if len(rows) < 2:
            return []

        # Header row
        headers = [
            th.get_text(strip=True).upper()
            for th in rows[0].find_all(["th", "td"])
        ]

        def col(cells, *names) -> str:
            for n in names:
                for i, h in enumerate(headers):
                    if n in h and i < len(cells):
                        return cells[i].get_text(strip=True)
            return ""

        results = []
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            try:
                link_tag = row.find("a", href=True)
                link = ""
                if link_tag:
                    href = link_tag["href"]
                    link = href if href.startswith("http") else RECORDER_BASE + "/" + href.lstrip("/")

                raw_type = col(cells, "DOC TYPE", "DOCTYPE", "TYPE", "DOCUMENT", "CODE")
                resolved = map_doc_code(raw_type) or code

                doc_num = col(cells, "DOC NUM", "DOCNUM", "NUMBER", "RECORDING", "INSTRUMENT")
                results.append({
                    "doc_num":   doc_num,
                    "doc_type":  raw_type or info["label"],
                    "cat":       DOC_TYPES[resolved]["cat"],
                    "cat_label": DOC_TYPES[resolved]["label"],
                    "filed":     col(cells, "DATE", "FILED", "RECORDED", "REC DATE"),
                    "owner":     col(cells, "GRANTOR", "OWNER", "FROM", "NAME"),
                    "grantee":   col(cells, "GRANTEE", "TO", "LENDER", "IN FAVOR"),
                    "legal":     col(cells, "LEGAL", "DESCRIPTION", "PARCEL", "PROPERTY"),
                    "amount":    parse_amount(col(cells, "AMOUNT", "DEBT", "VALUE", "CONSIDER")),
                    "clerk_url": link or build_clerk_url(doc_num),
                    "_code":     resolved,
                })
            except Exception as exc:
                log.debug("Row parse error: %s", exc)

        return results

    def _parse_json_item(self, item: dict, code: str, info: dict) -> dict:
        raw_type = (
            item.get("documentType") or item.get("docType")
            or item.get("document_type") or info["label"]
        )
        resolved = map_doc_code(raw_type) or code
        doc_num = str(item.get("documentNumber") or item.get("docNum") or item.get("id", ""))
        return {
            "doc_num":   doc_num,
            "doc_type":  raw_type,
            "cat":       DOC_TYPES.get(resolved, info)["cat"],
            "cat_label": DOC_TYPES.get(resolved, info)["label"],
            "filed":     str(item.get("filedDate") or item.get("recordedDate") or ""),
            "owner":     str(item.get("grantor") or item.get("owner") or ""),
            "grantee":   str(item.get("grantee") or ""),
            "legal":     str(item.get("legalDescription") or item.get("legal") or ""),
            "amount":    parse_amount(str(item.get("amount") or "")),
            "clerk_url": str(item.get("url") or build_clerk_url(doc_num)),
            "_code":     resolved,
        }


# ---------------------------------------------------------------------------
# Assessor enrichment — public search API (no token required)
# ---------------------------------------------------------------------------

class AssessorEnricher:
    """
    Looks up owner name and address using the Assessor's free public API.
      /search/property/?q={name}   — fuzzy name search, returns APN hits
      /parcel/{apn}/address        — site address
      /parcel/{apn}/owner-details  — owner / mailing address
    """

    def __init__(self):
        self._cache: dict[str, dict] = {}   # apn → parcel dict

    def enrich(self, rec: dict) -> dict:
        """Return rec with prop_* and mail_* fields populated if possible."""
        # 1. Try APN from legal description
        apn = self._extract_apn(rec.get("legal", ""))

        # 2. Try owner name search to get APN
        if not apn and rec.get("owner"):
            apn = self._search_owner(rec["owner"])

        if apn:
            parcel = self._fetch_parcel(apn)
            if parcel:
                rec.update(parcel)

        # Ensure keys exist even if lookup failed
        for k in ("prop_address","prop_city","prop_state","prop_zip",
                  "mail_address","mail_city","mail_state","mail_zip"):
            rec.setdefault(k, "")
        rec.setdefault("prop_state", "AZ")
        rec.setdefault("mail_state", "AZ")
        return rec

    # ------------------------------------------------------------------

    def _extract_apn(self, legal: str) -> str:
        """Pull APN patterns like 301-23-456 from a legal description."""
        m = re.search(r"(\d{3}-\d{2}-\d{3})", legal or "")
        return m.group(1) if m else ""

    def _search_owner(self, owner: str) -> str:
        """Search the Assessor API by owner name and return the first APN."""
        if not owner or len(owner) < 4:
            return ""
        cache_key = norm(owner)
        if cache_key in self._cache:
            return self._cache[cache_key].get("_apn", "")

        def _get():
            r = SESSION.get(
                ASSESSOR_SEARCH,
                params={"q": owner},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            # Response shape: {"RealProperty": {"results": [...], "total": N}}
            rp = data.get("RealProperty", {})
            results = rp.get("results", [])
            if results:
                apn = results[0].get("Parcel", {}).get("Apn", "")
                if apn:
                    self._cache[cache_key] = {"_apn": apn}
                return apn
            return ""

        try:
            return retry(_get, attempts=3, base_delay=2.0) or ""
        except Exception:
            return ""

    def _fetch_parcel(self, apn: str) -> dict | None:
        """Fetch address + owner details for an APN."""
        clean_apn = apn.replace("-", "").replace(" ", "")
        if clean_apn in self._cache and "prop_address" in self._cache[clean_apn]:
            return self._cache[clean_apn]

        result: dict = {}

        # Address endpoint
        def _get_addr():
            r = SESSION.get(
                ASSESSOR_ADDRESS.format(apn=apn),
                timeout=15,
            )
            r.raise_for_status()
            return r.json()

        # Owner endpoint
        def _get_owner():
            r = SESSION.get(
                ASSESSOR_OWNER.format(apn=apn),
                timeout=15,
            )
            r.raise_for_status()
            return r.json()

        try:
            addr_data = retry(_get_addr, attempts=3, base_delay=2.0)
            if addr_data:
                # Shape: {"situs": {"streetAddress": ..., "city": ..., "zip": ...}}
                situs = addr_data.get("situs", addr_data)
                result["prop_address"] = (
                    situs.get("streetAddress") or situs.get("address", "")
                )
                result["prop_city"]    = situs.get("city", "")
                result["prop_state"]   = situs.get("state", "AZ")
                result["prop_zip"]     = str(situs.get("zip", situs.get("zipCode", "")))
        except Exception:
            pass

        try:
            own_data = retry(_get_owner, attempts=3, base_delay=2.0)
            if own_data:
                # Shape: {"owner": {"name": ..., "mailingAddress": {...}}}
                owner_obj = own_data.get("owner", own_data)
                mail = owner_obj.get("mailingAddress", {})
                result["mail_address"] = (
                    mail.get("streetAddress") or mail.get("address", "")
                )
                result["mail_city"]    = mail.get("city", "")
                result["mail_state"]   = mail.get("state", "AZ")
                result["mail_zip"]     = str(mail.get("zip", mail.get("zipCode", "")))
                # Also backfill owner name if record is missing it
                if not result.get("owner"):
                    result["_owner_from_assessor"] = owner_obj.get("name", "")
        except Exception:
            pass

        if result:
            self._cache[clean_apn] = result
        return result or None


# ---------------------------------------------------------------------------
# Scoring & flagging
# ---------------------------------------------------------------------------

def score_record(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score = 30

    code   = rec.get("_code", "")
    cat    = rec.get("cat", "")
    amount = rec.get("amount")
    filed  = rec.get("filed", "")
    owner  = rec.get("owner", "")
    prop   = rec.get("prop_address", "")

    if code in ("LP", "RELLP") or cat == "foreclosure":
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

    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    try:
        fmt = "%Y-%m-%d" if "-" in filed[:10] else "%m/%d/%Y"
        filed_dt = datetime.strptime(filed[:10], fmt)
        if (date.today() - filed_dt.date()).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    if prop:
        score += 5

    if owner and re.search(r"\b(LLC|INC|CORP|LTD|LP|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")

    # deduplicate
    seen, clean = set(), []
    for f in flags:
        if f not in seen:
            seen.add(f)
            clean.append(f)

    return min(score, 100), clean


def apply_lp_fc_combo_bonus(records: list[dict]) -> list[dict]:
    lp_owners = {norm(r.get("owner", "")) for r in records if r.get("_code") == "LP"}
    fc_owners = {norm(r.get("owner", "")) for r in records if r.get("_code") in ("NOFC", "TAXDEED")}
    combo = lp_owners & fc_owners
    for r in records:
        if norm(r.get("owner", "")) in combo and "Pre-foreclosure" not in r.get("flags", []):
            r.setdefault("flags", []).insert(0, "Pre-foreclosure")
            r["score"] = min(r.get("score", 30) + 20, 100)
    return records


# ---------------------------------------------------------------------------
# GHL CSV writer
# ---------------------------------------------------------------------------

def write_ghl_csv(records: list[dict], path: Path):
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
                "Amount/Debt Owed":      r.get("amount") or "",
                "Seller Score":          r.get("score", 30),
                "Motivated Seller Flags":"; ".join(r.get("flags", [])),
                "Source":                "Maricopa County Recorder",
                "Public Records URL":    r.get("clerk_url", ""),
            })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("Maricopa County Motivated Seller Scraper  v2")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    log.info("Date range: %s → %s", start_dt, end_dt)

    # 1 — Scrape Recorder
    scraper = RecorderScraper(start_dt, end_dt)
    raw = scraper.run()
    log.info("Raw records: %d", len(raw))

    # 2 — Deduplicate
    seen, unique = set(), []
    for r in raw:
        key = r.get("doc_num", "") or str(id(r))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique records: %d", len(unique))

    # 3 — Enrich with Assessor data
    enricher = AssessorEnricher()
    enriched = []
    for r in unique:
        try:
            r = enricher.enrich(r)
        except Exception as exc:
            log.warning("Enrichment error on %s: %s", r.get("doc_num"), exc)
        # Build clerk URL if still missing
        if not r.get("clerk_url"):
            r["clerk_url"] = build_clerk_url(r.get("doc_num", ""))
        # Score
        score, flags = score_record(r)
        r["score"] = score
        r["flags"] = flags
        enriched.append(r)

    # Combo bonus
    enriched = apply_lp_fc_combo_bonus(enriched)
    enriched.sort(key=lambda x: x.get("score", 0), reverse=True)

    # 4 — Build output
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

    # 5 — Save
    for out in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log.info("Saved → %s", out)

    ghl_path = DATA_DIR / "ghl_export.csv"
    write_ghl_csv(payload["records"], ghl_path)
    log.info("GHL CSV → %s", ghl_path)

    log.info("Done. %d leads (%d with address)", payload["total"], with_address)


if __name__ == "__main__":
    main()
