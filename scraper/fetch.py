#!/usr/bin/env python3
"""
Maricopa County Motivated Seller Lead Scraper  – v3
====================================================
Sources (all confirmed accessible from cloud servers):
  1. Maricopa Superior Court Civil Docket  – foreclosures, lis pendens, judgments, liens
     https://www.superiorcourt.maricopa.gov/docket/CivilCourtCases/caseSearch.asp
  2. Maricopa Superior Court Probate Docket – probate / estate cases
     https://www.superiorcourt.maricopa.gov/docket/ProbateCourtCases/caseSearch.asp
  3. Maricopa County Assessor public API   – owner name + address enrichment
     https://mcassessor.maricopa.gov/search/property/?q={name}
     https://mcassessor.maricopa.gov/parcel/{apn}/owner-details
     https://mcassessor.maricopa.gov/parcel/{apn}/address

Strategy
--------
The Superior Court docket search accepts GET requests by last name or
business name and returns an HTML table of matching cases with case number,
party names, case type, and filing date.

We search for high-signal keyword terms that appear in defendant/plaintiff
names of distressed-property cases:
  - "TRUSTEE" (foreclosure trustees)
  - "BANK", "MORTGAGE", "FINANCIAL", "LENDING" (lender-initiated FC)
  - "HOA", "ASSOCIATION" (HOA lien cases)
  - "IRS", "INTERNAL REVENUE" (tax liens)
  - "ESTATE OF", "DECEASED" (probate)
  - Common foreclosure firm names in AZ

For each case found within the lookback window we enrich with Assessor data.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import time
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

CIVIL_SEARCH   = "https://www.superiorcourt.maricopa.gov/docket/CivilCourtCases/caseSearch.asp"
PROBATE_SEARCH = "https://www.superiorcourt.maricopa.gov/docket/ProbateCourtCases/caseSearch.asp"
CIVIL_CASE     = "https://www.superiorcourt.maricopa.gov/docket/CivilCourtCases/caseInfo.asp?caseNumber={}"
PROBATE_CASE   = "https://www.superiorcourt.maricopa.gov/docket/ProbateCourtCases/caseInfo.asp?caseNumber={}"

ASSESSOR_SEARCH  = "https://mcassessor.maricopa.gov/search/property/"
ASSESSOR_OWNER   = "https://mcassessor.maricopa.gov/parcel/{apn}/owner-details"
ASSESSOR_ADDRESS = "https://mcassessor.maricopa.gov/parcel/{apn}/address"

REPO_ROOT     = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR      = REPO_ROOT / "data"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Search terms → lead category mapping
# We search by BUSINESS NAME for entities that appear in distressed filings
# ---------------------------------------------------------------------------
BUSINESS_SEARCHES = [
    # Foreclosure / Trustee sales
    ("TRUSTEE CORPS",       "foreclosure", "LP"),
    ("QUALITY LOAN",        "foreclosure", "LP"),
    ("AZTEC FORECLOSURE",   "foreclosure", "LP"),
    ("WESTERN PROGRESSIVE", "foreclosure", "LP"),
    ("CLEAR RECON",         "foreclosure", "LP"),
    ("BARRETT DAFFIN",      "foreclosure", "LP"),
    ("TIFFANY AND BOSCO",   "foreclosure", "LP"),
    # Banks / lenders filing FC
    ("PENNYMAC",            "foreclosure", "NOFC"),
    ("LAKEVIEW LOAN",       "foreclosure", "NOFC"),
    ("NEWREZ",              "foreclosure", "NOFC"),
    ("SELENE FINANCE",      "foreclosure", "NOFC"),
    ("CARRINGTON MORTGAGE", "foreclosure", "NOFC"),
    # HOA liens
    ("HOMEOWNERS ASSOCIATION", "lien",     "LNHOA"),
    ("HOA",                    "lien",     "LNHOA"),
    ("COMMUNITY ASSOCIATION",  "lien",     "LNHOA"),
    # Tax liens
    ("INTERNAL REVENUE",    "tax_lien",   "LNIRS"),
    ("MARICOPA COUNTY TREASURER", "tax_lien", "LNCORPTX"),
    # Judgment / debt
    ("MIDLAND CREDIT",      "judgment",   "JUD"),
    ("PORTFOLIO RECOVERY",  "judgment",   "JUD"),
    ("LVNV FUNDING",        "judgment",   "JUD"),
    ("UNIFIN",              "judgment",   "JUD"),
    # Mechanic liens
    ("MECHANICAL",          "lien",       "LNMECH"),
    ("CONSTRUCTION",        "lien",       "LNMECH"),
    ("ROOFING",             "lien",       "LNMECH"),
    ("PLUMBING",            "lien",       "LNMECH"),
]

# Name searches for probate
PROBATE_SEARCHES = [
    ("ESTATE OF",  "probate", "PRO"),
    ("IN THE MATTER OF", "probate", "PRO"),
    ("DECEASED",   "probate", "PRO"),
    ("GUARDIAN",   "probate", "PRO"),
]

# Map case-type codes from court to our categories
CASE_TYPE_MAP = {
    "CV":  ("foreclosure", "LP",    "Civil / Foreclosure"),
    "FC":  ("foreclosure", "NOFC",  "Foreclosure"),
    "SC":  ("judgment",    "JUD",   "Small Claims"),
    "CV2": ("lien",        "LN",    "Civil Lien"),
    "PB":  ("probate",     "PRO",   "Probate"),
    "ES":  ("probate",     "PRO",   "Estate"),
    "GC":  ("probate",     "PRO",   "Guardianship / Conservatorship"),
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
# HTTP session
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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
    cleaned = re.sub(r"[^0-9.]", "", str(raw or ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def retry(fn, attempts: int = 3, base_delay: float = 3.0):
    for i in range(attempts):
        try:
            result = fn()
            if result is not None:
                return result
        except Exception as exc:
            log.warning("  Attempt %d/%d failed: %s", i + 1, attempts, exc)
        if i < attempts - 1:
            time.sleep(base_delay * (2 ** i))
    return None


def parse_court_date(raw: str) -> date | None:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except Exception:
            pass
    return None


def within_lookback(filed_str: str, lookback: int) -> bool:
    d = parse_court_date(filed_str)
    if not d:
        return True   # include if we can't parse — better safe
    return (date.today() - d).days <= lookback


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


def build_court_url(case_num: str, court: str = "civil") -> str:
    if not case_num:
        return ""
    if court == "probate":
        return PROBATE_CASE.format(case_num)
    return CIVIL_CASE.format(case_num)


# ---------------------------------------------------------------------------
# Superior Court scraper
# ---------------------------------------------------------------------------

class CourtScraper:

    def __init__(self, start: date, end: date):
        self.start    = start
        self.end      = end
        self.lookback = (end - start).days + 1

    def run(self) -> list[dict]:
        records: list[dict] = []
        seen: set[str] = set()

        # Civil docket — business name searches
        for term, cat, code in BUSINESS_SEARCHES:
            log.info("→ Civil search: %s", term)
            recs = retry(
                lambda t=term, c=cat, k=code: self._search_civil_business(t, c, k),
                attempts=3, base_delay=4.0
            ) or []
            for r in recs:
                key = r.get("case_num", "") or str(id(r))
                if key not in seen:
                    seen.add(key)
                    records.append(r)
            time.sleep(2.0)

        # Probate docket — name searches
        for term, cat, code in PROBATE_SEARCHES:
            log.info("→ Probate search: %s", term)
            recs = retry(
                lambda t=term, c=cat, k=code: self._search_probate(t, c, k),
                attempts=3, base_delay=4.0
            ) or []
            for r in recs:
                key = r.get("case_num", "") or str(id(r))
                if key not in seen:
                    seen.add(key)
                    records.append(r)
            time.sleep(2.0)

        log.info("Court scrape complete: %d unique records", len(records))
        return records

    # ------------------------------------------------------------------
    # Civil business-name search
    # ------------------------------------------------------------------

    def _search_civil_business(self, term: str, cat: str, code: str) -> list[dict]:
        r = SESSION.get(
            CIVIL_SEARCH,
            params={"bName": term, "btnSearch": "Search"},
            timeout=30,
        )
        r.raise_for_status()
        return self._parse_results(r.text, cat, code, "civil")

    # ------------------------------------------------------------------
    # Probate last-name search
    # ------------------------------------------------------------------

    def _search_probate(self, term: str, cat: str, code: str) -> list[dict]:
        r = SESSION.get(
            PROBATE_SEARCH,
            params={"lastName": term, "btnSearch": "Search"},
            timeout=30,
        )
        r.raise_for_status()
        return self._parse_results(r.text, cat, code, "probate")

    # ------------------------------------------------------------------
    # HTML result parser
    # ------------------------------------------------------------------

    def _parse_results(self, html: str, cat: str, code: str, court: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        records = []

        # Find the results table
        tbl = None
        for t in soup.find_all("table"):
            rows = t.find_all("tr")
            if len(rows) > 1:
                headers = [th.get_text(strip=True).upper() for th in rows[0].find_all(["th", "td"])]
                if any(h in ("CASE NUMBER", "CASE NO", "NUMBER") for h in headers):
                    tbl = t
                    break

        if not tbl:
            # Try any table with enough rows
            tables = soup.find_all("table")
            if tables:
                tbl = max(tables, key=lambda t: len(t.find_all("tr")))

        if not tbl:
            return records

        rows = tbl.find_all("tr")
        if len(rows) < 2:
            return records

        headers = [th.get_text(strip=True).upper() for th in rows[0].find_all(["th", "td"])]

        def col(cells, *names) -> str:
            for n in names:
                for i, h in enumerate(headers):
                    if n in h and i < len(cells):
                        return cells[i].get_text(strip=True)
            return ""

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            try:
                filed = col(cells, "FILE DATE", "FILED", "DATE FILED", "FILING DATE", "DATE")
                if not within_lookback(filed, self.lookback):
                    continue

                case_num = col(cells, "CASE NUMBER", "CASE NO", "NUMBER")
                # Extract parties
                plaintiff = col(cells, "PLAINTIFF", "PETITIONER", "PARTY1", "PARTY NAME")
                defendant = col(cells, "DEFENDANT", "RESPONDENT", "PARTY2")
                case_type_raw = col(cells, "CASE TYPE", "TYPE")

                # Try to refine category from case type
                refined_cat, refined_code = cat, code
                for ct_key, (ct_cat, ct_code, _) in CASE_TYPE_MAP.items():
                    if ct_key in case_num.upper() or ct_key in case_type_raw.upper():
                        refined_cat, refined_code = ct_cat, ct_code
                        break

                # Link
                link_tag = row.find("a", href=True)
                link = ""
                if link_tag:
                    href = link_tag["href"]
                    base = "https://www.superiorcourt.maricopa.gov"
                    link = href if href.startswith("http") else base + href

                if not link and case_num:
                    link = build_court_url(case_num, court)

                records.append({
                    "doc_num":   case_num,
                    "doc_type":  case_type_raw or CASE_TYPE_MAP.get(refined_code, ["","","Unknown"])[2],
                    "cat":       refined_cat,
                    "cat_label": self._cat_label(refined_code),
                    "filed":     filed,
                    "owner":     defendant or plaintiff,
                    "grantee":   plaintiff,
                    "legal":     col(cells, "DESCRIPTION", "LEGAL", "ADDRESS"),
                    "amount":    parse_amount(col(cells, "AMOUNT", "DEBT", "VALUE")),
                    "clerk_url": link,
                    "_code":     refined_code,
                    "_court":    court,
                })
            except Exception as exc:
                log.debug("Row parse error: %s", exc)

        return records

    @staticmethod
    def _cat_label(code: str) -> str:
        labels = {
            "LP":       "Lis Pendens",
            "NOFC":     "Notice of Foreclosure",
            "JUD":      "Judgment",
            "CCJ":      "Certified Judgment",
            "DRJUD":    "Domestic Judgment",
            "LNCORPTX": "Corp Tax Lien",
            "LNIRS":    "IRS Lien",
            "LNFED":    "Federal Lien",
            "LN":       "Lien",
            "LNMECH":   "Mechanic Lien",
            "LNHOA":    "HOA Lien",
            "MEDLN":    "Medicaid Lien",
            "PRO":      "Probate",
            "NOC":      "Notice of Commencement",
            "TAXDEED":  "Tax Deed",
            "RELLP":    "Release Lis Pendens",
        }
        return labels.get(code, code)


# ---------------------------------------------------------------------------
# Assessor enrichment
# ---------------------------------------------------------------------------

class AssessorEnricher:

    def __init__(self):
        self._cache: dict[str, dict] = {}

    def enrich(self, rec: dict) -> dict:
        apn = self._extract_apn(rec.get("legal", ""))
        if not apn and rec.get("owner"):
            apn = self._search_owner(rec["owner"])
        if apn:
            parcel = self._fetch_parcel(apn)
            if parcel:
                rec.update(parcel)
        for k in ("prop_address","prop_city","prop_state","prop_zip",
                  "mail_address","mail_city","mail_state","mail_zip"):
            rec.setdefault(k, "")
        rec.setdefault("prop_state", "AZ")
        rec.setdefault("mail_state", "AZ")
        return rec

    def _extract_apn(self, legal: str) -> str:
        m = re.search(r"(\d{3}-\d{2}-\d{3})", legal or "")
        return m.group(1) if m else ""

    def _search_owner(self, owner: str) -> str:
        if not owner or len(owner.strip()) < 4:
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
            rp = data.get("RealProperty", {})
            results = rp.get("results", [])
            if results:
                apn = results[0].get("Parcel", {}).get("Apn", "")
                if apn:
                    self._cache[cache_key] = {"_apn": apn}
                return apn
            return ""

        return retry(_get, attempts=3, base_delay=2.0) or ""

    def _fetch_parcel(self, apn: str) -> dict | None:
        clean = apn.replace("-", "").replace(" ", "")
        if clean in self._cache and "prop_address" in self._cache[clean]:
            return self._cache[clean]

        result: dict = {}

        try:
            def _addr():
                r = SESSION.get(ASSESSOR_ADDRESS.format(apn=apn), timeout=15)
                r.raise_for_status()
                return r.json()

            addr = retry(_addr, attempts=3, base_delay=2.0)
            if addr:
                situs = addr.get("situs", addr)
                result["prop_address"] = situs.get("streetAddress") or situs.get("address", "")
                result["prop_city"]    = situs.get("city", "")
                result["prop_state"]   = situs.get("state", "AZ")
                result["prop_zip"]     = str(situs.get("zip") or situs.get("zipCode", ""))
        except Exception:
            pass

        try:
            def _owner():
                r = SESSION.get(ASSESSOR_OWNER.format(apn=apn), timeout=15)
                r.raise_for_status()
                return r.json()

            own = retry(_owner, attempts=3, base_delay=2.0)
            if own:
                owner_obj = own.get("owner", own)
                mail = owner_obj.get("mailingAddress", {})
                result["mail_address"] = mail.get("streetAddress") or mail.get("address", "")
                result["mail_city"]    = mail.get("city", "")
                result["mail_state"]   = mail.get("state", "AZ")
                result["mail_zip"]     = str(mail.get("zip") or mail.get("zipCode", ""))
        except Exception:
            pass

        if result:
            self._cache[clean] = result
        return result or None


# ---------------------------------------------------------------------------
# Scoring
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
        score += 10
    if code == "NOFC":
        flags.append("Pre-foreclosure")
        score += 10
    if cat == "judgment":
        flags.append("Judgment lien")
        score += 10
    if cat == "tax_lien":
        flags.append("Tax lien")
        score += 10
    if code == "LNMECH":
        flags.append("Mechanic lien")
        score += 10
    if code == "PRO":
        flags.append("Probate / estate")
        score += 10
    if amount:
        score += 15 if amount > 100_000 else 10 if amount > 50_000 else 0
    if prop:
        score += 5
    try:
        fmt = "%Y-%m-%d" if "-" in filed[:10] else "%m/%d/%Y"
        if (date.today() - datetime.strptime(filed[:10], fmt).date()).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|LP|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")

    seen, clean = set(), []
    for f in flags:
        if f not in seen:
            seen.add(f); clean.append(f)
    return min(score, 100), clean


def apply_combo_bonus(records: list[dict]) -> list[dict]:
    lp = {norm(r.get("owner","")) for r in records if r.get("_code") == "LP"}
    fc = {norm(r.get("owner","")) for r in records if r.get("_code") in ("NOFC","TAXDEED")}
    combo = lp & fc
    for r in records:
        if norm(r.get("owner","")) in combo and "Pre-foreclosure" not in r.get("flags",[]):
            r.setdefault("flags",[]).insert(0,"Pre-foreclosure")
            r["score"] = min(r.get("score",30)+20, 100)
    return records


# ---------------------------------------------------------------------------
# GHL CSV
# ---------------------------------------------------------------------------

def write_ghl_csv(records: list[dict], path: Path):
    cols = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            first, last = split_name(r.get("owner",""))
            w.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       r.get("mail_address",""),
                "Mailing City":          r.get("mail_city",""),
                "Mailing State":         r.get("mail_state","AZ"),
                "Mailing Zip":           r.get("mail_zip",""),
                "Property Address":      r.get("prop_address",""),
                "Property City":         r.get("prop_city",""),
                "Property State":        r.get("prop_state","AZ"),
                "Property Zip":          r.get("prop_zip",""),
                "Lead Type":             r.get("cat_label",""),
                "Document Type":         r.get("doc_type",""),
                "Date Filed":            r.get("filed",""),
                "Document Number":       r.get("doc_num",""),
                "Amount/Debt Owed":      r.get("amount") or "",
                "Seller Score":          r.get("score",30),
                "Motivated Seller Flags":"; ".join(r.get("flags",[])),
                "Source":                "Maricopa County Superior Court",
                "Public Records URL":    r.get("clerk_url",""),
            })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("="*60)
    log.info("Maricopa Motivated Seller Scraper  v3")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("="*60)

    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    log.info("Date range: %s → %s", start_dt, end_dt)

    # 1 — Scrape Superior Court
    scraper  = CourtScraper(start_dt, end_dt)
    raw      = scraper.run()
    log.info("Raw records: %d", len(raw))

    # 2 — Enrich
    enricher = AssessorEnricher()
    enriched = []
    for r in raw:
        try:
            r = enricher.enrich(r)
        except Exception as exc:
            log.warning("Enrichment error %s: %s", r.get("doc_num"), exc)
        score, flags = score_record(r)
        r["score"] = score
        r["flags"] = flags
        enriched.append(r)

    enriched = apply_combo_bonus(enriched)
    enriched.sort(key=lambda x: x.get("score",0), reverse=True)

    # 3 — Output
    with_address = sum(1 for r in enriched if r.get("prop_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat()+"Z",
        "source":       "Maricopa County Superior Court",
        "date_range":   {"start": str(start_dt), "end": str(end_dt)},
        "total":        len(enriched),
        "with_address": with_address,
        "records": [{
            "doc_num":      r.get("doc_num",""),
            "doc_type":     r.get("doc_type",""),
            "filed":        r.get("filed",""),
            "cat":          r.get("cat",""),
            "cat_label":    r.get("cat_label",""),
            "owner":        r.get("owner",""),
            "grantee":      r.get("grantee",""),
            "amount":       r.get("amount"),
            "legal":        r.get("legal",""),
            "prop_address": r.get("prop_address",""),
            "prop_city":    r.get("prop_city",""),
            "prop_state":   r.get("prop_state","AZ"),
            "prop_zip":     r.get("prop_zip",""),
            "mail_address": r.get("mail_address",""),
            "mail_city":    r.get("mail_city",""),
            "mail_state":   r.get("mail_state","AZ"),
            "mail_zip":     r.get("mail_zip",""),
            "clerk_url":    r.get("clerk_url",""),
            "flags":        r.get("flags",[]),
            "score":        r.get("score",30),
        } for r in enriched],
    }

    for out in [DASHBOARD_DIR/"records.json", DATA_DIR/"records.json"]:
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log.info("Saved → %s", out)

    write_ghl_csv(payload["records"], DATA_DIR/"ghl_export.csv")
    log.info("Done. %d leads (%d with address)", len(enriched), with_address)


if __name__ == "__main__":
    main()
