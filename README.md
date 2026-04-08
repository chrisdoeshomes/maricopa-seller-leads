# Maricopa County Motivated Seller Lead Scraper

Automated daily scraper that pulls distressed-property documents from the **Maricopa County Recorder's Office**, enriches each record with owner/address data from the **Assessor's bulk parcel download**, scores every lead, and publishes a live dashboard to GitHub Pages.

---

## 🗂 File Structure

```
.
├── scraper/
│   ├── fetch.py          # Main scraper (Playwright + requests/BS4)
│   └── requirements.txt
├── dashboard/
│   ├── index.html        # GitHub Pages dashboard
│   └── records.json      # Latest lead data (auto-updated)
├── data/
│   ├── records.json      # Duplicate of dashboard/records.json
│   └── ghl_export.csv    # Go High Level / CRM export
└── .github/workflows/
    └── scrape.yml        # Daily 7 AM UTC cron + manual dispatch
```

---

## 🚀 Quick Start

### 1. Clone & install

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
```

### 2. Run manually

```bash
python scraper/fetch.py
# or override lookback window:
LOOKBACK_DAYS=14 python scraper/fetch.py
```

### 3. Enable GitHub Actions

- Push to GitHub
- Go to **Settings → Pages** → Source: `GitHub Actions`
- The workflow runs automatically every day at 7 AM UTC
- Trigger manually: **Actions → Scrape Motivated Seller Leads → Run workflow**

---

## 📊 Lead Types Collected

| Code | Label | Category |
|------|-------|----------|
| LP | Lis Pendens | foreclosure |
| NOFC | Notice of Foreclosure | foreclosure |
| TAXDEED | Tax Deed | tax |
| RELLP | Release Lis Pendens | release |
| JUD | Judgment | judgment |
| CCJ | Certified Judgment | judgment |
| DRJUD | Domestic Judgment | judgment |
| LNCORPTX | Corp Tax Lien | tax_lien |
| LNIRS | IRS Lien | tax_lien |
| LNFED | Federal Lien | tax_lien |
| LN | Lien | lien |
| LNMECH | Mechanic Lien | lien |
| LNHOA | HOA Lien | lien |
| MEDLN | Medicaid Lien | lien |
| PRO | Probate | probate |
| NOC | Notice of Commencement | construction |

---

## 🏆 Seller Score (0–100)

| Condition | Points |
|-----------|--------|
| Base score | +30 |
| Each distress flag | +10 |
| LP + Foreclosure combo (same owner) | +20 |
| Amount > $100k | +15 |
| Amount > $50k | +10 |
| Filed within last 7 days | +5 |
| Property address found | +5 |

---

## 🔗 Data Sources

| Source | URL | Purpose |
|--------|-----|---------|
| Recorder's Office | https://recorder.maricopa.gov | Document search (Playwright) |
| Assessor Bulk Data | https://mcassessor.maricopa.gov/page/data_sales/ | Owner/address enrichment |
| Assessor Parcel API | https://mcassessor.maricopa.gov/mcs.php | Per-parcel fallback |

---

## 📤 GHL / CRM Export

`data/ghl_export.csv` is generated on every run with these columns:

`First Name, Last Name, Mailing Address, Mailing City, Mailing State, Mailing Zip, Property Address, Property City, Property State, Property Zip, Lead Type, Document Type, Date Filed, Document Number, Amount/Debt Owed, Seller Score, Motivated Seller Flags, Source, Public Records URL`

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOOKBACK_DAYS` | `7` | Days back to search for documents |

---

## 📝 Notes

- The Recorder's portal uses JavaScript-heavy rendering; Playwright handles this automatically.
- If the modern search UI changes, the scraper automatically falls back to the legacy ASP.NET form endpoint.
- All errors are caught per-record — one bad record never crashes the entire run.
- The Assessor bulk download URL is scraped dynamically from the downloads page; it will self-heal if Maricopa updates the file location.
