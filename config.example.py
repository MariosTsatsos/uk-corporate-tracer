# =============================================================================
# config.example.py — copy this file to config.py and fill in your own values
# =============================================================================
# IMPORTANT:
#   1. Copy: cp config.example.py config.py
#   2. Fill in your CH API key and file paths below
#   3. config.py is in .gitignore — never commit it
#
# Companies House API key:
#   Register for a free key at https://developer.company-information.service.gov.uk/
# =============================================================================

CH_API_KEY  = "YOUR-CH-API-KEY-HERE"
CH_BASE_URL = "https://api.company-information.service.gov.uk"

DB_PATH = "asset_discovery.db"

# Rate limiting — CH allows 600 requests per 5 minutes (free tier)
REQUEST_DELAY = 0.5  # seconds between requests

# -----------------------------------------------------------------------
# Land Registry dataset file paths
# Download from: https://use-land-property-data.service.gov.uk/datasets
# -----------------------------------------------------------------------
path        = "/path/to/your/land-registry-data/"
CCOD_PATH   = path + "CCOD_FULL_YYYY_MM.csv"    # UK corporate ownership
OCOD_PATH   = path + "OCOD_FULL_YYYY_MM.csv"    # Overseas corporate ownership
LEASES_PATH = path + "LEASES_FULL_YYYY_MM.csv"  # Leases

# -----------------------------------------------------------------------
# Companies House bulk data
# Download from: https://download.companieshouse.gov.uk/en_output.html
# -----------------------------------------------------------------------
CH_BULK_DB_PATH  = "ch_bulk.db"
CH_COMPANIES_CSV = path + "Companies House/BasicCompanyDataAsOneFile-YYYY-MM-DD.csv"
CH_PSC_SNAPSHOT  = path + "Companies House/persons-with-significant-control-snapshot-YYYY-MM-DD.txt"

# -----------------------------------------------------------------------
# Directors / individuals to investigate
# Add as many entries as needed.
#
# name    — canonical name in "SURNAME, Forename(s)" format
# aliases — alternative name formats CH may return
# dob_year, dob_month — used to confirm identity; month is optional
# -----------------------------------------------------------------------
DIRECTORS = [
    {
        "name":      "SMITH, John William",
        "aliases":   ["John William Smith", "John Smith"],
        "dob_year":  1970,
        "dob_month": 4,
    },
    {
        "name":      "JONES, Sarah",
        "aliases":   ["Sarah Jones"],
        "dob_year":  1975,
        "dob_month": 11,
    },
]

# -----------------------------------------------------------------------
# Optional: nominee PSC detection
# Set to a name fragment to flag companies where a nominee service holds PSC.
# Leave as None to disable.
# Example: NOMINEE_PSC_FRAGMENT = "NOMINEE SERVICES"
# -----------------------------------------------------------------------
NOMINEE_PSC_FRAGMENT = None
