"""
Land Registry Dataset Search — Steps 5 & 6

CCOD/OCOD/LEASES column reference:
  Title Number, Tenure, Property Address, District, County, Region, Postcode,
  Multiple Address Indicator, Price Paid, Proprietor Name (1..4),
  Company Registration No. (1..4), Proprietorship Category (1..4),
  Proprietor (1..4) Address (1), Date Proprietor Added
"""

import csv
import os
import re

from config import CCOD_PATH, OCOD_PATH, LEASES_PATH
from database import get_conn

TITLE_PREFIX = re.compile(r'^(MR|MRS|MISS|MS|DR|PROF|SIR|LORD|LADY)\s+', re.IGNORECASE)


def _norm(s):
    if not s:
        return ""
    return re.sub(r'\s+', ' ', s.strip().upper())


def _build_search_terms(directors):
    conn = get_conn()

    company_numbers = set()
    company_names   = set()
    person_names    = set()

    for row in conn.execute("SELECT company_number, company_name FROM companies").fetchall():
        if row["company_number"]:
            company_numbers.add(row["company_number"].upper().strip())
        if row["company_name"]:
            company_names.add(_norm(row["company_name"]))

    for row in conn.execute("SELECT DISTINCT company_number, company_name FROM director_companies").fetchall():
        if row["company_number"]:
            company_numbers.add(row["company_number"].upper().strip())
        if row["company_name"]:
            company_names.add(_norm(row["company_name"]))

    for d in directors:
        person_names.add(_norm(d["name"]))
        for alias in d.get("aliases", []):
            person_names.add(_norm(alias))

    for row in conn.execute(
        "SELECT DISTINCT psc_name FROM psc WHERE psc_kind LIKE '%individual%'"
    ).fetchall():
        if row["psc_name"]:
            person_names.add(_norm(row["psc_name"]))

    conn.close()

    personal_variants = _build_personal_name_variants(person_names)

    print(f"[LR] Search terms: {len(company_numbers)} company numbers | "
          f"{len(company_names)} company names | "
          f"{len(personal_variants)} personal name variants (from {len(person_names)} names)")

    return {
        "company_numbers":   company_numbers,
        "company_names":     company_names,
        "personal_variants": personal_variants,
    }


def _build_personal_name_variants(person_names):
    """
    For each person name, generate every plausible format LR might store it in.
    Requires EXACT match against the normalised proprietor field — no substrings,
    no single-token matching. This eliminates false positives entirely.

    LR typically stores individuals as: SURNAME FORENAME(S)
    e.g. 'SMITH JOHN WILLIAM' or 'JOHN WILLIAM SMITH'
    """
    variants = set()

    for pname in person_names:
        pname = _norm(pname)
        if not pname:
            continue

        if "," in pname:
            surname, forenames = pname.split(",", 1)
            surname   = surname.strip()
            forenames = forenames.strip()
        else:
            parts     = pname.split()
            if len(parts) < 2:
                continue   # single token — not enough to match safely
            surname   = parts[-1]
            forenames = " ".join(parts[:-1])

        if not surname or not forenames:
            continue

        forename_parts = forenames.split()

        # Full name, surname first (most common in LR)
        variants.add(f"{surname} {forenames}")
        # Full name, forenames first
        variants.add(f"{forenames} {surname}")
        # Without middle name, surname first
        if len(forename_parts) > 1:
            variants.add(f"{surname} {forename_parts[0]}")
            variants.add(f"{forename_parts[0]} {surname}")

    return variants


def _search_dataset(filepath, dataset_name, terms, include_personal=False):
    if not os.path.exists(filepath):
        print(f"[LR] MISSING: {filepath} — skipping {dataset_name}")
        return []

    print(f"\n[LR] Scanning {dataset_name} ({filepath}) ...")

    cn_set       = terms["company_numbers"]
    name_set     = terms["company_names"]
    pers_set     = terms["personal_variants"] if include_personal else set()

    matches   = []
    row_count = 0

    with open(filepath, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)

        for row in reader:
            row_count += 1
            if row_count % 100_000 == 0:
                print(f"[LR]   {row_count:,} rows, {len(matches)} matches so far")

            matched = False

            # 1. Exact company number match
            for cn_col in ("Company Registration No. (1)", "Company Registration No. (2)",
                           "Company Registration No. (3)", "Company Registration No. (4)",
                           "Company Registration No.", "Proprietor Registration No."):
                val = (row.get(cn_col) or "").upper().strip()
                if val and val in cn_set:
                    matched = True
                    break

            if matched:
                matches.append(dict(row))

            # NOTE: Name-based matching removed — company number matching is sufficient
            # and exact. Name matching caused thousands of false positives from unrelated
            # proprietors who share name tokens with PSCs/directors in the database.

    print(f"[LR] {dataset_name}: {row_count:,} rows → {len(matches)} matches")
    return matches


def _save_matches(matches, dataset_source, terms):
    cn_set = terms["company_numbers"]
    conn   = get_conn()
    corp_saved = 0
    pers_saved = 0

    for row in matches:
        title      = row.get("Title Number", "")
        tenure     = row.get("Tenure", "")
        address    = row.get("Property Address", "")
        district   = row.get("District", "")
        county     = row.get("County", "")
        region     = row.get("Region", "")
        postcode   = row.get("Postcode", "")
        multi      = row.get("Multiple Address Indicator", "")
        price      = row.get("Price Paid", "")
        date_added = row.get("Date Proprietor Added", "")

        # Find the matching proprietor slot
        proprietor    = ""
        company_no    = ""
        prop_category = ""
        prop_address  = ""

        for i in ("(1)", "(2)", "(3)", "(4)"):
            cn = (row.get(f"Company Registration No. {i}") or "").upper().strip()
            if cn and cn in cn_set:
                proprietor    = row.get(f"Proprietor Name {i}", "")
                company_no    = cn
                prop_category = row.get(f"Proprietorship Category {i}", "")
                prop_address  = (row.get(f"Proprietor {i} Address (1)", "") or
                                 row.get(f"Proprietor {i[1]} Address (1)", ""))
                break

        is_corporate = bool(company_no)

        # Fallback to slot 1 for personal matches
        if not is_corporate:
            proprietor    = row.get("Proprietor Name (1)", "")
            company_no    = ""
            prop_category = row.get("Proprietorship Category (1)", "")
            prop_address  = row.get("Proprietor (1) Address (1)", "")

        if is_corporate:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO properties
                    (title_number, tenure, property_address, district, county, region,
                     postcode, multiple_address_indicator, price_paid, date_proprietor_added,
                     owner_company, owner_company_number, proprietorship_category,
                     proprietor_address, dataset_source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (title, tenure, address, district, county, region, postcode,
                      multi, price, date_added, proprietor, company_no,
                      prop_category, prop_address, dataset_source))
                corp_saved += 1
            except Exception as e:
                print(f"[DB] Property insert: {e}")
        else:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO personal_properties
                    (title_number, tenure, property_address, district, county, region,
                     postcode, price_paid, date_proprietor_added, owner_name,
                     proprietor_address, dataset_source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (title, tenure, address, district, county, region, postcode,
                      price, date_added, proprietor, prop_address, dataset_source))
                pers_saved += 1
            except Exception as e:
                print(f"[DB] Personal property insert: {e}")

    conn.commit()
    conn.close()
    print(f"[LR] Saved: {corp_saved} corporate, {pers_saved} personal from {dataset_source}")


def step5_search_land_registry(directors):
    print("\n" + "="*60)
    print("STEPS 5-6: Land Registry dataset search")
    print("="*60)

    terms = _build_search_terms(directors)

    for path, name in [
        (CCOD_PATH,  "CCOD"),   # UK corporate ownership — has company numbers
        (OCOD_PATH,  "OCOD"),   # Overseas corporate ownership — has company numbers
        # LEASES excluded: lease terms only, no company registration numbers
    ]:
        matches = _search_dataset(path, name, terms, include_personal=True)
        if matches:
            _save_matches(matches, name, terms)
