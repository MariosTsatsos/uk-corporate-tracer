"""
Final report builder.

Output: one row per director × property relationship.
If a property is linked to multiple configured directors via different companies,
it appears as multiple rows.

Columns:
  Title Number | Tenure | Property Address | District | County | Region |
  Postcode | Price Paid | Year of Transaction | Current Est. Price |
  Proprietor Company | Company Number | Proprietorship Category |
  Proprietor Address | [D1] Active | [D1] PSC | [D1] Director Until |
  [D2] Active | [D2] PSC | [D2] Director Until |
  Charge Holder | Charge Created | Charge Status | Charge Document
"""

import csv
import re
import os
from datetime import datetime, date

from config import DIRECTORS
from database import get_conn

REVALUATION_THRESHOLD_YEARS = 3


def _director_label(director):
    """
    Build initials in natural name order (forenames first, then surname).
    e.g. 'SMITH, John William'    → JWS  (John William Smith)
         'DOUBLE-BARREL, Alice'   → ADB  (Alice Double Barrel)
    Hyphenated surnames are split so each part contributes one initial.
    """
    name = director["name"]
    if "," in name:
        surname_part, forenames_part = name.split(",", 1)
        forename_tokens = forenames_part.strip().split()
        # Split hyphenated surname into separate tokens
        surname_tokens = re.split(r"[-\s]+", surname_part.strip())
    else:
        tokens = re.split(r"[-\s]+", name.strip())
        forename_tokens = tokens[:-1]
        surname_tokens  = tokens[-1:]
    parts    = forename_tokens + surname_tokens
    initials = "".join(p[0].upper() for p in parts if p)
    return initials


def _year_from_date(date_str):
    """Extract year from a date string in various formats."""
    if not date_str:
        return ""
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d %b %Y"):
        try:
            return str(datetime.strptime(date_str.strip(), fmt).year)
        except ValueError:
            continue
    # If it starts with 4 digits, grab those
    if date_str.strip()[:4].isdigit():
        return date_str.strip()[:4]
    return date_str


def _needs_revaluation(date_str):
    """Return True if transaction was more than REVALUATION_THRESHOLD_YEARS ago."""
    year_str = _year_from_date(date_str)
    if not year_str or not year_str.isdigit():
        return True   # unknown = flag it
    try:
        return (date.today().year - int(year_str)) > REVALUATION_THRESHOLD_YEARS
    except Exception:
        return True


def _get_director_status(company_number, director_name):
    """
    Returns (is_active, is_psc, director_until) for a given director/company pair.
    is_active = Y if they currently hold a role, N if resigned
    is_psc    = Y if they appear in PSC records for this company
    director_until = resigned_on date or 'Present'
    """
    conn = get_conn()

    # Check director_companies
    rows = conn.execute("""
        SELECT appointed_on, resigned_on
        FROM director_companies
        WHERE company_number = ? AND director_name = ?
        ORDER BY appointed_on DESC
    """, (company_number, director_name)).fetchall()

    is_active      = ""
    director_until = ""

    if rows:
        # Take most recent appointment
        row = rows[0]
        if row["resigned_on"]:
            is_active      = "N"
            director_until = row["resigned_on"]
        else:
            is_active      = "Y"
            director_until = "Present"

    # Check PSC
    psc_row = conn.execute("""
        SELECT psc_name, ceased_on FROM psc
        WHERE company_number = ? AND psc_name LIKE ?
    """, (company_number, f"%{director_name.split(',')[0].strip()}%")).fetchone()

    is_psc = "Y" if psc_row and not psc_row["ceased_on"] else ("N" if psc_row else ("?" if is_active else "N"))

    conn.close()
    return is_active, is_psc, director_until


def _get_charges(company_number):
    """
    Returns list of (holder, created, status, doc_link) for a company.
    Multiple charges are returned as separate entries.
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT charge_holder, charge_created, charge_status, charge_document_link
        FROM charges
        WHERE company_number = ?
        ORDER BY charge_created DESC
    """, (company_number,)).fetchall()
    conn.close()
    return [(r["charge_holder"], r["charge_created"],
             r["charge_status"], r["charge_document_link"]) for r in rows]


def build_master_report():
    """
    Build the master asset report.
    One row per director × property.
    Returns list of dicts.
    """
    conn = get_conn()

    directors     = DIRECTORS
    dir_labels    = [_director_label(d) for d in directors]
    dir_names     = [d["name"] for d in directors]

    # Get all properties
    properties = conn.execute("""
        SELECT p.*
        FROM properties p
        ORDER BY p.property_address
    """).fetchall()

    # Also personal properties
    personal = conn.execute("""
        SELECT pp.*
        FROM personal_properties pp
        ORDER BY pp.property_address
    """).fetchall()

    conn.close()

    rows_out = []

    def _base_row(p, is_personal=False):
        date_str  = p["date_proprietor_added"] if p["date_proprietor_added"] else ""
        year_tx   = _year_from_date(date_str)
        needs_rev = _needs_revaluation(date_str)
        cn        = p["owner_company_number"] if not is_personal else ""

        return {
            "Title Number":            p["title_number"],
            "Tenure":                  p["tenure"],
            "Property Address":        p["property_address"],
            "District":                p["district"] if not is_personal else "",
            "County":                  p["county"]   if not is_personal else "",
            "Region":                  p["region"]   if not is_personal else "",
            "Postcode":                p["postcode"],
            "Price Paid":              p["price_paid"] or "",
            "Year of Transaction":     year_tx,
            "Current Est. Price":      "NEEDS REVALUATION" if needs_rev and p["price_paid"] else "",
            "Proprietor Company":      p["owner_company"] if not is_personal else p["owner_name"],
            "Company Number":          cn,
            "Proprietorship Category": p["proprietorship_category"] if not is_personal else "Personal",
            "Proprietor Address":      p["proprietor_address"],
            "Dataset Source":          p["dataset_source"],
        }

    # --- Corporate properties ---
    for p in properties:
        company_no = p["owner_company_number"]

        # Find which directors are linked to this company
        linked_directors = []
        for i, (label, dname) in enumerate(zip(dir_labels, dir_names)):
            active, psc, until = _get_director_status(company_no, dname)
            if active or psc:  # include if director appointment OR PSC link
                linked_directors.append((i, label, dname, active, psc, until))

        # Skip properties with no director link — these are not relevant to the search
        if not linked_directors:
            continue

        # One row per property, all directors merged in
        row = _base_row(p)
        for lbl in dir_labels:
            row[f"{lbl} Active"]         = "N"
            row[f"{lbl} PSC"]            = "N"
            row[f"{lbl} Director Until"] = ""
        for (i, label, dname, active, psc, until) in linked_directors:
            row[f"{label} Active"]         = active
            row[f"{label} PSC"]            = psc
            row[f"{label} Director Until"] = until
        _add_charges(row, company_no)
        rows_out.append(row)

    # --- Personal properties ---
    for p in personal:
        row = _base_row(p, is_personal=True)
        for label in dir_labels:
            row[f"{label} Active"]         = ""
            row[f"{label} PSC"]            = ""
            row[f"{label} Director Until"] = ""
        # Try to match personal property owner name to a director
        owner = (p["owner_name"] or "").upper()
        for label, dname in zip(dir_labels, dir_names):
            surname = dname.split(",")[0].strip().upper()
            if surname and surname in owner:
                row[f"{label} Active"]         = "Y (Personal)"
                row[f"{label} Director Until"] = "Present"
        row["Charge Holder"]      = ""
        row["Charge Created"]     = ""
        row["Charge Status"]      = ""
        row["Charge Document"]    = ""
        rows_out.append(row)

    return rows_out, dir_labels


def _add_charges(row, company_no):
    charges = _get_charges(company_no)
    if charges:
        row["Charge Holder"]   = " | ".join(c[0] for c in charges if c[0])
        row["Charge Created"]  = " | ".join(c[1] for c in charges if c[1])
        row["Charge Status"]   = " | ".join(c[2] for c in charges if c[2])
        row["Charge Document"] = " | ".join(c[3] for c in charges if c[3])
    else:
        row["Charge Holder"]   = ""
        row["Charge Created"]  = ""
        row["Charge Status"]   = ""
        row["Charge Document"] = ""


def _company_status(company_number, conn):
    row = conn.execute(
        "SELECT status, incorporated_on FROM companies WHERE company_number=?",
        (company_number,)
    ).fetchone()
    if not row:
        return "Unknown"
    s = (row["status"] or "").lower()
    if s == "active":                  return "Active"
    if s == "dissolved":               return "Dissolved"
    if s == "liquidation":             return "In Liquidation"
    if s == "administration":          return "In Administration"
    if s == "voluntary-arrangement":   return "CVA"
    if s == "receivership":            return "Receivership"
    if s == "converted-closed":        return "Closed"
    if s == "insolvency-proceedings":  return "Insolvency Proceedings"
    if s == "":                        return "Unknown"
    return s.title()


def _total_active_pscs(company_number, conn):
    return conn.execute("""
        SELECT COUNT(DISTINCT psc_name) FROM psc
        WHERE company_number=? AND ceased_on IS NULL
    """, (company_number,)).fetchone()[0]


def _get_psc_ownership(company_number, director, conn):
    """
    Search for a director's active PSC ownership in a company.
    Tries multiple name fragments to handle hyphenated surnames,
    name variants, and inconsistent CH formatting.
    """
    name = director["name"]  # e.g. "SMITH, John William"
    if "," in name:
        surname, forenames = name.split(",", 1)
        surname   = surname.strip()
        forenames = forenames.strip()
    else:
        parts     = name.split()
        surname   = parts[-1]
        forenames = " ".join(parts[:-1])

    # Build search fragments — try surname variants and forename
    fragments = set()
    fragments.add(surname)                          # full surname
    fragments.add(surname.replace("-", " "))        # hyphen → space
    fragments.add(surname.replace("-", ""))         # hyphen removed
    fragments.add(surname.split("-")[-1])           # last part of hyphenated name
    fragments.add(surname.split("-")[0])            # first part of hyphenated name
    if forenames:
        fragments.add(forenames.split()[0])         # first forename

    for frag in fragments:
        if len(frag) < 3:
            continue
        rows = conn.execute("""
            SELECT natures_of_control FROM psc
            WHERE company_number=? AND ceased_on IS NULL AND psc_name LIKE ?
        """, (company_number, f"%{frag}%")).fetchall()
        for p in rows:
            ctrl = p["natures_of_control"] or ""
            if "75-to-100" in ctrl: return "75-100%"
            if "50-to-75"  in ctrl: return "50-75%"
            if "25-to-50"  in ctrl: return "25-50%"
    return ""


def _combined_ownership(company_number, directors, conn):
    """
    Calculate minimum combined target-director ownership by residual logic:
    their minimum = 100% minus the maximum possible held by others.

    For each non-target PSC, assume they hold the MAXIMUM of their band.
    Whatever is left is the minimum the target directors can hold together.
    """
    band_max = {"75-to-100": 100, "50-to-75": 75, "25-to-50": 50}
    band_min = {"75-to-100": 75,  "50-to-75": 50, "25-to-50": 25}

    # Identify which PSCs are target directors
    dir_surnames = set()
    for d in directors:
        name = d["name"]
        surname = name.split(",")[0].strip().upper()
        dir_surnames.add(surname)
        dir_surnames.add(surname.replace("-", " "))
        dir_surnames.add(surname.replace("-", ""))
        dir_surnames.add(surname.split("-")[-1])
        dir_surnames.add(surname.split("-")[0])

    def is_target(psc_name):
        n = (psc_name or "").upper()
        return any(s in n for s in dir_surnames if len(s) >= 4)

    all_pscs = conn.execute("""
        SELECT psc_name, natures_of_control FROM psc
        WHERE company_number=? AND ceased_on IS NULL
    """, (company_number,)).fetchall()

    # Sum maximum possible held by third parties
    third_party_max = 0
    jam_dmf_min     = 0

    for p in all_pscs:
        ctrl = p["natures_of_control"] or ""
        if is_target(p["psc_name"]):
            # Add target directors' minimum
            for key, val in band_min.items():
                if key in ctrl:
                    jam_dmf_min += val
                    break
        else:
            # Add third party maximum
            for key, val in band_max.items():
                if key in ctrl:
                    third_party_max += val
                    break

    # Residual: what's left after third parties take their max
    residual = max(0, 100 - third_party_max)
    # Take the higher of direct sum and residual
    result = max(jam_dmf_min, residual)
    return f"{result}%+" if result else ""


def _fmt_cn(cn):
    """Zero-pad company number to 8 digits."""
    if cn and cn.isdigit():
        return cn.zfill(8)
    return cn or ""


def export_master_csv():
    os.makedirs("output", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    rows, dir_labels = build_master_report()
    if not rows:
        print("[REPORT] No data to export.")
        return None

    conn = get_conn()

    output_rows = []

    for r in rows:
        tenure = r.get("Tenure", "")
        cn     = r.get("Company Number", "")

        # Outstanding charge count
        charge_statuses = r.get("Charge Status", "")
        n_outstanding = charge_statuses.lower().count("outstanding") if charge_statuses else 0

        # Price
        try:
            price = float(r.get("Price Paid", "").replace(",", "").replace("£", "") or 0)
        except Exception:
            price = 0

        # Per-director details
        dir_details = {}
        any_active = False
        for lbl, d in zip(dir_labels, DIRECTORS):
            active   = r.get(f"{lbl} Active", "N")
            psc_flag = r.get(f"{lbl} PSC", "N")
            ownership = _get_psc_ownership(cn, d, conn) if (active == "Y" or psc_flag == "Y") else ""
            roles = []
            if active   == "Y": roles.append("Director"); any_active = True
            if psc_flag == "Y": roles.append("PSC")
            dir_details[lbl] = {
                "active":    active,
                "psc":       psc_flag,
                "ownership": ownership,
                "role":      "+".join(roles) if roles else "—",
                "until":     r.get(f"{lbl} Director Until", ""),
            }

        # Company status
        co_status = _company_status(cn, conn)

        # Tier — leaseholds and no-active-director go to bottom tiers
        if tenure == "Leasehold":
            tier = "F - Leasehold"
        elif not any_active:
            tier = "F - No active director"
        elif co_status not in ("Active",):
            tier = "F - Company not active"
        elif n_outstanding == 0:   tier = "A - Unencumbered"
        elif n_outstanding == 1:   tier = "B - One charge"
        elif n_outstanding == 2:   tier = "C - Two charges"
        elif n_outstanding <= 3:   tier = "D - Three charges"
        else:                      tier = "E - Heavy debt"

        # Sort key
        ownership_vals = {"75-100%": 0, "50-75%": 1, "25-50%": 2, "": 3}
        active_ownerships = [
            ownership_vals.get(dir_details[lbl]["ownership"], 3)
            for lbl in dir_labels if dir_details[lbl]["active"] == "Y"
        ]
        best_ownership = min(active_ownerships) if active_ownerships else 3

        output_rows.append({
            "Priority":              "",
            "Tier":                  tier,
            # Company info
            "Company":               r.get("Proprietor Company", ""),
            "Company Number":        _fmt_cn(cn),
            "Company Status":        co_status,
            "Total Active PSCs":     _total_active_pscs(cn, conn),
            "Min. Combined Ownership": _combined_ownership(cn, DIRECTORS, conn),
            # Property info
            "Property Address":      r.get("Property Address", ""),
            "District":              r.get("District", ""),
            "County":                r.get("County", ""),
            "Postcode":              r.get("Postcode", ""),
            "Tenure":                tenure,
            "Title Number":          r.get("Title Number", ""),
            "Price Paid":            price,
            "Year of Transaction":   r.get("Year of Transaction", ""),
            # Per-director
            **{f"{lbl} Active":      dir_details[lbl]["active"]    for lbl in dir_labels},
            **{f"{lbl} PSC":         dir_details[lbl]["psc"]       for lbl in dir_labels},
            **{f"{lbl} Ownership":   dir_details[lbl]["ownership"] for lbl in dir_labels},
            **{f"{lbl} Role":        dir_details[lbl]["role"]      for lbl in dir_labels},
            **{f"{lbl} Until":       dir_details[lbl]["until"]     for lbl in dir_labels},
            # Charges
            "Outstanding Charges":   n_outstanding,
            "Charge Holders":        r.get("Charge Holder", ""),
            "Charge Status":         r.get("Charge Status", ""),
            "Charge Created":        r.get("Charge Created", ""),
            "Dataset Source":        r.get("Dataset Source", ""),
            "_sort":                 (tier, best_ownership, -price),
        })

    conn.close()

    # Sort and assign priority
    output_rows.sort(key=lambda x: x["_sort"])
    for i, r in enumerate(output_rows, 1):
        r["Priority"]   = i
        r["Price Paid"] = f"£{r['Price Paid']:,.0f}" if r["Price Paid"] else "Unknown"
        del r["_sort"]

    # Column order
    cols = (
        ["Priority", "Tier",
         "Company", "Company Number", "Company Status",
         "Total Active PSCs", "Min. Combined Ownership",
         "Property Address", "District", "County", "Postcode",
         "Tenure", "Title Number", "Price Paid", "Year of Transaction"] +
        [f"{lbl} Active"    for lbl in dir_labels] +
        [f"{lbl} PSC"       for lbl in dir_labels] +
        [f"{lbl} Ownership" for lbl in dir_labels] +
        [f"{lbl} Role"      for lbl in dir_labels] +
        [f"{lbl} Until"     for lbl in dir_labels] +
        ["Outstanding Charges", "Charge Holders",
         "Charge Status", "Charge Created", "Dataset Source"]
    )

    filepath = f"output/asset_register_{ts}.csv"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    total_price = sum(
        float(r["Price Paid"].replace("£", "").replace(",", ""))
        for r in output_rows
        if r["Price Paid"] not in ("", "Unknown")
    )

    print(f"\n[REPORT] {len(output_rows)} properties written to {filepath}")
    print(f"[REPORT] Total price paid (known): £{total_price:,.0f}")
    return filepath


def export_hidden_companies_csv():
    """
    Step 5: find companies discovered via BFS corporate chain where NONE of
    the configured directors appear by name in directors or PSC records.

    These are assets held at arm's length — controlled indirectly through
    corporate PSC chains but with no direct name link to the targets.
    """
    conn = get_conn()

    # Build surname tokens for all directors
    dir_tokens = set()
    for d in DIRECTORS:
        name = d["name"]
        surname = name.split(",")[0].strip().upper()
        dir_tokens.add(surname)
        if "-" in surname:
            for part in surname.split("-"):
                if len(part) >= 3:
                    dir_tokens.add(part)

    # Companies found via bulk BFS traversal (not the direct PSC seeds)
    hop_companies = conn.execute("""
        SELECT DISTINCT dc.company_number, dc.company_name, dc.role,
               c.status, c.registered_office, c.sic_codes
        FROM director_companies dc
        LEFT JOIN companies c ON c.company_number = dc.company_number
        WHERE dc.role LIKE 'bulk-hop%'
           OR dc.role LIKE 'psc-chain-gen%'
        ORDER BY dc.role, dc.company_name
    """).fetchall()

    rows_out = []
    for co in hop_companies:
        cn = _fmt_cn(co["company_number"] or "")
        if not cn:
            continue

        # Collect all known person names for this company
        psc_names = [
            r[0] or "" for r in
            conn.execute("SELECT psc_name FROM psc WHERE company_number=?", (cn,)).fetchall()
        ]
        officer_names = [
            r[0] or "" for r in
            conn.execute("SELECT officer_name FROM company_officers WHERE company_number=?", (cn,)).fetchall()
        ]
        all_names_upper = [(n or "").upper() for n in psc_names + officer_names]

        # Skip if any target surname appears
        has_target = any(
            token in name
            for name in all_names_upper
            for token in dir_tokens
            if len(token) >= 3
        )
        if has_target:
            continue

        # Get charges
        charges = conn.execute("""
            SELECT charge_holder, charge_status, charge_created
            FROM charges WHERE company_number=?
            ORDER BY charge_created DESC
        """, (cn,)).fetchall()
        n_outstanding = sum(1 for c in charges if (c["charge_status"] or "").lower() == "outstanding")

        # Get properties
        props = conn.execute("""
            SELECT property_address, price_paid, tenure
            FROM properties WHERE owner_company_number=?
        """, (cn,)).fetchall()

        # Known non-target PSCs (corporate parent chain info)
        corp_pscs = conn.execute("""
            SELECT psc_name, registration_number FROM psc
            WHERE company_number=? AND psc_kind LIKE '%corporate%' AND ceased_on IS NULL
        """, (cn,)).fetchall()
        corp_psc_str = " | ".join(
            f"{r['psc_name']} ({r['registration_number']})" for r in corp_pscs if r['psc_name']
        )

        rows_out.append({
            "Company Number":       cn,
            "Company Name":         co["company_name"] or "",
            "Chain Role":           co["role"] or "",
            "Status":               (co["status"] or "unknown").title(),
            "Properties":           len(props),
            "Property Addresses":   " | ".join(p["property_address"] or "" for p in props),
            "Outstanding Charges":  n_outstanding,
            "Charge Holders":       " | ".join(c["charge_holder"] or "" for c in charges),
            "Corporate PSC (parent)": corp_psc_str,
            "Registered Office":    co["registered_office"] or "",
            "SIC Codes":            co["sic_codes"] or "",
            "Known PSC/Officers":   " | ".join(set(n for n in (psc_names + officer_names) if n)),
        })

    conn.close()

    if not rows_out:
        print("[HIDDEN] No hidden chain companies found. Run bulk expansion first.")
        return None

    # Sort: properties first, then by status
    rows_out.sort(key=lambda r: (-r["Properties"], -r["Outstanding Charges"], r["Company Name"]))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("output", exist_ok=True)
    filepath = f"output/hidden_companies_{ts}.csv"

    cols = [
        "Company Number", "Company Name", "Chain Role", "Status",
        "Properties", "Property Addresses", "Outstanding Charges", "Charge Holders",
        "Corporate PSC (parent)", "Registered Office", "SIC Codes", "Known PSC/Officers",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows_out)

    with_props = sum(1 for r in rows_out if r["Properties"] > 0)
    print(f"\n[HIDDEN] {len(rows_out)} companies in corporate chain with NO target director name")
    print(f"[HIDDEN]   {with_props} of these hold registered property")
    print(f"[HIDDEN]   → {filepath}")
    return filepath


def print_summary():
    conn = get_conn()
    print("\n" + "="*70)
    print("ASSET DISCOVERY — SUMMARY")
    print("="*70)

    for d in DIRECTORS:
        name  = d["name"]
        label = _director_label(d)
        total = conn.execute(
            "SELECT COUNT(DISTINCT company_number) FROM director_companies WHERE director_name=?",
            (name,)
        ).fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(DISTINCT company_number) FROM director_companies WHERE director_name=? AND resigned_on IS NULL",
            (name,)
        ).fetchone()[0]
        print(f"\n  {label} — {name}")
        print(f"    Companies: {total} total, {active} active appointments")

    cc = conn.execute("SELECT COUNT(*) FROM charges").fetchone()[0]
    oc = conn.execute("SELECT COUNT(*) FROM charges WHERE charge_status='outstanding'").fetchone()[0]
    pc = conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
    pp = conn.execute("SELECT COUNT(*) FROM personal_properties").fetchone()[0]

    print(f"\n  Properties (corporate): {pc}")
    print(f"  Properties (personal):  {pp}")
    print(f"  Charges: {cc} total, {oc} outstanding")
    print("="*70)
    conn.close()
