import requests
import time
import json
from datetime import datetime

from config import CH_API_KEY, CH_BASE_URL, REQUEST_DELAY
from database import get_conn


def _get(path, params=None, _retries=3):
    url = f"{CH_BASE_URL}{path}"
    try:
        response = requests.get(
            url,
            params=params,
            auth=(CH_API_KEY, ""),
            timeout=15
        )
        time.sleep(REQUEST_DELAY)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        elif response.status_code == 429:
            print(f"[CH] Rate limited. Sleeping 60s...")
            time.sleep(60)
            return _get(path, params, _retries)
        else:
            print(f"[CH] Error {response.status_code} for {url}: {response.text[:100]}")
            return None

    except requests.exceptions.ConnectionError as e:
        print(f"[CH] Connection error for {url}: {e}")
        if _retries > 0:
            print(f"[CH] Retrying in 5s... ({_retries} left)")
            time.sleep(5)
            return _get(path, params, _retries - 1)
        return None
    except requests.exceptions.Timeout:
        print(f"[CH] Timeout for {url}")
        if _retries > 0:
            time.sleep(5)
            return _get(path, params, _retries - 1)
        return None
    except Exception as e:
        print(f"[CH] Unexpected error for {url}: {e}")
        return None


def _log_fetch(fetch_type, fetch_key, result_count=0):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO fetch_log (fetch_type, fetch_key, fetched_at, result_count)
        VALUES (?, ?, ?, ?)
    """, (fetch_type, fetch_key, datetime.now().isoformat(), result_count))
    conn.commit()
    conn.close()


def _already_fetched(fetch_type, fetch_key):
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM fetch_log WHERE fetch_type=? AND fetch_key=?",
        (fetch_type, fetch_key)
    ).fetchone()
    conn.close()
    return row is not None


# -----------------------------------------------------------------------
# STEP 1: Search for a name and get all company appointments
# -----------------------------------------------------------------------

def _extract_surnames(name):
    """
    Extract all surname tokens from a name in either format:
      'SMITH, John William'     → ['SMITH']
      'Alice DOUBLE-BARREL'     → ['DOUBLE', 'BARREL', 'DOUBLE-BARREL']
      'John Smith'              → ['SMITH']
    Returns a set of uppercase tokens that MUST appear in the returned officer name.
    """
    name = name.strip().upper()
    if "," in name:
        # SURNAME, Forename — surname is everything before the comma
        surname_part = name.split(",")[0].strip()
    else:
        # Forename Surname — surname is the last word
        parts = name.split()
        surname_part = parts[-1] if parts else name

    # Split hyphenated surnames into individual tokens plus the full hyphenated form
    tokens = set()
    tokens.add(surname_part)
    if "-" in surname_part:
        for part in surname_part.split("-"):
            if part:
                tokens.add(part)
    return tokens


def search_officer_by_name(search_name, canonical_name, dob_year=None, dob_month=None):
    """
    Search CH for a single name string and return verified matches only.

    The CH /search/officers API is a loose full-text search — searching
    a given name may return many unrelated results from the register.
    
    Fix: after getting results, we filter client-side to ensure the
    returned officer name actually contains the target surname(s).
    DOB is used as a secondary filter only when explicitly present and wrong.
    """
    print(f"  [CH] Searching: '{search_name}'")
    data = _get("/search/officers", params={"q": search_name, "items_per_page": 100})
    if not data:
        return []

    items = data.get("items", [])
    required_surnames = _extract_surnames(search_name)
    print(f"  [CH] Required surname tokens: {required_surnames}")

    results = []
    skipped_name   = 0
    skipped_dob    = 0

    for item in items:
        officer_id = item.get("links", {}).get("self", "").split("/officers/")[-1].split("/")[0]
        if not officer_id:
            continue

        returned_name = (item.get("title") or "").upper().strip()

        # --- NAME FILTER (primary) ---
        # Every required surname token must appear in the returned name
        name_match = all(token in returned_name for token in required_surnames)
        if not name_match:
            skipped_name += 1
            continue

        # --- DOB FILTER (secondary — only when DOB is explicitly present) ---
        dob        = item.get("date_of_birth", {})
        item_year  = dob.get("year")
        item_month = dob.get("month")

        if item_year is not None:
            if dob_year and item_year != dob_year:
                print(f"  [CH] SKIP {returned_name} — DOB year mismatch ({item_year} != {dob_year})")
                skipped_dob += 1
                continue
            if dob_month and item_month and item_month != dob_month:
                print(f"  [CH] SKIP {returned_name} — DOB month mismatch ({item_month} != {dob_month})")
                skipped_dob += 1
                continue
            dob_status = "confirmed"
        else:
            # DOB absent from search result.
            # If we have a DOB to verify against, skip — too risky to include.
            # Only include if we have no DOB info at all to check against.
            if dob_year:
                skipped_dob += 1
                continue
            dob_status = "no_dob_in_search"

        results.append((officer_id, item.get("title", search_name), dob_status))

    confirmed = sum(1 for _, _, s in results if s == "confirmed")
    print(f"  [CH] '{search_name}' → {len(results)} verified matches "
          f"({confirmed} DOB confirmed) | skipped: {skipped_name} wrong name, {skipped_dob} wrong DOB/no DOB")
    return results


def get_officer_appointments(officer_id, canonical_name, director_dob, search_name):
    """Fetch all company appointments for a given officer ID."""
    fetch_key = f"{officer_id}:{canonical_name}"
    if _already_fetched("appointments", fetch_key):
        print(f"  [CH] Already fetched appointments for {officer_id}, skipping.")
        return 0

    start_index = 0
    all_items = []

    while True:
        data = _get(
            f"/officers/{officer_id}/appointments",
            params={"items_per_page": 50, "start_index": start_index}
        )
        if not data:
            break
        items = data.get("items", [])
        all_items.extend(items)
        total = data.get("total_results", 0)
        start_index += len(items)
        if start_index >= total or not items:
            break

    conn = get_conn()
    saved = 0
    for appt in all_items:
        company = appt.get("appointed_to", {})
        try:
            conn.execute("""
                INSERT OR IGNORE INTO director_companies
                (director_name, director_dob, company_number, company_name,
                 role, appointed_on, resigned_on, officer_id, search_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                canonical_name,
                director_dob,
                company.get("company_number"),
                company.get("company_name"),
                appt.get("officer_role"),
                appt.get("appointed_on"),
                appt.get("resigned_on"),
                officer_id,
                search_name
            ))
            saved += 1
        except Exception as e:
            print(f"  [DB] Insert error: {e}")

    conn.commit()
    conn.close()
    _log_fetch("appointments", fetch_key, saved)
    print(f"  [CH] officer {officer_id}: {saved} appointments saved")
    return saved


def step1_director_companies(directors):
    """
    Step 1: For each director, search EVERY alias and the primary name.
    Each distinct name variant is queried independently at CH.
    """
    print("\n" + "="*60)
    print("STEP 1: Finding all company appointments")
    print("="*60)

    for director in directors:
        canonical = director["name"]
        dob_year  = director.get("dob_year")
        dob_month = director.get("dob_month")
        dob_str   = f"{dob_year or '?'}-{dob_month or '?':02d}" if dob_year else "unknown"

        # Build full list of names to search: primary + all aliases
        all_names = [canonical] + director.get("aliases", [])
        # Deduplicate while preserving order
        seen = set()
        search_names = []
        for n in all_names:
            key = n.upper().strip()
            if key not in seen:
                seen.add(key)
                search_names.append(n)

        print(f"\n[Director] {canonical} — searching {len(search_names)} name variant(s)")

        seen_officer_ids = set()
        for name in search_names:
            matches = search_officer_by_name(name, canonical, dob_year, dob_month)
            for officer_id, display, dob_status in matches:
                if officer_id not in seen_officer_ids:
                    seen_officer_ids.add(officer_id)
                    get_officer_appointments(officer_id, canonical, dob_str, name)

    # Summary
    conn = get_conn()
    rows = conn.execute("""
        SELECT director_name, COUNT(DISTINCT company_number) as cnt
        FROM director_companies
        GROUP BY director_name
    """).fetchall()
    conn.close()

    print("\n[STEP 1 SUMMARY]")
    for row in rows:
        print(f"  {row['director_name']}: {row['cnt']} companies")


# -----------------------------------------------------------------------
# STEP 2: Expand company network
# -----------------------------------------------------------------------

def _address_str(addr_dict):
    return ", ".join(filter(None, [
        addr_dict.get("premises"),
        addr_dict.get("address_line_1"),
        addr_dict.get("address_line_2"),
        addr_dict.get("locality"),
        addr_dict.get("region"),
        addr_dict.get("postal_code"),
        addr_dict.get("country"),
    ]))


def get_company_details(company_number):
    if _already_fetched("company", company_number):
        return
    data = _get(f"/company/{company_number}")
    if not data:
        return

    address_str = _address_str(data.get("registered_office_address", {}))
    sic_codes = json.dumps(data.get("sic_codes", []))

    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO companies
        (company_number, company_name, status, company_type, incorporated_on,
         registered_office, sic_codes, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        company_number,
        data.get("company_name"),
        data.get("company_status"),
        data.get("type"),
        data.get("date_of_creation"),
        address_str,
        sic_codes,
        datetime.now().isoformat()
    ))
    if address_str:
        conn.execute("""
            INSERT OR IGNORE INTO addresses (address_raw, source_type, source_id, source_name)
            VALUES (?, 'company', ?, ?)
        """, (address_str, company_number, data.get("company_name")))
    conn.commit()
    conn.close()
    _log_fetch("company", company_number, 1)


def get_company_psc(company_number):
    if _already_fetched("psc", company_number):
        return
    data = _get(f"/company/{company_number}/persons-with-significant-control",
                params={"items_per_page": 100})
    if not data:
        return

    items = data.get("items", [])
    conn = get_conn()
    for item in items:
        address_str = _address_str(item.get("address", {}))
        dob = item.get("date_of_birth", {})
        natures = json.dumps(item.get("natures_of_control", []))
        # For corporate PSCs, extract their registration number
        identification = item.get("identification", {})
        reg_number = identification.get("registration_number", "") if identification else ""
        try:
            conn.execute("""
                INSERT OR IGNORE INTO psc
                (company_number, psc_name, psc_kind, natures_of_control, notified_on,
                 ceased_on, dob_year, dob_month, nationality, country_of_residence, address,
                 registration_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company_number, item.get("name"), item.get("kind"), natures,
                item.get("notified_on"), item.get("ceased_on"),
                dob.get("year"), dob.get("month"),
                item.get("nationality"), item.get("country_of_residence"), address_str,
                reg_number
            ))
            if address_str:
                conn.execute("""
                    INSERT OR IGNORE INTO addresses (address_raw, source_type, source_id, source_name)
                    VALUES (?, 'psc', ?, ?)
                """, (address_str, company_number, item.get("name")))
        except Exception as e:
            print(f"  [DB] PSC insert: {e}")
    conn.commit()
    conn.close()
    _log_fetch("psc", company_number, len(items))


def get_company_officers(company_number):
    if _already_fetched("officers", company_number):
        return
    data = _get(f"/company/{company_number}/officers", params={"items_per_page": 100})
    if not data:
        return

    items = data.get("items", [])
    conn = get_conn()
    for item in items:
        address_str = _address_str(item.get("address", {}))
        dob = item.get("date_of_birth", {})
        officer_link = item.get("links", {}).get("officer", {}).get("appointments", "")
        officer_id = officer_link.split("/officers/")[-1].split("/")[0] if officer_link else None
        try:
            conn.execute("""
                INSERT OR IGNORE INTO company_officers
                (company_number, officer_name, officer_role, appointed_on, resigned_on,
                 dob_year, dob_month, nationality, country_of_residence, address, officer_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company_number, item.get("name"), item.get("officer_role"),
                item.get("appointed_on"), item.get("resigned_on"),
                dob.get("year"), dob.get("month"),
                item.get("nationality"), item.get("country_of_residence"),
                address_str, officer_id
            ))
            if address_str:
                conn.execute("""
                    INSERT OR IGNORE INTO addresses (address_raw, source_type, source_id, source_name)
                    VALUES (?, 'officer', ?, ?)
                """, (address_str, company_number, item.get("name")))
        except Exception as e:
            print(f"  [DB] Officer insert: {e}")
    conn.commit()
    conn.close()
    _log_fetch("officers", company_number, len(items))


def step2_expand_companies():
    """
    Step 2: Profile every company found — details, PSC, co-directors.
    Also searches CH for companies where PSC names appear as officers
    (catches SPVs not directly linked to original directors).
    """
    print("\n" + "="*60)
    print("STEP 2: Expanding company network")
    print("="*60)

    conn = get_conn()
    companies = conn.execute("""
        SELECT DISTINCT company_number, company_name FROM director_companies
        WHERE company_number IS NOT NULL
    """).fetchall()
    conn.close()

    print(f"[STEP 2] Profiling {len(companies)} companies...")
    for i, row in enumerate(companies):
        cn = row["company_number"]
        print(f"\n[{i+1}/{len(companies)}] {row['company_name']} ({cn})")
        get_company_details(cn)
        get_company_psc(cn)
        get_company_officers(cn)

    # Summary
    conn = get_conn()
    psc_count  = conn.execute("SELECT COUNT(DISTINCT psc_name) FROM psc").fetchone()[0]
    addr_count = conn.execute("SELECT COUNT(*) FROM addresses").fetchone()[0]
    conn.close()
    print(f"\n[STEP 2 SUMMARY] {len(companies)} companies | {psc_count} PSC records | {addr_count} addresses")


def step2b_follow_corporate_pscs(directors):
    """
    Step 2b: Downward PSC ownership chain — 3 generations.

    Starting from list-1 companies (direct director appointments), find
    companies that list-1 companies CONTROL as corporate PSC with ≥25%
    ownership. Filter: Active companies only. Repeat for gen 2 and gen 3.

    Uses ch_bulk.db (local offline data — no API calls, no rate limits).
    Prints count per generation so you can see if the network converges
    or diverges.

    Adds discovered companies to director_companies with role:
      psc-chain-gen1, psc-chain-gen2, psc-chain-gen3
    """
    import os
    import sqlite3
    from collections import deque
    from config import CH_BULK_DB_PATH

    print("\n" + "="*60)
    print("STEP 2b: Downward PSC ownership chain (gen 1 → 2 → 3)")
    print("="*60)

    # --- Open bulk DB ---
    if not os.path.exists(CH_BULK_DB_PATH):
        print(f"[2b] ch_bulk.db not found at '{CH_BULK_DB_PATH}' — skipping.")
        print("[2b] Run:  python load_ch_bulk.py  (takes ~15 mins)")
        return

    bulk = sqlite3.connect(CH_BULK_DB_PATH)
    bulk.execute("PRAGMA query_only=ON")
    bulk.row_factory = sqlite3.Row

    app = get_conn()

    # --- Seed: companies where configured directors have an ACTIVE (not resigned) role ---
    # Resigned director appointments are excluded: those companies are no longer
    # under the directors' control so following their corporate PSC chains
    # would produce false positives.
    list1 = set(
        r[0].strip().upper()
        for r in app.execute("""
            SELECT DISTINCT company_number FROM director_companies
            WHERE company_number IS NOT NULL
              AND resigned_on IS NULL
        """).fetchall()
        if r[0]
    )
    print(f"[2b] Seed: {len(list1)} companies with active director role")

    # --- BFS across 3 generations ---
    # visited: all company numbers we've already processed or queued
    visited  = set(list1)
    # current frontier = list-1
    frontier = list(list1)

    # gen_results[g] = list of (company_number, company_name, status) found at gen g
    gen_results = {}

    MAX_GEN = 3

    for gen in range(1, MAX_GEN + 1):
        next_frontier = []
        found_this_gen = []

        print(f"\n[2b] Generation {gen} — checking {len(frontier)} parent companies...")

        for parent_cn in frontier:
            # Find companies where parent_cn is a corporate PSC with ≥25%
            rows = bulk.execute("""
                SELECT DISTINCT p.company_number, c.company_name, c.status,
                       p.natures_of_control
                FROM   psc p
                LEFT JOIN companies c ON c.company_number = p.company_number
                WHERE  p.registration_number = ?
                  AND  p.psc_kind LIKE '%corporate%'
                  AND  p.ceased_on IS NULL
                  AND  (   p.natures_of_control LIKE '%25-to-50%'
                        OR p.natures_of_control LIKE '%50-to-75%'
                        OR p.natures_of_control LIKE '%75-to-100%')
            """, (parent_cn,)).fetchall()

            for row in rows:
                child_cn   = (row["company_number"] or "").strip().upper()
                child_name = row["company_name"] or ""
                status     = (row["status"] or "").lower()

                if not child_cn or child_cn in visited:
                    continue

                visited.add(child_cn)

                # Only traverse and report Active companies
                if status != "active":
                    continue

                found_this_gen.append((child_cn, child_name, status))
                next_frontier.append(child_cn)

                # Write to asset_discovery.db
                role = f"psc-chain-gen{gen}"
                try:
                    app.execute("""
                        INSERT OR IGNORE INTO director_companies
                        (director_name, company_number, company_name, role, search_name)
                        VALUES (?, ?, ?, ?, ?)
                    """, ("PSC-CHAIN", child_cn, child_name, role, f"2b-gen{gen}"))
                    # Also add to companies table (from bulk data)
                    app.execute("""
                        INSERT OR IGNORE INTO companies
                        (company_number, company_name, status, fetched_at)
                        VALUES (?, ?, ?, ?)
                    """, (child_cn, child_name, status,
                          datetime.now().isoformat()))
                except Exception as e:
                    print(f"  [DB] {e}")

        app.commit()
        gen_results[gen] = found_this_gen
        frontier = next_frontier

        print(f"[2b] Generation {gen}: {len(found_this_gen)} active secondary companies found")
        for cn, name, _ in found_this_gen:
            print(f"       {name or cn} ({cn})")

        if not frontier:
            print(f"[2b] → Converges: no further companies at gen {gen + 1}")
            break

    bulk.close()
    app.close()

    # --- Convergence summary ---
    print(f"\n{'='*60}")
    print("STEP 2b SUMMARY — Downward PSC Chain")
    print(f"{'='*60}")
    total = 0
    for g in range(1, MAX_GEN + 1):
        n = len(gen_results.get(g, []))
        total += n
        arrow = "→ converges" if n == 0 else ("⚠ diverging" if (g > 1 and n > len(gen_results.get(g-1, []))) else "")
        print(f"  Gen {g}: {n:>4} active companies  {arrow}")
    print(f"  Total secondary companies added: {total}")
    print(f"{'='*60}")


# -----------------------------------------------------------------------
# STEP 2c: Recursive network expansion
# -----------------------------------------------------------------------

def _search_company_as_officer(company_name, company_number):
    """Search CH for companies where company_name appears as a director/officer."""
    data = _get("/search/officers", params={"q": company_name, "items_per_page": 100})
    if not data:
        return []
    results = []
    for item in data.get("items", []):
        returned_name = (item.get("title") or "").upper().strip()
        # Must be a reasonable name match
        # Use first significant word of company name (skip THE/A etc)
        words = [w for w in company_name.upper().split() if w not in ("THE", "A", "AN", "OF", "AND", "&", "LTD", "LIMITED", "PLC")]
        if not words:
            continue
        # At least first two significant words must match
        match_words = words[:2]
        if not all(w in returned_name for w in match_words):
            continue
        officer_id = item.get("links", {}).get("self", "").split("/officers/")[-1].split("/")[0]
        if officer_id:
            results.append(officer_id)
    return results


def _get_officer_companies(officer_id):
    """Get all company numbers for a given officer ID."""
    data = _get(f"/officers/{officer_id}/appointments", params={"items_per_page": 50})
    if not data:
        return []
    companies = []
    for item in data.get("items", []):
        co = item.get("appointed_to", {})
        cn = co.get("company_number", "")
        name = co.get("company_name", "")
        if cn:
            companies.append((cn, name))
    return companies


def step2c_recursive_network_expansion():
    """
    Step 2c: Recursively expand the company network.

    Two directions per known company X:
      a) Search CH for companies where X appears as an officer/director
         → finds subsidiaries or related companies X directs
      b) Check X's own officers and PSCs for corporate entities
         → fetch those entities and add to network

    Repeat until no new companies are found.
    """
    print("\n" + "="*60)
    print("STEP 2c: Recursive network expansion")
    print("="*60)

    conn = get_conn()

    def known_companies():
        return set(
            r[0] for r in conn.execute(
                "SELECT DISTINCT company_number FROM director_companies WHERE company_number IS NOT NULL"
            ).fetchall()
        )

    total_new = 0
    iteration = 0

    while True:
        iteration += 1
        before = known_companies()
        newly_found = {}  # cn -> name

        print(f"\n[2c] Iteration {iteration} — {len(before)} companies in network")

        for cn in list(before):
            # Get company name
            co_row = conn.execute(
                "SELECT company_name FROM companies WHERE company_number=?", (cn,)
            ).fetchone()
            if not co_row or not co_row["company_name"]:
                continue
            co_name = co_row["company_name"]

            # --- Direction A: search CH for companies where this company is an officer ---
            fetch_key = f"co_as_officer:{cn}"
            if not _already_fetched("co_as_officer", cn):
                officer_ids = _search_company_as_officer(co_name, cn)
                for oid in officer_ids:
                    for found_cn, found_name in _get_officer_companies(oid):
                        if found_cn not in before and found_cn not in newly_found:
                            newly_found[found_cn] = found_name
                _log_fetch("co_as_officer", cn, len(officer_ids))

            # --- Direction B: corporate officers/PSCs of this company ---
            fetch_key_b = f"corp_officers:{cn}"
            if not _already_fetched("corp_officers", cn):
                # Corporate PSCs
                psc_rows = conn.execute("""
                    SELECT registration_number, psc_name FROM psc
                    WHERE company_number=? AND psc_kind LIKE '%corporate%'
                    AND registration_number IS NOT NULL AND registration_number != ''
                    AND ceased_on IS NULL
                """, (cn,)).fetchall()
                for p in psc_rows:
                    reg = p["registration_number"].strip()
                    if reg and reg not in before and reg not in newly_found:
                        newly_found[reg] = p["psc_name"]

                # Corporate officers (companies appointed as directors)
                off_rows = conn.execute("""
                    SELECT officer_name, officer_id FROM company_officers
                    WHERE company_number=? AND resigned_on IS NULL
                    AND (officer_name LIKE '%LIMITED%' OR officer_name LIKE '%LTD%'
                         OR officer_name LIKE '%PLC%' OR officer_name LIKE '%LLP%')
                """, (cn,)).fetchall()
                for o in off_rows:
                    # Try to resolve officer_id to a company number
                    if o["officer_id"]:
                        appt_data = _get(f"/officers/{o['officer_id']}/appointments",
                                         params={"items_per_page": 5})
                        if appt_data:
                            for item in appt_data.get("items", []):
                                found_cn = item.get("appointed_to", {}).get("company_number", "")
                                found_name = item.get("appointed_to", {}).get("company_name", "")
                                if found_cn and found_cn not in before and found_cn not in newly_found:
                                    newly_found[found_cn] = found_name

                _log_fetch("corp_officers", cn, 1)

        # Add newly found companies to director_companies under a special marker
        new_this_round = 0
        for new_cn, new_name in newly_found.items():
            # Fetch details first
            get_company_details(new_cn)
            get_company_psc(new_cn)
            get_company_officers(new_cn)
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO director_companies
                    (director_name, company_number, company_name, role, search_name)
                    VALUES (?, ?, ?, 'network-expansion', ?)
                """, ("NETWORK", new_cn, new_name, "network-expansion"))
                conn.commit()
                new_this_round += 1
                print(f"  [2c] NEW: {new_name} ({new_cn})")
            except Exception as e:
                print(f"  [DB] {e}")

        total_new += new_this_round
        print(f"[2c] Iteration {iteration} complete — {new_this_round} new companies found")

        if new_this_round == 0:
            break

    conn.close()

    print(f"\n[STEP 2c SUMMARY] {iteration} iterations | {total_new} new companies added to network")
    if total_new > 0:
        # Print what we found
        conn = get_conn()
        new_cos = conn.execute("""
            SELECT dc.company_number, dc.company_name, c.status, c.sic_codes
            FROM director_companies dc
            LEFT JOIN companies c ON c.company_number = dc.company_number
            WHERE dc.role = 'network-expansion'
            ORDER BY dc.company_name
        """).fetchall()
        conn.close()
        print(f"\n{'─'*60}")
        print(f"NEW COMPANIES FOUND VIA NETWORK EXPANSION")
        print(f"{'─'*60}")
        for r in new_cos:
            sic = r["sic_codes"] or ""
            print(f"  {r['company_name']:<45} {r['company_number']}  {r['status'] or '?':<12}  SIC: {sic}")
        print(f"{'─'*60}")


# -----------------------------------------------------------------------
# STEP 3: Charges
# -----------------------------------------------------------------------

def get_company_charges(company_number):
    if _already_fetched("charges", company_number):
        return
    data = _get(f"/company/{company_number}/charges", params={"items_per_page": 100})
    if not data:
        return

    items = data.get("items", [])
    conn = get_conn()
    saved = 0
    for item in items:
        persons = item.get("persons_entitled", [])
        holder = "; ".join([p.get("name", "") for p in persons])
        links = item.get("links", {})
        doc_link = f"https://api.company-information.service.gov.uk{links['self']}" if links.get("self") else ""
        try:
            conn.execute("""
                INSERT OR IGNORE INTO charges
                (company_number, charge_code, charge_holder, charge_created, charge_status,
                 charge_description, assets_ceased_released, charge_document_link)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company_number,
                item.get("charge_code"),
                holder,
                item.get("created_on"),
                item.get("status"),
                item.get("particulars", {}).get("description", ""),
                item.get("assets_ceased_released", ""),
                doc_link
            ))
            saved += 1
        except Exception as e:
            print(f"  [DB] Charge insert: {e}")
    conn.commit()
    conn.close()
    _log_fetch("charges", company_number, saved)
    if saved:
        print(f"  [CH] {saved} charges for {company_number}")


def step3_get_charges():
    """Step 3: Pull charges for every known company."""
    print("\n" + "="*60)
    print("STEP 3: Fetching charges register")
    print("="*60)

    conn = get_conn()
    companies = conn.execute("""
        SELECT DISTINCT company_number, company_name FROM director_companies
        WHERE company_number IS NOT NULL
    """).fetchall()
    conn.close()

    for i, row in enumerate(companies):
        print(f"[{i+1}/{len(companies)}] {row['company_name']} ({row['company_number']})")
        get_company_charges(row["company_number"])

    conn = get_conn()
    charge_count = conn.execute("SELECT COUNT(*) FROM charges").fetchone()[0]
    lenders = conn.execute("""
        SELECT charge_holder, COUNT(*) as cnt FROM charges
        WHERE charge_holder != '' GROUP BY charge_holder ORDER BY cnt DESC LIMIT 15
    """).fetchall()
    conn.close()

    print(f"\n[STEP 3 SUMMARY] {charge_count} charges found")
    print("  Lenders:")
    for l in lenders:
        print(f"    {l['charge_holder']}: {l['cnt']}")
