"""
CH Bulk Expansion — Step 4 (bulk)

Uses ch_bulk.db (built by load_ch_bulk.py) to:

  1. Find ALL companies where any configured director appears as an individual PSC,
     matched by DOB year+month + surname confirmation.

  2. For each such company, recursively follow corporate PSC chains:
     if one of our companies is itself a corporate PSC of another company,
     add that company to the network too. No hop limit.

  3. Stop traversing (but still record) dissolved / liquidated companies.

Results are written directly into asset_discovery.db (director_companies
and companies tables) without any API calls.

After this step, run step2_expand_companies() again (incremental) to fetch
PSC/officer/charge data for newly found companies via the CH API.
"""

import os
import json
import sqlite3
from collections import deque
from datetime import datetime

from config import CH_BULK_DB_PATH, DIRECTORS
from database import get_conn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Company statuses where we record the company but do NOT traverse further
_STOP_STATUSES = frozenset({
    "dissolved", "liquidation", "administration",
    "receivership", "converted-closed", "insolvency-proceedings",
})


def _is_traversable(status: str) -> bool:
    return (status or "").lower().strip() not in _STOP_STATUSES


def _fmt_cn(cn: str) -> str:
    """Normalise to 8-digit zero-padded uppercase string."""
    if not cn:
        return ""
    cn = cn.strip().upper()
    if cn.isdigit():
        return cn.zfill(8)
    return cn


def _surname_tokens(director: dict) -> set:
    """
    Extract surname tokens for bulk name matching.
    e.g. 'HYPHEN-NAME, Forename'   → {'HYPHEN-NAME', 'HYPHEN', 'NAME'}
         'SMITH, John William'     → {'SMITH'}
    """
    name = director["name"]
    if "," in name:
        surname = name.split(",")[0].strip().upper()
    else:
        surname = name.split()[-1].upper()

    tokens = {surname}
    if "-" in surname:
        for part in surname.split("-"):
            if part:
                tokens.add(part)
    return tokens


def _get_bulk_conn():
    if not os.path.exists(CH_BULK_DB_PATH):
        return None
    conn = sqlite3.connect(CH_BULK_DB_PATH)
    conn.execute("PRAGMA query_only=ON")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Step 1: find companies where a director is a direct individual PSC
# ---------------------------------------------------------------------------

def _find_direct_psc_companies(bulk, director: dict) -> dict:
    """
    Search ch_bulk.db PSC table for records matching this director
    by birth YEAR (not month) + surname confirmation.

    Includes both active and ceased PSC records.
    Returns {company_number: (psc_name, is_active)} for all confirmed matches.
      is_active = True  → current PSC (ceased_on IS NULL)
      is_active = False → past/ceased PSC (ceased_on IS NOT NULL)
    """
    dob_year = director.get("dob_year")
    surnames = _surname_tokens(director)

    if not dob_year:
        print(f"  [BULK] {director['name']}: birth year not set — cannot search PSC snapshot")
        return {}

    # Filter by year only (no month) — uses idx_psc_dob index efficiently.
    # Ceased records included; confirmation by surname in Python.
    rows = bulk.execute("""
        SELECT company_number, psc_name, ceased_on
        FROM psc
        WHERE psc_kind LIKE '%individual%'
          AND dob_year = ?
    """, (dob_year,)).fetchall()

    # For hyphenated surnames (e.g. DOUBLE-BARREL) require ALL component parts
    # to appear in the name, preventing false positives from common single-part
    # surnames matching unrelated individuals.
    surname = director["name"].split(",")[0].strip().upper()
    if "-" in surname:
        # e.g. DOUBLE-BARREL → must contain both DOUBLE and BARREL
        required_parts = [p for p in surname.split("-") if len(p) >= 3]
        def _name_matches(name: str) -> bool:
            return all(part in name for part in required_parts)
    else:
        # Simple surname — any token match is sufficient (already single token)
        def _name_matches(name: str) -> bool:
            return any(tok in name for tok in surnames if len(tok) >= 3)

    confirmed = {}
    skipped   = 0
    for row in rows:
        name = (row["psc_name"] or "").upper()
        if _name_matches(name):
            cn = _fmt_cn(row["company_number"])
            if cn:
                is_active = (row["ceased_on"] is None or row["ceased_on"] == "")
                # If multiple records for same company, prefer active over ceased
                if cn not in confirmed or is_active:
                    confirmed[cn] = (row["psc_name"], is_active)
        else:
            skipped += 1

    active_count = sum(1 for _, (_, a) in confirmed.items() if a)
    ceased_count = len(confirmed) - active_count
    print(f"  [BULK] {director['name']} (birth year {dob_year}): "
          f"{active_count} active + {ceased_count} past PSC matches "
          f"(+{skipped} same-year rejected on name)")
    return confirmed


# ---------------------------------------------------------------------------
# Step 2: find companies where a known company is a corporate PSC
# ---------------------------------------------------------------------------

def _find_corporate_children(bulk, company_number: str) -> list:
    """
    Return all company_numbers where 'company_number' appears as an active
    corporate PSC (i.e. our company controls those companies).
    """
    rows = bulk.execute("""
        SELECT DISTINCT company_number
        FROM psc
        WHERE registration_number = ?
          AND psc_kind LIKE '%corporate%'
          AND ceased_on IS NULL
    """, (company_number,)).fetchall()
    return [_fmt_cn(r["company_number"]) for r in rows if r["company_number"]]


# ---------------------------------------------------------------------------
# Step 3: get company name + status from bulk DB
# ---------------------------------------------------------------------------

def _get_bulk_company(bulk, company_number: str):
    """Return (company_name, status) from ch_bulk.db or ('', '') if not found."""
    row = bulk.execute(
        "SELECT company_name, status FROM companies WHERE company_number = ?",
        (company_number,)
    ).fetchone()
    if row:
        return row["company_name"] or "", row["status"] or ""
    return "", ""


# ---------------------------------------------------------------------------
# Write results to asset_discovery.db
# ---------------------------------------------------------------------------

def _upsert_director_company(app, director_name: str, cn: str,
                              company_name: str, role: str):
    try:
        app.execute("""
            INSERT OR IGNORE INTO director_companies
                (director_name, company_number, company_name, role, search_name)
            VALUES (?, ?, ?, ?, ?)
        """, (director_name, cn, company_name, role, "bulk-expansion"))
    except Exception as e:
        print(f"  [DB] director_companies insert: {e}")


def _upsert_company(app, cn: str, name: str, status: str):
    """Insert into asset_discovery.db companies table (from bulk data)."""
    try:
        # Only insert if not already present — don't overwrite API-fetched data
        app.execute("""
            INSERT OR IGNORE INTO companies
                (company_number, company_name, status, fetched_at)
            VALUES (?, ?, ?, ?)
        """, (cn, name, status.lower() if status else None,
              datetime.now().isoformat()))
    except Exception as e:
        print(f"  [DB] companies insert: {e}")


# ---------------------------------------------------------------------------
# Main expansion function
# ---------------------------------------------------------------------------

def _find_api_psc_seeds(app, director: dict) -> dict:
    """
    Find companies already in asset_discovery.db (from step1/step2 API calls)
    where this director is confirmed as an active individual PSC.

    Returns {company_number: (psc_name, True)} — same shape as _find_direct_psc_companies.
    These supplement the bulk snapshot search so that API-found companies also
    seed the corporate BFS, even if the snapshot missed them.
    """
    surnames = _surname_tokens(director)
    like_clauses = " OR ".join(f"psc_name LIKE '%{s}%'" for s in surnames if len(s) >= 3)
    if not like_clauses:
        return {}

    rows = app.execute(f"""
        SELECT DISTINCT p.company_number, p.psc_name
        FROM psc p
        WHERE p.psc_kind LIKE '%individual%'
          AND p.ceased_on IS NULL
          AND ({like_clauses})
    """).fetchall()

    seeds = {}
    for row in rows:
        cn = _fmt_cn(row[0] or "")
        if cn:
            seeds[cn] = (row[1] or "", True)

    if seeds:
        print(f"  [BULK] {director['name']}: +{len(seeds)} API-confirmed PSC seed(s) from local DB")
    return seeds


def step_bulk_expansion(directors=None):
    """
    Bulk PSC expansion using ch_bulk.db.

    For each director:
      - Find all companies where they are an active individual PSC
        (via bulk PSC snapshot + API-fetched local data)
      - BFS traverse corporate PSC chains from those companies (no hop limit)
      - Add all discovered companies to asset_discovery.db

    Dissolved/liquidated companies are recorded but not traversed further.
    """
    print("\n" + "="*60)
    print("STEP 4 (BULK): CH bulk PSC expansion")
    print("="*60)

    if directors is None:
        directors = DIRECTORS

    bulk = _get_bulk_conn()
    if bulk is None:
        print(f"[BULK] ch_bulk.db not found at '{CH_BULK_DB_PATH}'.")
        print("[BULK] Run:  python load_ch_bulk.py  (takes ~15 mins)")
        print("[BULK] Skipping bulk expansion.")
        return

    app = get_conn()

    # Track all company numbers already in asset_discovery.db before we start
    existing = set(
        _fmt_cn(r[0]) for r in
        app.execute("SELECT DISTINCT company_number FROM director_companies WHERE company_number IS NOT NULL").fetchall()
    )

    total_direct = 0
    total_chain  = 0
    total_new    = 0

    for director in directors:
        dname = director["name"]
        print(f"\n[BULK] Director: {dname}")

        # --- Direct PSC search: bulk snapshot + API-confirmed local DB ---
        direct = _find_direct_psc_companies(bulk, director)
        api_seeds = _find_api_psc_seeds(app, director)
        # Merge: bulk snapshot takes priority (it has ceased_on info)
        for cn, val in api_seeds.items():
            if cn not in direct:
                direct[cn] = val
        total_direct += len(direct)

        # BFS from direct companies, following corporate chains.
        # Ceased PSC records are recorded but NOT traversed — the director
        # no longer controls that company, so we don't follow its chain.
        visited: set = set()
        queue:  deque = deque()

        for cn, (psc_name, is_active) in direct.items():
            cn = _fmt_cn(cn)
            if cn and cn not in visited:
                visited.add(cn)
                role = "bulk-direct" if is_active else "bulk-direct-ceased"
                # Only traverse active PSC links
                if is_active:
                    queue.append((cn, role, 0))
                else:
                    # Record ceased link directly without BFS traversal
                    name, status = _get_bulk_company(bulk, cn)
                    _upsert_director_company(app, dname, cn, name, role)
                    _upsert_company(app, cn, name, status)

        new_this_dir = 0
        chain_found  = 0

        while queue:
            cn, role, hop = queue.popleft()

            name, status = _get_bulk_company(bulk, cn)

            # Record in asset_discovery.db
            _upsert_director_company(app, dname, cn, name, role)
            _upsert_company(app, cn, name, status)

            if cn not in existing:
                new_this_dir += 1
                existing.add(cn)   # don't double-count across directors

            # Do NOT traverse dissolved/liquidated companies
            if not _is_traversable(status):
                status_str = f" [{status}]" if status else ""
                if hop == 0:
                    print(f"  [BULK] {name or cn}{status_str} — direct PSC, not traversing")
                continue

            # Follow corporate chain: find companies where 'cn' is corporate PSC
            children = _find_corporate_children(bulk, cn)
            for child_cn in children:
                child_cn = _fmt_cn(child_cn)
                if child_cn and child_cn not in visited:
                    visited.add(child_cn)
                    child_role = f"bulk-hop{hop+1}"
                    queue.append((child_cn, child_role, hop + 1))
                    chain_found += 1

        app.commit()
        total_chain += chain_found
        total_new   += new_this_dir
        print(f"  [BULK] {dname}: {len(visited)} companies found "
              f"({len(direct)} direct + {chain_found} via corporate chain), "
              f"{new_this_dir} new to network")

    bulk.close()
    app.close()

    print(f"\n[BULK SUMMARY]")
    print(f"  Direct PSC matches (name + birth year): {total_direct}")
    print(f"  Corporate chain companies found:        {total_chain}")
    print(f"  New companies added to network:         {total_new}")
    print(f"  Note: 'bulk-direct-ceased' = past PSC link, recorded for LR scan")
    print(f"        but NOT traversed (director no longer controls).")
    if total_new > 0:
        print(f"\n  Re-run step2 (incremental) to fetch PSC/officer data for new companies.")


# ---------------------------------------------------------------------------
# Diagnostic: show what the bulk DB knows about a company
# ---------------------------------------------------------------------------

def bulk_company_info(company_number: str):
    """Print bulk DB info for a specific company number. Useful for debugging."""
    bulk = _get_bulk_conn()
    if not bulk:
        print("ch_bulk.db not found.")
        return

    cn = _fmt_cn(company_number)
    row = bulk.execute(
        "SELECT * FROM companies WHERE company_number=?", (cn,)
    ).fetchone()
    if row:
        print(f"Company: {row['company_name']}  Status: {row['status']}")
    else:
        print(f"Company {cn} not found in bulk companies table.")

    pscs = bulk.execute(
        "SELECT psc_name, psc_kind, natures_of_control, ceased_on FROM psc WHERE company_number=?",
        (cn,)
    ).fetchall()
    print(f"PSC records: {len(pscs)}")
    for p in pscs:
        ceased = f"  ceased:{p['ceased_on']}" if p['ceased_on'] else ""
        print(f"  {p['psc_name']:<45} [{p['psc_kind']}]{ceased}")

    bulk.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        bulk_company_info(sys.argv[1])
    else:
        step_bulk_expansion()
