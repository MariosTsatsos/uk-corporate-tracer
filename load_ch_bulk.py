#!/usr/bin/env python3
"""
One-time loader: populates ch_bulk.db from CH bulk data files.

Run ONCE before using ch_bulk.py:
  python load_ch_bulk.py

Sources (paths set in config.py):
  CH_COMPANIES_CSV  — BasicCompanyDataAsOneFile-*.csv  (~5M rows)
  CH_PSC_SNAPSHOT   — PSC snapshot JSON lines (~12GB, ~10M records)

Output: ch_bulk.db with two tables:
  companies(company_number PK, company_name, status)
  psc(company_number, psc_name, psc_kind, natures_of_control,
      notified_on, ceased_on, dob_year, dob_month, registration_number)

Total load time: ~10-20 minutes depending on disk speed.
"""

import os
import sys
import csv
import json
import time
import sqlite3
import argparse

from config import CH_BULK_DB_PATH, CH_COMPANIES_CSV, CH_PSC_SNAPSHOT


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

def get_bulk_conn():
    conn = sqlite3.connect(CH_BULK_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-65536")   # 64 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def init_schema(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            company_number  TEXT PRIMARY KEY,
            company_name    TEXT,
            status          TEXT
        );

        CREATE TABLE IF NOT EXISTS psc (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number      TEXT    NOT NULL,
            psc_name            TEXT,
            psc_kind            TEXT,
            natures_of_control  TEXT,
            notified_on         TEXT,
            ceased_on           TEXT,
            dob_year            INTEGER,
            dob_month           INTEGER,
            registration_number TEXT
        );

        -- Indexes for fast lookups used by ch_bulk.py
        CREATE INDEX IF NOT EXISTS idx_psc_company   ON psc(company_number);
        CREATE INDEX IF NOT EXISTS idx_psc_dob       ON psc(dob_year, dob_month);
        CREATE INDEX IF NOT EXISTS idx_psc_regnum    ON psc(registration_number);
        CREATE INDEX IF NOT EXISTS idx_psc_kind      ON psc(psc_kind);
        CREATE INDEX IF NOT EXISTS idx_co_status     ON companies(status);
    """)
    conn.commit()
    print("[BULK] Schema initialised.")


# ---------------------------------------------------------------------------
# Load companies CSV
# ---------------------------------------------------------------------------

def _col(row, *names):
    """Read a column, trying each candidate name (handles leading-space headers)."""
    for n in names:
        v = row.get(n) or row.get(n.strip()) or row.get(" " + n.strip())
        if v is not None:
            return v.strip()
    return ""


def load_companies(conn):
    if not os.path.exists(CH_COMPANIES_CSV):
        print(f"[BULK] WARNING: Companies CSV not found — {CH_COMPANIES_CSV}")
        print("[BULK]   Skipping companies table. Company status lookups will be unavailable.")
        return

    print(f"\n[BULK] Loading companies: {CH_COMPANIES_CSV}")
    t0    = time.time()
    count = 0
    batch = []
    BATCH = 20_000

    with open(CH_COMPANIES_CSV, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        # Strip leading/trailing spaces from all header names
        reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]

        for row in reader:
            cn     = (row.get("CompanyNumber") or "").strip().upper()
            name   = (row.get("CompanyName") or "").strip()
            status = (row.get("CompanyStatus") or "").strip()

            if not cn:
                continue

            # Zero-pad numeric company numbers to 8 digits
            if cn.isdigit():
                cn = cn.zfill(8)

            batch.append((cn, name, status))

            if len(batch) >= BATCH:
                conn.executemany(
                    "INSERT OR REPLACE INTO companies VALUES (?,?,?)", batch
                )
                conn.commit()
                count += len(batch)
                batch = []
                if count % 500_000 == 0:
                    print(f"[BULK]   {count:,} companies  ({time.time()-t0:.0f}s)")

    if batch:
        conn.executemany("INSERT OR REPLACE INTO companies VALUES (?,?,?)", batch)
        conn.commit()
        count += len(batch)

    elapsed = time.time() - t0
    print(f"[BULK] Companies: {count:,} rows loaded in {elapsed:.0f}s")


# ---------------------------------------------------------------------------
# Load PSC snapshot (JSON lines)
# ---------------------------------------------------------------------------

PSC_INSERT = """
    INSERT INTO psc
        (company_number, psc_name, psc_kind, natures_of_control,
         notified_on, ceased_on, dob_year, dob_month, registration_number)
    VALUES (?,?,?,?,?,?,?,?,?)
"""


def _stream_psc_lines(path):
    """
    Yield raw lines from path whether it is a single file or a directory
    of JSON-lines files.
    """
    if os.path.isdir(path):
        for fname in sorted(os.listdir(path)):
            fpath = os.path.join(path, fname)
            if os.path.isfile(fpath):
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    yield from f
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            yield from f


def load_psc(conn):
    if not os.path.exists(CH_PSC_SNAPSHOT):
        print(f"[BULK] WARNING: PSC snapshot not found — {CH_PSC_SNAPSHOT}")
        print("[BULK]   Skipping PSC table. Bulk director matching will be unavailable.")
        return

    print(f"\n[BULK] Loading PSC snapshot: {CH_PSC_SNAPSHOT}")
    print("[BULK]   This typically takes 10-20 minutes for the full 12GB file.")

    t0     = time.time()
    count  = 0
    errors = 0
    batch  = []
    BATCH  = 20_000

    for line in _stream_psc_lines(CH_PSC_SNAPSHOT):
        line = line.strip()
        if not line:
            continue
        try:
            obj  = json.loads(line)
            cn   = obj.get("company_number", "").strip().upper()
            data = obj.get("data") or {}
            if not cn or not data:
                continue

            # Normalise company number
            if cn.isdigit():
                cn = cn.zfill(8)

            kind      = data.get("kind", "")
            name      = data.get("name", "")
            noc       = json.dumps(data.get("natures_of_control") or [])
            notified  = data.get("notified_on", "")
            ceased    = data.get("ceased_on", "") or None

            dob       = data.get("date_of_birth") or {}
            dob_year  = dob.get("year")
            dob_month = dob.get("month")

            ident  = data.get("identification") or {}
            reg_no = (ident.get("registration_number") or "").strip().upper() or None
            if reg_no and reg_no.isdigit():
                reg_no = reg_no.zfill(8)

            batch.append((cn, name, kind, noc, notified, ceased,
                          dob_year, dob_month, reg_no))

            if len(batch) >= BATCH:
                conn.executemany(PSC_INSERT, batch)
                conn.commit()
                count += len(batch)
                batch = []
                if count % 1_000_000 == 0:
                    rate = count / (time.time() - t0)
                    print(f"[BULK]   {count:,} PSC records  ({time.time()-t0:.0f}s, {rate:,.0f}/s)")

        except (json.JSONDecodeError, KeyError, TypeError):
            errors += 1
            continue

    if batch:
        conn.executemany(PSC_INSERT, batch)
        conn.commit()
        count += len(batch)

    elapsed = time.time() - t0
    print(f"[BULK] PSC: {count:,} records loaded in {elapsed:.0f}s  ({errors} parse errors ignored)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--psc-only", action="store_true",
                        help="Load PSC snapshot only (skip companies CSV)")
    args = parser.parse_args()

    if os.path.exists(CH_BULK_DB_PATH) and not args.psc_only:
        size_mb = os.path.getsize(CH_BULK_DB_PATH) / 1_048_576
        ans = input(
            f"\n[BULK] {CH_BULK_DB_PATH} already exists ({size_mb:.0f} MB).\n"
            f"       Rebuild from scratch? [y/N]: "
        ).strip().lower()
        if ans != "y":
            print("[BULK] Aborted — existing database kept.")
            sys.exit(0)
        os.remove(CH_BULK_DB_PATH)
        print(f"[BULK] Deleted old {CH_BULK_DB_PATH}.")

    print("\n" + "="*60)
    print("CH BULK DATA LOADER")
    print("="*60)

    conn = get_bulk_conn()
    init_schema(conn)
    if not args.psc_only:
        load_companies(conn)
    load_psc(conn)

    # Final stats
    co_count  = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    psc_count = conn.execute("SELECT COUNT(*) FROM psc").fetchone()[0]
    ind_count = conn.execute(
        "SELECT COUNT(*) FROM psc WHERE psc_kind LIKE '%individual%'"
    ).fetchone()[0]
    corp_count = conn.execute(
        "SELECT COUNT(*) FROM psc WHERE psc_kind LIKE '%corporate%'"
    ).fetchone()[0]

    print(f"\n{'='*60}")
    print(f"LOAD COMPLETE")
    print(f"  Companies:        {co_count:>10,}")
    print(f"  PSC records:      {psc_count:>10,}")
    print(f"    Individual PSC: {ind_count:>10,}")
    print(f"    Corporate PSC:  {corp_count:>10,}")
    db_mb = os.path.getsize(CH_BULK_DB_PATH) / 1_048_576
    print(f"  DB size:          {db_mb:>9.0f} MB")
    print(f"{'='*60}")
    print(f"\nReady. Run:  python run.py --all")

    conn.close()


if __name__ == "__main__":
    main()
