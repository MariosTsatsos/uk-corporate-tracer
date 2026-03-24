#!/usr/bin/env python3
"""
ASSET DISCOVERY PIPELINE
=========================
Pipeline order:
  Step 1    CH API  — direct director appointments for all configured directors
  Step 2    CH API  — details, PSC, officers for all list-1 companies
  Step 2b   Bulk DB — downward PSC ownership chain (gen 1/2/3, active, ≥25%)
  Step 4    Bulk DB — additional DOB-matched PSC companies + corporate chains
  Step 2    CH API  — incremental details for any new bulk-found companies
  Step 3    CH API  — charges register for all companies
  Step 5    Local   — CCOD/OCOD Land Registry scan → properties

Usage:
  python run.py --all        # Full pipeline
  python run.py --no-bulk    # Skip bulk expansion (steps 2b + 4)
  python run.py --step 1     # Single step (1/2/3/5/bulk)
  python run.py --report     # Rebuild CSV from existing DB (no API)
  python run.py --summary    # Print DB summary only
  python run.py --status     # Show progress of latest run

Prerequisites:
  python load_ch_bulk.py     # Build ch_bulk.db once (~15 mins)
"""

import sys
import os
import argparse
from datetime import datetime

from config import DIRECTORS
from database import init_db
from companies_house import step1_director_companies, step2_expand_companies, step2b_follow_corporate_pscs, step2c_recursive_network_expansion, step3_get_charges
from ch_bulk import step_bulk_expansion
from land_registry import step5_search_land_registry
from report import export_master_csv, export_hidden_companies_csv, print_summary
from logger import RunLogger, show_status


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all",     action="store_true")
    parser.add_argument("--step",    type=str)   # accepts int or "bulk"
    parser.add_argument("--report",  action="store_true")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--status",  action="store_true",
                        help="Show status of the latest run and exit")
    parser.add_argument("--no-bulk", action="store_true",
                        help="Skip bulk expansion step")
    args = parser.parse_args()

    # --status: show log summary without running anything
    if args.status:
        show_status()
        return

    rl = RunLogger()

    print("="*60)
    print("ASSET DISCOVERY PIPELINE")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # Delete old DB if schema has changed (run once after upgrading)
    init_db()

    if DIRECTORS[0]["name"] in ("JOHN WILLIAM SMITH", "FULL NAME HERE"):
        print("\n[ERROR] Edit config.py with real director names.")
        sys.exit(1)

    print(f"\nTargets:")
    for d in DIRECTORS:
        from report import _director_label
        aliases = d.get("aliases", [])
        print(f"  [{_director_label(d)}] {d['name']}  +{len(aliases)} alias(es)")

    rl.db_snapshot("run start")

    if args.summary:
        print_summary()
        rl.close()
        return

    if args.report:
        print_summary()
        export_master_csv()
        export_hidden_companies_csv()
        rl.close()
        return

    if args.step:
        rl.step(f"step {args.step}")
        run_step(args.step, DIRECTORS)
        rl.done(f"step {args.step}")
        print_summary()
        export_master_csv()
        rl.close()
        return

    # Default: CH steps 1-4 (API + bulk)
    rl.step("Step 1 — director appointments")
    step1_director_companies(DIRECTORS)
    rl.done("Step 1 — director appointments")

    rl.step("Step 2 — company details / PSC / officers")
    step2_expand_companies()
    rl.done("Step 2 — company details / PSC / officers")

    # Step 2b: downward ownership chain from list-1 companies
    # Finds companies controlled (≥25% PSC) by list-1, active only, gen 1→2→3
    rl.step("Step 2b — downward PSC ownership chain (gen 1/2/3)")
    step2b_follow_corporate_pscs(DIRECTORS)
    rl.done("Step 2b — downward PSC ownership chain (gen 1/2/3)")

    # Step 2c (recursive officer-network expansion) intentionally removed —
    # it follows co-officer relationships which expand uncontrollably into
    # thousands of unrelated companies.

    # Bulk expansion — requires ch_bulk.db (python load_ch_bulk.py)
    if not args.no_bulk:
        rl.step("Step 4 — bulk PSC expansion (offline, name+birth-year matched)")
        step_bulk_expansion(DIRECTORS)
        rl.done("Step 4 — bulk PSC expansion (offline, DOB-matched)")

        rl.step("Step 2 (incremental) — details for bulk-found companies")
        step2_expand_companies()
        rl.done("Step 2 (incremental) — details for bulk-found companies")

    rl.step("Step 3 — charges")
    step3_get_charges()
    rl.done("Step 3 — charges")

    if args.all:
        rl.step("Step 5 — Land Registry scan (CCOD/OCOD)")
        step5_search_land_registry(DIRECTORS)
        rl.done("Step 5 — Land Registry scan (CCOD/OCOD)")

    print_summary()
    export_master_csv()
    export_hidden_companies_csv()

    print("\n[DONE]")
    print("  Database: asset_discovery.db")
    print("  Report:   output/asset_register_*.csv")
    if not args.all:
        print("\n  Land Registry files found — run:  python3 run.py --all")
        print("  to search CCOD/OCOD and populate property data.")

    rl.close()


def run_step(n, directors):
    steps = {
        "1":    lambda: step1_director_companies(directors),
        "2":    step2_expand_companies,
        "3":    step3_get_charges,
        "5":    lambda: step5_search_land_registry(directors),
        "bulk": lambda: step_bulk_expansion(directors),
    }
    fn = steps.get(str(n))
    if not fn:
        print(f"[ERROR] Unknown step '{n}'. Valid: 1, 2, 3, 5, bulk")
        return
    fn()


if __name__ == "__main__":
    main()
