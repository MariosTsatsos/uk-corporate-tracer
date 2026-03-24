#!/usr/bin/env python3
"""
ppd_scan.py — Scan Land Registry Price Paid Data for all addresses
associated with the discovered company network.

PPD format (no header, CSV with quotes):
  0: transaction_id
  1: price paid
  2: date of transfer (YYYY-MM-DD HH:MM)
  3: postcode
  4: property type  (D/S/T/F/O)
  5: old/new        (Y=new build, N=established)
  6: duration       (F=freehold, L=leasehold, U=unknown)
  7: PAON           (house number/name)
  8: SAON           (flat/unit number)
  9: street
  10: locality
  11: town/city
  12: district
  13: county
  14: PPD category  (A=standard, B=additional)
  15: record status (A=addition, C=change, D=delete)

Note: PPD contains NO buyer/seller names — we search by known postcodes
and street names derived from our own database and charge descriptions.
"""

import csv
import re
import sqlite3
from collections import defaultdict

from config import DB_PATH, path

PPD_PATH = path + "pp-complete.txt"

POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', re.IGNORECASE)
HOUSE_NUM_RE = re.compile(r'^\d+')


def norm_pc(pc: str) -> str:
    """Normalise postcode to 'AA9 9AA' format."""
    pc = pc.upper().strip().replace(" ", "")
    if len(pc) >= 5:
        return pc[:-3] + " " + pc[-3:]
    return ""


def _extract_leading_number(s: str):
    """Return the leading house number from an address string, or None."""
    m = HOUSE_NUM_RE.match(s.strip())
    return m.group(0) if m else None


def build_search_targets(conn) -> dict:
    """
    Build search targets from:
      1. Properties table (postcode + known house number)
      2. Postcodes embedded in charge descriptions
      3. Street-name targets for addresses without postcodes
      4. Manual additions (known from context)

    Returns:
      postcode_targets: dict[postcode → list[dict]]
      street_targets:   list[dict]  — searched in a second pass by street name
    """
    postcode_targets = defaultdict(list)
    street_targets = []   # [{street, paon, label, address}]

    # ── 1. Properties table ──────────────────────────────────────────────────
    rows = conn.execute("""
        SELECT postcode, property_address, owner_company, owner_company_number, title_number
        FROM properties
        WHERE postcode IS NOT NULL AND postcode != ''
    """).fetchall()
    for pc, addr, company, cn, title in rows:
        npc = norm_pc(pc)
        if npc:
            postcode_targets[npc].append({
                "label":      f"{company} ({cn}) / {title}",
                "address":    addr,
                "paon_hint":  _extract_leading_number(addr),
                "source":     "properties_table",
            })

    # ── 2. Postcodes in charge descriptions ──────────────────────────────────
    rows = conn.execute("""
        SELECT DISTINCT dc.company_name, c.company_number, c.charge_description
        FROM charges c
        JOIN director_companies dc ON dc.company_number = c.company_number
        WHERE c.charge_description IS NOT NULL AND c.charge_description != ''
        GROUP BY c.company_number, c.charge_description
    """).fetchall()
    seen_charge_pcs = set()
    for company, cn, desc in rows:
        for m in POSTCODE_RE.finditer(desc.upper()):
            npc = norm_pc(m.group(1))
            key = (npc, cn)
            if npc and key not in seen_charge_pcs:
                seen_charge_pcs.add(key)
                postcode_targets[npc].append({
                    "label":     f"{company} ({cn}) / charge",
                    "address":   desc[:100].strip(),
                    "paon_hint": None,
                    "source":    "charge_description",
                })

    # ── 3. Manual additions (addresses without postcodes in charges) ─────────
    # These require a street-name search in a second PPD pass.
    # Add entries here for properties where charges reference a street address
    # but no postcode is available in the database.
    street_targets = [
        # Example entries — replace with real addresses from your investigation:
        # {
        #     "street": "EXAMPLE STREET",
        #     "paon":   "42",
        #     "label":  "EXAMPLE COMPANY LIMITED (12345678) — outstanding charge",
        #     "address": "42 Example Street, London E1",
        # },
        # {
        #     "street": "SAMPLE ROAD",
        #     "paon":   None,
        #     "label":  "ANOTHER COMPANY LTD (87654321) — satisfied charge",
        #     "address": "Sample Road, Birmingham",
        # },
    ]

    return postcode_targets, street_targets


def scan_ppd(postcode_targets: dict, street_targets: list):
    """
    Single-pass scan of PPD file.
    Matches on postcode OR (street name + optional PAON) for street targets.
    """
    pc_matches   = defaultdict(list)   # postcode → [transaction dicts]
    st_matches   = defaultdict(list)   # street   → [transaction dicts]

    target_pcs   = set(postcode_targets.keys())
    street_index = {}  # street_upper → list of {paon, label, address}
    for st in street_targets:
        street_index.setdefault(st["street"].upper(), []).append(st)

    print(f"Scanning PPD: {PPD_PATH}")
    print(f"  Postcode targets : {len(target_pcs)}")
    print(f"  Street targets   : {len(street_index)}")
    print()

    count = 0
    pc_found = 0
    st_found = 0

    with open(PPD_PATH, "r", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        for row in reader:
            count += 1
            if count % 2_000_000 == 0:
                print(f"  {count // 1_000_000}M rows … {pc_found} postcode hits, {st_found} street hits")

            if len(row) < 14:
                continue

            pc     = norm_pc(row[3])
            street = row[9].upper().strip()
            paon   = row[7].strip()

            # Postcode match
            if pc in target_pcs:
                pc_found += 1
                pc_matches[pc].append(_make_txn(row))

            # Street match (separate, no postcode needed)
            if street in street_index:
                for st_def in street_index[street]:
                    if st_def["paon"] is None or paon == st_def["paon"]:
                        st_found += 1
                        st_matches[street].append(_make_txn(row))
                        break

    print(f"\nDone. Scanned {count:,} rows.")
    print(f"  Postcode matches : {pc_found}")
    print(f"  Street matches   : {st_found}")
    return pc_matches, st_matches


def _make_txn(row: list) -> dict:
    return {
        "txid":     row[0],
        "price":    row[1],
        "date":     row[2][:10] if row[2] else "",
        "ptype":    row[4],
        "new":      row[5],
        "tenure":   row[6],
        "paon":     row[7].strip(),
        "saon":     row[8].strip(),
        "street":   row[9].strip(),
        "locality": row[10].strip(),
        "town":     row[11].strip(),
        "postcode": norm_pc(row[3]),
        "status":   row[15].strip() if len(row) > 15 else "",
    }


PTYPE = {"D": "Detached", "S": "Semi-detached", "T": "Terraced",
         "F": "Flat/Maisonette", "O": "Other/Commercial"}
TENURE = {"F": "Freehold", "L": "Leasehold", "U": "Unknown"}


def _fmt_txn(t: dict) -> str:
    parts = [p for p in [t["saon"], t["paon"], t["street"], t["locality"], t["town"], t["postcode"]] if p]
    address = ", ".join(parts)
    price_str = f"£{int(t['price']):>12,}" if t["price"].isdigit() else f"{'n/a':>13}"
    tenure = TENURE.get(t["tenure"], t["tenure"])
    ptype  = PTYPE.get(t["ptype"], t["ptype"])
    flag = " ⚠ DELETED" if t["status"] == "D" else ""
    return f"  {t['date']}  {price_str}  {tenure:<10}  {ptype:<20}  {address}{flag}"


def print_report(postcode_targets, street_targets, pc_matches, st_matches):
    sep = "=" * 90

    print("\n" + sep)
    print("UK CORPORATE TRACER — Price Paid Data Scan")
    print(sep)

    # ── Postcode results ────────────────────────────────────────────────────
    print("\n── POSTCODE MATCHES ──────────────────────────────────────────────────────────\n")

    grand_total = 0
    for pc in sorted(pc_matches.keys()):
        txns = sorted(pc_matches[pc], key=lambda x: x["date"])
        labels = postcode_targets.get(pc, [])
        label_str = labels[0]["label"] if labels else ""
        addr_str  = labels[0]["address"] if labels else ""

        print(f"📍 {pc}  —  {label_str}")
        if addr_str:
            print(f"   {addr_str}")
        print(f"   {len(txns)} transaction(s):")
        for t in txns:
            print(_fmt_txn(t))
        print()
        grand_total += len(txns)

    # ── Street results ──────────────────────────────────────────────────────
    print("\n── STREET MATCHES (no-postcode addresses) ────────────────────────────────────\n")

    st_index = {s["street"].upper(): s for s in street_targets}

    for street in sorted(st_matches.keys()):
        txns = sorted(st_matches[street], key=lambda x: x["date"])
        st_def = st_index.get(street, {})
        print(f"📍 {street}  —  {st_def.get('label', '')}")
        if st_def.get("address"):
            print(f"   {st_def['address']}")
        print(f"   {len(txns)} transaction(s):")
        for t in txns:
            print(_fmt_txn(t))
        print()
        grand_total += len(txns)

    print(sep)
    print(f"TOTAL TRANSACTIONS FOUND: {grand_total}")
    print(sep)


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    postcode_targets, street_targets = build_search_targets(conn)
    pc_matches, st_matches = scan_ppd(postcode_targets, street_targets)
    print_report(postcode_targets, street_targets, pc_matches, st_matches)
