#!/usr/bin/env python3
"""
Diagnostic script — run this to see exactly what CH returns.
Usage: python debug.py
"""
import requests
import json
from config import CH_API_KEY, CH_BASE_URL, DIRECTORS

def raw_search(name):
    print(f"\n{'='*60}")
    print(f"RAW CH SEARCH: '{name}'")
    print('='*60)
    r = requests.get(
        f"{CH_BASE_URL}/search/officers",
        params={"q": name, "items_per_page": 10},
        auth=(CH_API_KEY, ""),
        timeout=15
    )
    print(f"Status: {r.status_code}")
    if r.status_code != 200:
        print(r.text)
        return

    data = r.json()
    items = data.get("items", [])
    print(f"Total results: {data.get('total_results', 0)}, showing first {len(items)}")

    for i, item in enumerate(items):
        print(f"\n  [{i+1}] {item.get('title', '?')}")
        print(f"       DOB field raw: {item.get('date_of_birth', 'NOT PRESENT')}")
        print(f"       address: {item.get('address', {})}")
        links = item.get("links", {})
        self_link = links.get("self", "")
        officer_id = self_link.split("/officers/")[-1].split("/")[0]
        print(f"       officer_id: {officer_id}")

if __name__ == "__main__":
    for d in DIRECTORS:
        all_names = [d["name"]] + d.get("aliases", [])
        for name in all_names:
            raw_search(name)
