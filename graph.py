#!/usr/bin/env python3
"""
graph.py — Interactive asset network visualisation

Reads asset_discovery.db → outputs output/graph.html

Usage:
  python graph.py              # generate graph
  python graph.py --open       # generate and open in browser
  python graph.py --output /path/to/file.html

Node colour:
  Green        = Active
  Grey         = Dissolved
  Red          = Liquidation / Insolvency
  Orange       = Administration / Receivership
  Yellow       = Dormant / CVA
  Steel blue   = Unknown / not yet fetched

Node size:    proportional to registered property value (£)
Node border:  thick (4px) = LR property registered; thin = no property
Node shape:   ellipse = person (target individual); box = company

Edge solid dark  = person → company  (director or PSC, active)
Edge solid purple= person → company  (PSC-only link, active)
Edge dashed blue = company → company (corporate PSC chain, active)
Edge faded       = resigned / ceased
"""

import os
import sys
import json
import math
import argparse
import webbrowser
from collections import defaultdict

try:
    from pyvis.network import Network
except ImportError:
    print("[ERROR] pip install pyvis")
    sys.exit(1)

from config import DIRECTORS
from database import get_conn
from report import _director_label


# ── Colour palette ──────────────────────────────────────────────────────────

STATUS_BG = {
    "active":                "#27ae60",
    "dissolved":             "#7f8c8d",
    "liquidation":           "#c0392b",
    "insolvency-proceedings":"#c0392b",
    "administration":        "#d35400",
    "receivership":          "#d35400",
    "voluntary-arrangement": "#f39c12",
    "dormant":               "#f1c40f",
    "converted-closed":      "#95a5a6",
}
STATUS_BG_DEFAULT = "#2980b9"

PERSON_BG     = "#2c3e50"
EDGE_DIRECTOR = "#2c3e50"
EDGE_PSC      = "#8e44ad"
EDGE_CORP     = "#2980b9"
EDGE_FADED    = "#bdc3c7"

LEGEND_HTML = """
<div id="legend" style="
  position:fixed; bottom:18px; left:18px; z-index:9999;
  background:rgba(255,255,255,0.94); border:1px solid #ccc;
  border-radius:8px; padding:14px 18px; font-family:monospace;
  font-size:12px; line-height:1.8; box-shadow:0 2px 8px rgba(0,0,0,.2);">
  <b style="font-size:13px">Node colour</b><br>
  <span style="color:#27ae60">&#9632;</span> Active &nbsp;
  <span style="color:#7f8c8d">&#9632;</span> Dissolved &nbsp;
  <span style="color:#c0392b">&#9632;</span> Liquidation/Insolvency &nbsp;
  <span style="color:#d35400">&#9632;</span> Administration<br>
  <span style="color:#f39c12">&#9632;</span> CVA &nbsp;
  <span style="color:#f1c40f">&#9632;</span> Dormant &nbsp;
  <span style="color:#2980b9">&#9632;</span> Unknown &nbsp;
  <span style="color:#2c3e50">&#9632;</span> Person (target individual)<br>
  <b>Node border</b>: thick = LR property registered &nbsp;|&nbsp;
  <b>Node size</b>: ∝ property value<br>
  <b>Edge ─ solid dark</b>: person→company (director/PSC, active)<br>
  <b>Edge ─ dashed blue</b>: company→company (corporate PSC chain)<br>
  <b>Edge ─ faded</b>: resigned / ceased
</div>
"""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _fmt_cn(cn):
    if not cn:
        return ""
    cn = cn.strip().upper()
    return cn.zfill(8) if cn.isdigit() else cn


def _status_color(status):
    return STATUS_BG.get((status or "").lower().strip(), STATUS_BG_DEFAULT)


def _node_size(value, lo=20, hi=80):
    if not value or value <= 0:
        return lo
    # Log scale: £100k → ~25, £500k → ~40, £1M → ~50, £5M → ~70
    s = math.log1p(value / 100_000) * 14 + lo
    return round(min(hi, max(lo, s)))


def _ownership_label(noc_json):
    try:
        noc = json.loads(noc_json) if isinstance(noc_json, str) else (noc_json or [])
        for item in (noc if isinstance(noc, list) else []):
            s = str(item)
            if "75-to-100" in s: return "75-100%"
            if "50-to-75"  in s: return "50-75%"
            if "25-to-50"  in s: return "25-50%"
    except Exception:
        pass
    return ""


def _dir_surname_tokens(director):
    name = director["name"]
    surname = name.split(",")[0].strip().upper()
    tokens = {surname}
    if "-" in surname:
        for part in surname.split("-"):
            if part:
                tokens.add(part)
    return tokens


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data():
    conn = get_conn()

    # Property value and count per company
    prop_value    = defaultdict(float)
    prop_count    = defaultdict(int)
    prop_titles   = defaultdict(list)   # short address list for tooltip

    for row in conn.execute(
        "SELECT owner_company_number, price_paid, property_address FROM properties"
    ).fetchall():
        cn = _fmt_cn(row["owner_company_number"] or "")
        if not cn:
            continue
        try:
            v = float((row["price_paid"] or "0").replace(",", "").replace("£", ""))
        except Exception:
            v = 0.0
        prop_value[cn]  += v
        prop_count[cn]  += 1
        if row["property_address"]:
            prop_titles[cn].append(row["property_address"])

    # Outstanding charges per company
    charge_count = defaultdict(int)
    for row in conn.execute("""
        SELECT company_number, COUNT(*) as n FROM charges
        WHERE LOWER(charge_status) = 'outstanding'
        GROUP BY company_number
    """).fetchall():
        charge_count[_fmt_cn(row["company_number"])] = row["n"]

    # All charges (any status) per company for tooltip
    all_charges = defaultdict(list)
    for row in conn.execute("""
        SELECT company_number, charge_holder, charge_status, charge_created
        FROM charges ORDER BY charge_created DESC
    """).fetchall():
        all_charges[_fmt_cn(row["company_number"])].append(
            (row["charge_holder"] or "", row["charge_status"] or "",
             row["charge_created"] or "")
        )

    # Company master data
    companies = {}
    for row in conn.execute("SELECT * FROM companies").fetchall():
        cn = _fmt_cn(row["company_number"])
        companies[cn] = {
            "name":   row["company_name"] or cn,
            "status": (row["status"] or "").lower(),
            "type":   row["company_type"] or "",
            "reg_office": row["registered_office"] or "",
        }

    # Supplement with director_companies (may have companies not yet in companies table)
    for row in conn.execute(
        "SELECT DISTINCT company_number, company_name FROM director_companies WHERE company_number IS NOT NULL"
    ).fetchall():
        cn = _fmt_cn(row["company_number"])
        if cn not in companies:
            companies[cn] = {
                "name":   row["company_name"] or cn,
                "status": "",
                "type":   "",
                "reg_office": "",
            }

    # Director → company edges
    person_links = []  # (director_name, cn, role, resigned_on)
    for row in conn.execute(
        "SELECT director_name, company_number, role, resigned_on FROM director_companies WHERE company_number IS NOT NULL"
    ).fetchall():
        person_links.append((
            row["director_name"],
            _fmt_cn(row["company_number"]),
            row["role"] or "director",
            row["resigned_on"],
        ))

    # Individual PSC → company edges
    psc_links = []  # (director_label, cn, ownership, ceased_on)
    dir_tokens = {_director_label(d): _dir_surname_tokens(d) for d in DIRECTORS}
    dir_name_to_label = {d["name"]: _director_label(d) for d in DIRECTORS}

    for row in conn.execute("""
        SELECT company_number, psc_name, natures_of_control, ceased_on
        FROM psc WHERE psc_kind LIKE '%individual%'
    """).fetchall():
        psc_upper = (row["psc_name"] or "").upper()
        cn = _fmt_cn(row["company_number"])
        for label, tokens in dir_tokens.items():
            if any(t in psc_upper for t in tokens if len(t) >= 3):
                psc_links.append((
                    label,
                    cn,
                    _ownership_label(row["natures_of_control"]),
                    row["ceased_on"],
                ))
                break

    # Corporate PSC edges (company → company)
    corp_links = []  # (controlling_cn, controlled_cn, ownership, ceased_on)
    for row in conn.execute("""
        SELECT company_number, registration_number, psc_name,
               natures_of_control, ceased_on
        FROM psc
        WHERE psc_kind LIKE '%corporate%'
          AND registration_number IS NOT NULL AND registration_number != ''
    """).fetchall():
        ctrl  = _fmt_cn(row["registration_number"])
        ctrd  = _fmt_cn(row["company_number"])
        if not ctrl or not ctrd:
            continue
        corp_links.append((
            ctrl, ctrd,
            row["psc_name"] or ctrl,
            _ownership_label(row["natures_of_control"]),
            row["ceased_on"],
        ))
        # Ensure controlling company is in the node set
        if ctrl not in companies:
            companies[ctrl] = {"name": row["psc_name"] or ctrl,
                               "status": "", "type": "", "reg_office": ""}

    conn.close()

    return {
        "companies":    companies,
        "prop_value":   prop_value,
        "prop_count":   prop_count,
        "prop_titles":  prop_titles,
        "charge_count": charge_count,
        "all_charges":  all_charges,
        "person_links": person_links,
        "psc_links":    psc_links,
        "corp_links":   corp_links,
        "dir_name_to_label": dir_name_to_label,
    }


# ── Graph construction ────────────────────────────────────────────────────────

def build_network(data):
    net = Network(
        height="920px",
        width="100%",
        directed=True,
        bgcolor="#f8f9fa",
        font_color="#2c3e50",
        notebook=False,
        select_menu=False,
        filter_menu=False,
    )

    companies    = data["companies"]
    prop_value   = data["prop_value"]
    prop_count   = data["prop_count"]
    prop_titles  = data["prop_titles"]
    charge_count = data["charge_count"]
    all_charges  = data["all_charges"]
    dir_name_to_label = data["dir_name_to_label"]

    # ── Person nodes ────────────────────────────────────────────────────────
    for d in DIRECTORS:
        label = _director_label(d)
        net.add_node(
            label,
            label=label,
            title=f"<b>{d['name']}</b><br>DOB: {d['dob_year']}-{d['dob_month']:02d}",
            color={"background": PERSON_BG, "border": "#1a252f",
                   "highlight": {"background": "#34495e", "border": "#1a252f"}},
            shape="ellipse",
            size=45,
            borderWidth=3,
            font={"color": "#ffffff", "size": 16, "bold": True},
            mass=4,
        )

    # ── Company nodes ────────────────────────────────────────────────────────
    for cn, info in companies.items():
        value   = prop_value.get(cn, 0)
        n_prop  = prop_count.get(cn, 0)
        n_chrg  = charge_count.get(cn, 0)
        status  = info["status"]
        bg      = _status_color(status)
        size    = _node_size(value)
        has_prop = n_prop > 0

        # Truncate display label
        name_short = info["name"]
        if len(name_short) > 32:
            name_short = name_short[:30] + "…"
        display = f"{name_short}\n{cn}"

        # Rich tooltip
        addr_lines = prop_titles.get(cn, [])
        addr_html  = "".join(f"<br>&nbsp;• {a}" for a in addr_lines[:6])
        if len(addr_lines) > 6:
            addr_html += f"<br>&nbsp;… +{len(addr_lines)-6} more"

        charge_rows = all_charges.get(cn, [])[:4]
        chrg_html   = "".join(
            f"<br>&nbsp;⚖ {h} [{s}] {dt}" for h, s, dt in charge_rows
        )

        tip = (
            f"<b>{info['name']}</b><br>"
            f"<code>{cn}</code> &nbsp; {status.title() or 'Unknown'}<br>"
        )
        if info["reg_office"]:
            tip += f"{info['reg_office']}<br>"
        if n_prop:
            tip += f"<br><b>Properties: {n_prop}</b>"
            if value:
                tip += f" &nbsp; (£{value:,.0f} paid)"
            tip += addr_html
        if n_chrg:
            tip += f"<br><b>Outstanding charges: {n_chrg}</b>"
            tip += chrg_html

        net.add_node(
            cn,
            label=display,
            title=tip,
            color={"background": bg, "border": "#2c3e50" if has_prop else "#95a5a6",
                   "highlight": {"background": bg, "border": "#e74c3c"}},
            shape="box",
            size=size,
            borderWidth=5 if has_prop else 1,
            borderWidthSelected=7,
            mass=2 if has_prop else 1,
        )

    # ── Person → company edges (appointments) ────────────────────────────────
    seen_dir_edges: dict = {}   # (label, cn) → edge attrs to merge

    for (dname, cn, role, resigned) in data["person_links"]:
        label = dir_name_to_label.get(dname)
        if not label or cn not in companies:
            continue
        active = not resigned
        key    = (label, cn)

        # Determine display role
        base_roles = {"director", "secretary", "indirect-psc-chain",
                      "network-expansion", "bulk-direct"}
        short_role = "D" if any(r in role for r in ("director",)) else role[:12]

        if key not in seen_dir_edges:
            seen_dir_edges[key] = {
                "roles": set(),
                "active": active,
                "resigned": resigned,
            }
        seen_dir_edges[key]["roles"].add(short_role)
        if active:
            seen_dir_edges[key]["active"] = True

    # Collect PSC links per (label, cn) to merge
    seen_psc: dict = {}  # (label, cn) → (ownership, active)
    for (label, cn, ownership, ceased) in data["psc_links"]:
        if cn not in companies:
            continue
        key    = (label, cn)
        active = not ceased
        if key not in seen_psc or (active and not seen_psc[key][1]):
            seen_psc[key] = (ownership, active)

    # Emit merged person → company edges
    all_person_keys = set(seen_dir_edges.keys()) | set(seen_psc.keys())
    for key in all_person_keys:
        label, cn = key
        is_dir  = key in seen_dir_edges
        is_psc  = key in seen_psc

        d_active = seen_dir_edges[key]["active"]  if is_dir else False
        p_active = seen_psc[key][1]               if is_psc else False
        active   = d_active or p_active

        parts = []
        if is_dir:
            roles = seen_dir_edges[key]["roles"] - {"director"}
            parts.append("D")
            if roles:
                parts.append("/".join(sorted(roles)))
        if is_psc:
            own = seen_psc[key][0]
            parts.append(f"PSC{' '+own if own else ''}")

        edge_label = " + ".join(parts)

        net.add_edge(
            label, cn,
            label=edge_label,
            title=f"{'Active' if active else 'Resigned'}: {edge_label}",
            color={"color": EDGE_DIRECTOR if active else EDGE_FADED,
                   "opacity": 1.0 if active else 0.35},
            width=3 if active else 1,
            dashes=not active,
        )

    # ── Corporate PSC edges (company → company) ──────────────────────────────
    seen_corp: set = set()
    for (ctrl, ctrd, psc_name, ownership, ceased) in data["corp_links"]:
        if ctrl not in companies or ctrd not in companies:
            continue
        key    = (ctrl, ctrd)
        if key in seen_corp:
            continue
        seen_corp.add(key)
        active = not ceased
        lbl    = ownership or ""

        net.add_edge(
            ctrl, ctrd,
            label=lbl,
            title=f"<b>{psc_name}</b> is corporate PSC of <code>{ctrd}</code>"
                  f"<br>Ownership: {ownership or '?'}"
                  f"<br>{'Active' if active else 'Ceased'}",
            color={"color": EDGE_CORP if active else EDGE_FADED,
                   "opacity": 0.85 if active else 0.3},
            width=2 if active else 1,
            dashes=True,
        )

    # ── Physics & interaction options ────────────────────────────────────────
    net.set_options("""
    {
      "physics": {
        "enabled": true,
        "forceAtlas2Based": {
          "gravitationalConstant": -60,
          "centralGravity": 0.008,
          "springLength": 180,
          "springConstant": 0.05,
          "damping": 0.45,
          "avoidOverlap": 0.5
        },
        "solver": "forceAtlas2Based",
        "stabilization": {
          "enabled": true,
          "iterations": 250,
          "updateInterval": 50
        }
      },
      "edges": {
        "smooth": {"enabled": true, "type": "dynamic", "roundness": 0.3},
        "font":   {"size": 10, "align": "middle", "strokeWidth": 0},
        "arrows": {"to": {"enabled": true, "scaleFactor": 0.6}}
      },
      "nodes": {
        "font":    {"size": 12, "face": "monospace"},
        "scaling": {"min": 15, "max": 80}
      },
      "interaction": {
        "hover":              true,
        "tooltipDelay":       80,
        "navigationButtons":  true,
        "keyboard":           {"enabled": true},
        "multiselect":        true,
        "hideEdgesOnDrag":    false
      },
      "layout": {
        "improvedLayout": true
      }
    }
    """)

    return net


# ── Inject legend into saved HTML ────────────────────────────────────────────

def _inject_legend(html_path):
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    html = html.replace("</body>", LEGEND_HTML + "\n</body>")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)


# ── Summary ──────────────────────────────────────────────────────────────────

def print_summary(data):
    companies   = data["companies"]
    prop_count  = data["prop_count"]
    prop_value  = data["prop_value"]
    charge_count = data["charge_count"]
    corp_links  = data["corp_links"]

    active   = sum(1 for c in companies.values() if c["status"] == "active")
    dissolved= sum(1 for c in companies.values() if c["status"] == "dissolved")
    liq      = sum(1 for c in companies.values()
                   if c["status"] in ("liquidation", "insolvency-proceedings"))
    with_prop = len(prop_count)
    total_val = sum(prop_value.values())
    total_chrg = sum(charge_count.values())

    print(f"\n{'='*60}")
    print(f"  GRAPH SUMMARY")
    print(f"{'='*60}")
    print(f"  Companies:     {len(companies):>4}  "
          f"(active:{active}  dissolved:{dissolved}  liquidation:{liq})")
    print(f"  With property: {with_prop:>4}  "
          f"(£{total_val:,.0f} total price paid)")
    print(f"  Charges:       {total_chrg:>4}  outstanding across all companies")
    print(f"  Corp PSC edges:{len(set((c,d) for c,d,*_ in corp_links)):>4}")
    print(f"{'='*60}\n")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate asset network graph")
    parser.add_argument("--output", default="output/graph.html",
                        help="Output HTML path")
    parser.add_argument("--open", action="store_true",
                        help="Open in browser after generating")
    args = parser.parse_args()

    os.makedirs("output", exist_ok=True)

    print("[GRAPH] Loading data from asset_discovery.db ...")
    data = load_data()
    print_summary(data)

    print("[GRAPH] Building network ...")
    net = build_network(data)

    print(f"[GRAPH] Saving → {args.output}")
    net.save_graph(args.output)
    _inject_legend(args.output)

    n_nodes = len(net.nodes)
    n_edges = len(net.edges)
    print(f"[GRAPH] Done.  {n_nodes} nodes  |  {n_edges} edges")
    print(f"[GRAPH] Open:  {os.path.abspath(args.output)}")

    if args.open:
        webbrowser.open(f"file://{os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
