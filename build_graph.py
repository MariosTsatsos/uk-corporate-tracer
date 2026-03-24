"""
UK Corporate Tracer — Interactive Network Graph
"""

import sqlite3, re
from pyvis.network import Network
from config import DIRECTORS, NOMINEE_PSC_FRAGMENT

main = sqlite3.connect('asset_discovery.db')
bulk = sqlite3.connect('ch_bulk.db')
mc = main.cursor()
bc = bulk.cursor()

# ── Load all companies ──────────────────────────────────────────────────────
mc.execute("""
    SELECT DISTINCT c.company_number, c.company_name, c.status,
           c.incorporated_on, c.registered_office
    FROM companies c
    JOIN director_companies dc ON c.company_number = dc.company_number
""")
companies = {r[0]: {'name': r[1], 'status': r[2], 'inc': r[3], 'office': r[4]}
             for r in mc.fetchall()}

# ── Which companies hold CCOD property? ──────────────────────────────────
mc.execute("SELECT DISTINCT owner_company_number FROM properties WHERE owner_company_number IS NOT NULL")
asset_holders = {r[0] for r in mc.fetchall()}

# ── Build director node lookup from config ─────────────────────────────────
def _build_initials(d):
    name = d["name"]
    if "," in name:
        surname_part, forenames_part = name.split(",", 1)
        forename_tokens = forenames_part.strip().split()
        surname_tokens  = re.split(r"[-\s]+", surname_part.strip())
    else:
        tokens = re.split(r"[-\s]+", name.strip())
        forename_tokens = tokens[:-1]
        surname_tokens  = tokens[-1:]
    parts = forename_tokens + surname_tokens
    return "".join(p[0].upper() for p in parts if p)

# {(dob_year, dob_month): node_id}
director_dob_map   = {}
# {node_id: display_label}
director_label_map = {}

for _d in DIRECTORS:
    _initials = _build_initials(_d)
    _nid      = f"__{_initials}"
    _yr       = _d.get("dob_year")
    _mo       = _d.get("dob_month")
    if _yr and _mo:
        director_dob_map[(_yr, _mo)] = _nid
    director_label_map[_nid] = f"{_initials}\n{_d['name'].replace(', ', chr(10))}"

# ── Nominee PSC ────────────────────────────────────────────────────────────
nominee_psc = set()
if NOMINEE_PSC_FRAGMENT:
    mc.execute(
        "SELECT company_number FROM psc WHERE psc_name LIKE ? AND (ceased_on IS NULL OR ceased_on='')",
        (f"%{NOMINEE_PSC_FRAGMENT}%",)
    )
    for r in mc.fetchall(): nominee_psc.add(r[0])

# ── PSC handovers (directors left as PSC in recent years) ─────────────────
_like_clauses = " OR ".join(
    f"psc_name LIKE '%{d['name'].split(',')[0].strip()}%'" for d in DIRECTORS
)
recent_handover = set()
if _like_clauses:
    mc.execute(f"""SELECT DISTINCT company_number FROM psc
                   WHERE ceased_on >= '2024-01-01' AND ({_like_clauses})""")
    recent_handover = {{r[0] for r in mc.fetchall()}}

# ── Outstanding charges ────────────────────────────────────────────────────
mc.execute("SELECT company_number, COUNT(*) FROM charges WHERE charge_status='outstanding' GROUP BY company_number")
outstanding_charges = {r[0]: r[1] for r in mc.fetchall()}

# ── Edges ──────────────────────────────────────────────────────────────────
edges = []

# Active director links (configured directors only, matched by DOB)
mc.execute("""SELECT company_number, dob_year, dob_month FROM company_officers
              WHERE (resigned_on IS NULL OR resigned_on = '')""")
for cn, dy, dm in mc.fetchall():
    if cn not in companies: continue
    nid = director_dob_map.get((dy, dm))
    if nid:
        edges.append((nid, cn, 'director', True))

# All PSC links from configured directors (active + ceased for completeness)
mc.execute("SELECT company_number, dob_year, dob_month, ceased_on FROM psc WHERE psc_kind LIKE '%individual%'")
for cn, dy, dm, ceased in mc.fetchall():
    if cn not in companies: continue
    active = not ceased
    nid = director_dob_map.get((dy, dm))
    if nid:
        edges.append((nid, cn, 'PSC', active))

# Corporate PSC links
mc.execute("""SELECT company_number, registration_number, psc_name, ceased_on
              FROM psc WHERE (psc_kind LIKE '%corporate%' OR psc_kind LIKE '%legal%')
                AND registration_number IS NOT NULL AND registration_number != ''""")
for cn, reg, psc_name, ceased in mc.fetchall():
    if cn not in companies: continue
    active = not ceased
    if reg in companies:
        edges.append((reg, cn, 'corp-PSC', active))
    else:
        edges.append(('__EXT_' + reg, cn, 'corp-PSC (external)', active))

# ── Colour scheme ──────────────────────────────────────────────────────────
def node_colour(cn):
    status = companies[cn]['status']
    has_assets = cn in asset_holders
    if status == 'liquidation':
        return ('#ff4444', '#cc0000', 32)
    if status == 'dissolved':
        return ('#aaaaaa', '#777777', 22)
    if has_assets:
        return ('#f5c518', '#b8860b', 30)
    if cn in nominee_psc:
        return ('#ff88aa', '#cc3366', 24)
    if cn in recent_handover:
        return ('#ff9944', '#cc5500', 26)
    return ('#5599dd', '#2266aa', 22)

# ── Build network ──────────────────────────────────────────────────────────
net = Network(height='100vh', width='100%', bgcolor='#0d1117',
              font_color='white', directed=True, notebook=False)

# Individual director nodes (from config)
for nid, label in director_label_map.items():
    net.add_node(nid, label=label,
                 color={'background': '#22cc55', 'border': '#007722'},
                 size=42, shape='ellipse',
                 font={'size': 16, 'bold': True, 'color': '#ffffff'},
                 borderWidth=3)

# External corporate PSC nodes
mc.execute("""SELECT DISTINCT p.registration_number, p.psc_name
              FROM psc p WHERE (p.psc_kind LIKE '%corporate%' OR p.psc_kind LIKE '%legal%')
                AND p.registration_number IS NOT NULL AND p.registration_number != ''
                AND p.registration_number NOT IN (SELECT company_number FROM director_companies)""")
for reg, pname in mc.fetchall():
    nid  = '__EXT_' + reg
    lbl  = (pname[:22] + '\n[' + reg + ']\nexternal')
    net.add_node(nid, label=lbl,
                 color={'background': '#9966dd', 'border': '#5522aa'},
                 size=22, shape='box',
                 font={'size': 11, 'color': '#ffffff'},
                 title=f"External entity: {pname} [{reg}]",
                 borderWidth=2)

# Company nodes
for cn, info in companies.items():
    status     = info['status']
    has_assets = cn in asset_holders
    charges_n  = outstanding_charges.get(cn, 0)
    handover   = cn in recent_handover
    nominee    = cn in nominee_psc
    bg, border, size = node_colour(cn)

    # Tooltip HTML
    tips = [
        f"<b>{info['name']}</b>",
        f"No: {cn}",
        f"Status: {status.upper()}",
        f"Incorporated: {info['inc'] or '—'}",
        f"Office: {(info['office'] or '—')[:60]}",
    ]
    if has_assets:
        mc.execute("SELECT title_number, property_address, tenure FROM properties WHERE owner_company_number=?", (cn,))
        for p in mc.fetchall():
            tips.append(f"🏠 {p[0]} ({p[2]}): {p[1][:50]}")
    if charges_n:
        tips.append(f"⚠️  {charges_n} outstanding charge(s)")
    if handover:
        tips.append("⚡ Director(s) withdrew as PSC 2024–2025")
    if nominee:
        tips.append("🚨 Nominee PSC in place")
    if status == 'dissolved':
        tips.append("✝  Company dissolved")
    if status == 'liquidation':
        tips.append("❌ Company in CVL (liquidation)")

    # Label: name on two lines + icons
    name_parts = info['name'].split()
    mid = len(name_parts) // 2
    line1 = ' '.join(name_parts[:mid]) if mid else info['name'][:16]
    line2 = ' '.join(name_parts[mid:])
    # Trim lines
    line1 = line1[:22]
    line2 = line2[:22]
    label = line1 + '\n' + line2 if line2 else line1

    icons = ''
    if has_assets:                  icons += ' 🏠'
    if charges_n:                   icons += ' ⚠'
    if handover:                    icons += ' ⚡'
    if nominee:                     icons += ' 🚨'
    if status == 'liquidation':     icons += '\n❌ CVL'
    if status == 'dissolved':       icons += '\n✝'

    shape = 'box' if status in ('dissolved', 'liquidation') else 'dot'
    net.add_node(cn, label=label + icons,
                 color={'background': bg, 'border': border},
                 size=size, shape=shape,
                 font={'size': 12, 'color': '#ffffff'},
                 title='<br>'.join(tips),
                 borderWidth=3 if status == 'liquidation' else 2)

# Edges
seen_edges = set()
for src, dst, lbl, active in edges:
    key = (src, dst, lbl)
    if key in seen_edges: continue
    seen_edges.add(key)
    if src not in companies and not src.startswith('__'): continue

    if lbl == 'director':
        color, width = ('#33ff88' if active else '#335544'), 2
    elif lbl == 'PSC':
        color, width = ('#ffdd00' if active else '#665500'), 2
    elif 'corp-PSC' in lbl:
        color, width = ('#ff8800' if active else '#553300'), 2
    else:
        color, width = ('#aaaaaa', 1)

    net.add_edge(src, dst,
                 label='' if active else lbl,
                 color={'color': color, 'opacity': 0.85 if active else 0.45},
                 width=width,
                 dashes=not active,
                 arrows='to',
                 title=f"{lbl} ({'active' if active else 'ceased'})")

# ── Physics: strong repulsion, long springs ───────────────────────────────
net.set_options("""
{
  "nodes": {
    "font": {"size": 12, "face": "Arial", "strokeWidth": 2, "strokeColor": "#000000"},
    "shadow": {"enabled": true, "size": 6, "x": 2, "y": 2}
  },
  "edges": {
    "font": {"size": 10, "align": "middle", "strokeWidth": 2, "strokeColor": "#000000"},
    "smooth": {"type": "dynamic"},
    "shadow": false
  },
  "interaction": {
    "hover": true,
    "tooltipDelay": 80,
    "navigationButtons": true,
    "keyboard": {"enabled": true},
    "zoomView": true,
    "dragView": true
  },
  "physics": {
    "solver": "barnesHut",
    "barnesHut": {
      "gravitationalConstant": -22000,
      "centralGravity": 0.05,
      "springLength": 280,
      "springConstant": 0.02,
      "damping": 0.12,
      "avoidOverlap": 0.9
    },
    "stabilization": {
      "enabled": true,
      "iterations": 400,
      "updateInterval": 25
    }
  }
}
""")

# ── Save and post-process HTML to inject proper legend ────────────────────
out = 'output/corporate_network.html'
net.save_graph(out)

LEGEND_HTML = """
<style>
  #network-legend {
    position: fixed;
    top: 18px;
    right: 18px;
    z-index: 9999;
    background: rgba(10, 14, 26, 0.93);
    border: 1.5px solid #445577;
    border-radius: 10px;
    padding: 16px 20px 14px 20px;
    min-width: 230px;
    font-family: Arial, sans-serif;
    font-size: 13px;
    color: #e8e8f0;
    box-shadow: 0 4px 24px rgba(0,0,0,0.7);
    line-height: 1.7;
  }
  #network-legend h3 {
    margin: 0 0 10px 0;
    font-size: 14px;
    letter-spacing: 1px;
    color: #aaccff;
    border-bottom: 1px solid #334466;
    padding-bottom: 7px;
    text-transform: uppercase;
  }
  #network-legend .leg-section {
    margin-top: 9px;
    margin-bottom: 2px;
    font-size: 11px;
    color: #8899bb;
    letter-spacing: 0.5px;
    text-transform: uppercase;
  }
  #network-legend .leg-row {
    display: flex;
    align-items: center;
    gap: 9px;
    margin: 3px 0;
  }
  #network-legend .swatch {
    width: 18px;
    height: 18px;
    border-radius: 3px;
    flex-shrink: 0;
    border: 1.5px solid rgba(255,255,255,0.25);
  }
  #network-legend .swatch-circle {
    width: 18px;
    height: 18px;
    border-radius: 50%;
    flex-shrink: 0;
    border: 2px solid rgba(255,255,255,0.4);
  }
  #network-legend .leg-line {
    display: flex;
    align-items: center;
    gap: 9px;
    margin: 3px 0;
  }
  #network-legend .edge-solid {
    width: 32px;
    height: 3px;
    flex-shrink: 0;
  }
  #network-legend .edge-dashed {
    width: 32px;
    height: 0;
    border-top: 3px dashed;
    flex-shrink: 0;
  }
  #network-legend .icon-row {
    display: flex;
    align-items: center;
    gap: 7px;
    margin: 3px 0;
    font-size: 13px;
  }
  #network-legend #leg-toggle {
    margin-top: 11px;
    background: #223355;
    border: 1px solid #445577;
    color: #aaccff;
    border-radius: 5px;
    padding: 4px 10px;
    cursor: pointer;
    font-size: 11px;
    width: 100%;
  }
  #network-legend #leg-toggle:hover { background: #334466; }
  #network-legend #leg-body.hidden { display: none; }
</style>

<div id="network-legend">
  <h3>⬡ Legend</h3>
  <div id="leg-body">

    <div class="leg-section">Nodes — status</div>
    <div class="leg-row"><div class="swatch-circle" style="background:#22cc55;border-color:#007722;"></div> Target individual (director)</div>
    <div class="leg-row"><div class="swatch" style="background:#f5c518;border-color:#b8860b;"></div> Active — holds property</div>
    <div class="leg-row"><div class="swatch" style="background:#ff4444;border-color:#cc0000;"></div> In liquidation (CVL)</div>
    <div class="leg-row"><div class="swatch" style="background:#aaaaaa;border-color:#777777;"></div> Dissolved</div>
    <div class="leg-row"><div class="swatch" style="background:#5599dd;border-color:#2266aa;"></div> Active — no assets</div>
    <div class="leg-row"><div class="swatch" style="background:#9966dd;border-color:#5522aa;"></div> External corporate PSC</div>
    <div class="leg-row"><div class="swatch" style="background:#ff9944;border-color:#cc5500;"></div> PSC withdrawn 2024–25</div>
    <div class="leg-row"><div class="swatch" style="background:#ff88aa;border-color:#cc3366;"></div> Nominee PSC</div>

    <div class="leg-section">Edges — relationship</div>
    <div class="leg-line"><div class="edge-solid" style="background:#33ff88;"></div> Active director link</div>
    <div class="leg-line"><div class="edge-solid" style="background:#ffdd00;"></div> Active PSC (individual)</div>
    <div class="leg-line"><div class="edge-solid" style="background:#ff8800;"></div> Active corp-PSC link</div>
    <div class="leg-line"><div class="edge-dashed" style="border-color:#888888;"></div> Ceased / historical</div>

    <div class="leg-section">Node icons</div>
    <div class="icon-row">🏠 Holds CCOD-registered property</div>
    <div class="icon-row">⚠ Outstanding charge(s)</div>
    <div class="icon-row">⚡ Director(s) PSC recently withdrawn</div>
    <div class="icon-row">🚨 Nominee PSC in place</div>
    <div class="icon-row">❌ In CVL liquidation</div>
    <div class="icon-row">✝ Dissolved</div>

    <div class="leg-section">Tips</div>
    <div style="font-size:11px;color:#99aacc;line-height:1.6">
      Hover node → full details<br>
      Drag to pan · Scroll to zoom<br>
      Click node → highlight links<br>
      Stabilise button → freeze layout
    </div>
  </div>
  <button id="leg-toggle" onclick="
    var b=document.getElementById('leg-body');
    var t=document.getElementById('leg-toggle');
    if(b.classList.contains('hidden')){b.classList.remove('hidden');t.textContent='▲ Collapse';}
    else{b.classList.add('hidden');t.textContent='▼ Legend';}
  ">▲ Collapse</button>
</div>
"""

# Inject legend into the generated HTML just before </body>
with open(out, 'r', encoding='utf-8') as f:
    html = f.read()

html = html.replace('</body>', LEGEND_HTML + '\n</body>')

# Also make the canvas truly full-screen and dark
html = html.replace(
    '<body>',
    '<body style="margin:0;padding:0;overflow:hidden;background:#0d1117;">'
)
# Make the mynetwork div full page
html = re.sub(
    r'(#mynetwork\s*\{[^}]*?)height\s*:\s*[^;]+;',
    r'\1height: 100vh;',
    html
)

with open(out, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Graph written → {out}")
print(f"  {len(companies)} company nodes, {len(seen_edges)} edges")
print(f"  Gold (assets): {len(asset_holders & companies.keys())}")
print(f"  Grey (dissolved): {sum(1 for c in companies.values() if c['status']=='dissolved')}")
print(f"  Red (liquidation): {sum(1 for c in companies.values() if c['status']=='liquidation')}")
print(f"  Orange (PSC handover): {len(recent_handover)}")
print(f"  Pink (nominee PSC): {len(nominee_psc)}")

main.close()
bulk.close()
