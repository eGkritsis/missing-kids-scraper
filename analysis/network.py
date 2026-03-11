"""
analysis/network.py
===================
Phase 3: Network Graph Builder

Builds a graph of connections between:
  - Missing children (nodes)
  - Family/surname clusters (nodes)
  - Locations / burst zones (nodes)
  - Named suspects from enrichment (nodes)
  - Court cases / DOJ press releases (nodes)
  - Trafficking corridors (edges)

Outputs:
  - analysis/output/network.json   (graph data for D3)
  - output/network.html            (interactive D3 force graph)

Usage:
  python analysis/network.py
  python analysis/network.py --min-connections 2
  python analysis/network.py --cluster-only
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.models import init_db, MissingPerson
from sqlalchemy import text

DB_PATH = "missing_children.db"

COUNTRY_NORM = {
    "USA": "United States", "US": "United States",
    "UK": "United Kingdom", "Russia": "Russian Federation",
}

def nc(c):
    if not c: return None
    return COUNTRY_NORM.get(c.strip(), c.strip())

def effective_age(p):
    if p.age_at_disappearance and p.age_at_disappearance >= 0:
        return p.age_at_disappearance
    if p.date_of_birth and p.date_missing:
        try:
            a = p.date_missing.year - p.date_of_birth.year
            return a if 0 <= a <= 17 else None
        except: pass
    return None


def build_network(db_path=DB_PATH, min_connections=1, cluster_only=False):
    engine, Session = init_db(db_path)
    db = Session()

    nodes = {}   # id -> node dict
    edges = []   # list of edge dicts
    node_id = [0]

    def make_id():
        node_id[0] += 1
        return node_id[0]

    def add_node(key, label, node_type, meta=None):
        if key not in nodes:
            nodes[key] = {
                "id":    key,
                "label": label[:60] if label else "",
                "type":  node_type,
                "meta":  meta or {},
                "connections": 0,
            }
        return key

    def add_edge(src, tgt, rel, weight=1, meta=None):
        edges.append({
            "source": src,
            "target": tgt,
            "relation": rel,
            "weight": weight,
            "meta": meta or {},
        })
        if src in nodes: nodes[src]["connections"] += 1
        if tgt in nodes: nodes[tgt]["connections"] += 1

    # -----------------------------------------------------------------------
    # Load cases
    # -----------------------------------------------------------------------
    cases = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False
    ).all()

    print(f"Building network from {len(cases)} cases...")

    # -----------------------------------------------------------------------
    # 1. SURNAME CLUSTERS → cluster nodes + child nodes
    # -----------------------------------------------------------------------
    from collections import Counter
    surname_country = defaultdict(list)
    for p in cases:
        if p.last_name:
            key = f"{p.last_name.strip().upper()}|{nc(p.country_last_seen) or 'Unknown'}"
            surname_country[key].append(p)

    cluster_nodes = {}
    for key, members in surname_country.items():
        if len(members) < 2:
            continue
        surname, country = key.split("|", 1)
        cluster_key = f"cluster_{key}"

        add_node(cluster_key, f"{surname} Family ({country})",
                 "cluster", {
                     "surname": surname,
                     "country": country,
                     "count":   len(members),
                 })

        for m in members:
            child_key = f"child_{m.id}"
            age = effective_age(m)
            add_node(child_key, m.full_name or f"Unknown #{m.id}",
                     "child", {
                         "id":      m.id,
                         "age":     age,
                         "gender":  m.gender,
                         "country": nc(m.country_last_seen),
                         "city":    m.city_last_seen,
                         "date":    str(m.date_missing) if m.date_missing else None,
                         "source":  m.source,
                         "url":     m.source_url,
                         "photo":   m.photo_url,
                     })
            add_edge(cluster_key, child_key, "FAMILY_MEMBER", weight=2)

    # -----------------------------------------------------------------------
    # 2. SPATIO-TEMPORAL BURSTS → location nodes
    # -----------------------------------------------------------------------
    from collections import defaultdict as dd
    location_cases = dd(list)
    for p in cases:
        if not p.date_missing:
            continue
        city    = (p.city_last_seen or "").strip().upper()
        country = nc(p.country_last_seen) or "Unknown"
        location_cases[(city, country)].append(p)

    for (city, country), group in location_cases.items():
        if len(group) < 3:
            continue
        # Check for burst (30-day window)
        dated = sorted([m for m in group if m.date_missing],
                       key=lambda x: x.date_missing)
        if not dated:
            continue
        span = (dated[-1].date_missing - dated[0].date_missing).days
        if span > 90:
            continue

        loc_key  = f"location_{city}_{country}".replace(" ", "_")
        is_active = dated[-1].date_missing >= (date.today() - timedelta(days=90))
        add_node(loc_key, f"{city.title()}, {country}" if city else country,
                 "location_burst", {
                     "city":      city.title(),
                     "country":   country,
                     "count":     len(group),
                     "span_days": span,
                     "is_active": is_active,
                     "date_from": str(dated[0].date_missing),
                     "date_to":   str(dated[-1].date_missing),
                 })

        for m in group:
            child_key = f"child_{m.id}"
            if child_key not in nodes:
                add_node(child_key, m.full_name or f"Unknown #{m.id}",
                         "child", {
                             "id":      m.id,
                             "age":     effective_age(m),
                             "country": nc(m.country_last_seen),
                             "city":    m.city_last_seen,
                             "date":    str(m.date_missing) if m.date_missing else None,
                             "source":  m.source,
                             "url":     m.source_url,
                             "photo":   m.photo_url,
                         })
            add_edge(loc_key, child_key, "BURST_LOCATION", weight=1)

    # -----------------------------------------------------------------------
    # 3. CORRIDOR EDGES (nationality → country)
    # -----------------------------------------------------------------------
    nat_cases = [p for p in cases if p.nationality and p.country_last_seen]
    corridor_flows = defaultdict(int)
    for p in nat_cases:
        nats = [n.strip() for n in p.nationality.split(",")]
        dest = nc(p.country_last_seen)
        for nat in nats:
            nat = nc(nat) or nat
            if nat != dest:
                corridor_flows[(nat, dest)] += 1

    for (origin, dest), count in corridor_flows.items():
        if count < 1:
            continue
        orig_key = f"country_{origin}".replace(" ", "_")
        dest_key = f"country_{dest}".replace(" ", "_")
        add_node(orig_key, origin, "country", {"country": origin})
        add_node(dest_key, dest,   "country", {"country": dest})
        add_edge(orig_key, dest_key, "TRAFFICKING_FLOW",
                 weight=count, meta={"count": count})

        # Connect nat_cases children to corridor
        for p in nat_cases:
            nats = {nc(n.strip()) for n in p.nationality.split(",")}
            if origin in nats and nc(p.country_last_seen) == dest:
                child_key = f"child_{p.id}"
                if child_key not in nodes:
                    add_node(child_key, p.full_name or f"ID:{p.id}",
                             "child", {"id": p.id, "source": p.source,
                                       "url": p.source_url, "photo": p.photo_url})
                add_edge(orig_key, child_key, "NATIONALITY_ORIGIN", weight=1)
                add_edge(dest_key, child_key, "DISAPPEARED_IN",     weight=1)

    # -----------------------------------------------------------------------
    # 4. ENRICHMENT FINDINGS → suspect/court/news nodes
    # -----------------------------------------------------------------------
    try:
        findings = list(db.execute(text("""
            SELECT missing_person_id, source_type, source_name,
                   title, url, snippet, finding_type, relevance_score
            FROM enrichment_findings
            WHERE relevance_score >= 0.5
            ORDER BY relevance_score DESC
        """)).fetchall())

        print(f"  Loading {len(findings)} enrichment findings...")

        for row in findings:
            (pid, stype, sname, title, url,
             snippet, ftype, score) = row

            child_key   = f"child_{pid}"
            finding_key = f"finding_{stype}_{hash(url or title) % 999999}"

            node_type = {
                "COURT_TRAFFICKING": "court_case",
                "COURT_MENTION":     "court_case",
                "DOJ_TRAFFICKING":   "doj_case",
                "DOJ_MENTION":       "doj_case",
                "FBI_WANTED":        "fbi_wanted",
                "EUROPOL_OPERATION": "europol_op",
                "NEWS_MENTION":      "news",
                "NEWS_RESOLUTION":   "news",
                "SANCTIONS_NETWORK": "sanctions",
            }.get(ftype, "finding")

            add_node(finding_key, title[:60] if title else sname,
                     node_type, {
                         "source":  sname,
                         "url":     url,
                         "snippet": (snippet or "")[:200],
                         "type":    ftype,
                         "score":   score,
                     })

            if child_key not in nodes:
                # Minimal child node
                p = db.query(MissingPerson).get(pid)
                if p:
                    add_node(child_key, p.full_name or f"ID:{pid}",
                             "child", {"id": pid, "source": p.source,
                                       "url": p.source_url, "photo": p.photo_url})

            if child_key in nodes:
                add_edge(child_key, finding_key, ftype, weight=score)

    except Exception as e:
        print(f"  Enrichment findings not available: {e}")

    db.close()

    # -----------------------------------------------------------------------
    # Filter low-connection nodes
    # -----------------------------------------------------------------------
    if min_connections > 0:
        keep = {n for n, d in nodes.items() if d["connections"] >= min_connections}
        # Always keep high-value node types
        keep |= {n for n, d in nodes.items()
                 if d["type"] in ("court_case", "doj_case", "fbi_wanted",
                                  "europol_op", "sanctions")}
        nodes = {k: v for k, v in nodes.items() if k in keep}
        edges = [e for e in edges
                 if e["source"] in nodes and e["target"] in nodes]

    graph = {
        "nodes": list(nodes.values()),
        "edges": edges,
        "stats": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "by_type":     dict(Counter(n["type"] for n in nodes.values())),
            "generated":   datetime.now().isoformat(),
        },
    }

    print(f"Graph: {len(nodes)} nodes, {len(edges)} edges")
    return graph


def build_network_html(graph):
    graph_json = json.dumps(graph, default=str)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MCID — Network Graph</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:#07090f; color:#e8edf5; font-family:'Space Mono',monospace; overflow:hidden; }}
#controls {{
  position:fixed; top:0; left:0; right:0; z-index:100;
  background:rgba(7,9,15,.92); backdrop-filter:blur(12px);
  border-bottom:1px solid #1e2a3a;
  display:flex; align-items:center; gap:12px; padding:10px 20px;
  flex-wrap:wrap;
}}
.ctrl-label {{ font-size:11px; color:#6b7a99; letter-spacing:1px; text-transform:uppercase; }}
.ctrl-btn {{
  font-family:'Space Mono',monospace; font-size:11px; letter-spacing:1px;
  padding:5px 14px; border-radius:4px; cursor:pointer;
  border:1px solid #1e2a3a; background:#0e1220; color:#6b7a99;
  transition:.2s; text-transform:uppercase;
}}
.ctrl-btn:hover,.ctrl-btn.active {{ background:#e63946; border-color:#e63946; color:#fff; }}
#search {{
  font-family:'Space Mono',monospace; font-size:12px;
  background:#0e1220; border:1px solid #1e2a3a; color:#e8edf5;
  border-radius:6px; padding:6px 14px; outline:none; width:220px;
}}
#search:focus {{ border-color:#e63946; }}
#stats {{ font-size:11px; color:#6b7a99; margin-left:auto; }}
#tooltip {{
  position:fixed; background:#0e1220; border:1px solid #1e2a3a;
  border-radius:8px; padding:14px 18px; pointer-events:none;
  display:none; max-width:320px; z-index:200;
  box-shadow:0 8px 32px rgba(0,0,0,.6); font-size:12px;
}}
#tooltip .tt-name {{ font-size:14px; font-weight:700; margin-bottom:8px; }}
#tooltip .tt-row {{ color:#6b7a99; margin:3px 0; }}
#tooltip .tt-row span {{ color:#e8edf5; }}
#tooltip a {{ color:#e63946; }}
#legend {{
  position:fixed; bottom:20px; left:20px; z-index:100;
  background:rgba(14,18,32,.92); border:1px solid #1e2a3a;
  border-radius:8px; padding:14px 16px;
}}
.leg-item {{ display:flex; align-items:center; gap:8px; margin:4px 0; font-size:11px; color:#6b7a99; }}
.leg-dot {{ width:10px; height:10px; border-radius:50%; flex-shrink:0; }}
svg {{ width:100vw; height:100vh; cursor:grab; }}
svg:active {{ cursor:grabbing; }}
</style>
</head>
<body>
<div id="controls">
  <span class="ctrl-label">MCID // Network</span>
  <button class="ctrl-btn active" onclick="filterType('all')">All</button>
  <button class="ctrl-btn" onclick="filterType('child')">Children</button>
  <button class="ctrl-btn" onclick="filterType('cluster')">Clusters</button>
  <button class="ctrl-btn" onclick="filterType('location_burst')">Bursts</button>
  <button class="ctrl-btn" onclick="filterType('court_case,doj_case,europol_op,sanctions')">Legal</button>
  <input id="search" placeholder="Search node..." oninput="searchNode(this.value)">
  <span id="stats"></span>
</div>
<div id="tooltip"></div>
<div id="legend">
  <div class="leg-item"><div class="leg-dot" style="background:#e63946"></div>Child (missing)</div>
  <div class="leg-item"><div class="leg-dot" style="background:#f4a261"></div>Family Cluster</div>
  <div class="leg-item"><div class="leg-dot" style="background:#e9c46a"></div>Location Burst</div>
  <div class="leg-item"><div class="leg-dot" style="background:#2a9d8f"></div>Court / Legal</div>
  <div class="leg-item"><div class="leg-dot" style="background:#457b9d"></div>Country / Corridor</div>
  <div class="leg-item"><div class="leg-dot" style="background:#9b5de5"></div>DOJ / FBI</div>
</div>
<svg id="svg"></svg>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script>
const GRAPH = {graph_json};

const TYPE_COLOR = {{
  child:            '#e63946',
  cluster:          '#f4a261',
  location_burst:   '#e9c46a',
  court_case:       '#2a9d8f',
  doj_case:         '#9b5de5',
  fbi_wanted:       '#9b5de5',
  europol_op:       '#2a9d8f',
  sanctions:        '#ff6b6b',
  news:             '#a8dadc',
  country:          '#457b9d',
  finding:          '#6b7a99',
}};

const TYPE_RADIUS = {{
  child:          5,
  cluster:        14,
  location_burst: 18,
  court_case:     10,
  doj_case:       10,
  fbi_wanted:     10,
  europol_op:     12,
  sanctions:      12,
  country:        16,
  news:           6,
  finding:        7,
}};

let currentFilter = 'all';
let allNodes = GRAPH.nodes;
let allEdges = GRAPH.edges;
let simulation, svg, g, link, node, label;

function getVisibleData(filter) {{
  let nodes, edges;
  if(filter === 'all') {{
    nodes = allNodes;
    edges = allEdges;
  }} else {{
    const types = filter.split(',');
    const nodeSet = new Set(allNodes.filter(n=>types.includes(n.type)).map(n=>n.id));
    // Include connected nodes
    allEdges.forEach(e => {{
      if(nodeSet.has(e.source) || nodeSet.has(e.target)) {{
        nodeSet.add(e.source);
        nodeSet.add(e.target);
      }}
    }});
    nodes = allNodes.filter(n => nodeSet.has(n.id));
    edges = allEdges.filter(e => nodeSet.has(e.source) && nodeSet.has(e.target));
  }}
  return {{nodes: nodes.slice(0,800), edges}};
}}

function init() {{
  svg = d3.select('#svg');
  const W = window.innerWidth, H = window.innerHeight;
  g = svg.append('g');

  // Zoom
  svg.call(d3.zoom()
    .scaleExtent([0.05, 5])
    .on('zoom', e => g.attr('transform', e.transform)));

  render('all');
}}

function render(filter) {{
  currentFilter = filter;
  const {{nodes, edges}} = getVisibleData(filter);
  const nodeById = Object.fromEntries(nodes.map(n=>[n.id,n]));

  // Build links using index refs
  const links = edges
    .filter(e => nodeById[e.source] && nodeById[e.target])
    .map(e => ({{...e, source: e.source, target: e.target}}));

  g.selectAll('*').remove();

  if(simulation) simulation.stop();

  simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d=>d.id)
                                      .distance(d => {{
                                        if(d.relation==='FAMILY_MEMBER') return 60;
                                        if(d.relation==='BURST_LOCATION') return 80;
                                        return 120;
                                      }})
                                      .strength(0.4))
    .force('charge', d3.forceManyBody().strength(-120))
    .force('center', d3.forceCenter(window.innerWidth/2, window.innerHeight/2))
    .force('collision', d3.forceCollide().radius(d =>
      (TYPE_RADIUS[d.type]||6) + 4))
    .alphaDecay(0.02);

  // Links
  link = g.append('g').selectAll('line').data(links).join('line')
    .attr('stroke', d => {{
      if(d.relation==='TRAFFICKING_FLOW') return '#f4a261';
      if(d.relation.includes('COURT')||d.relation.includes('DOJ')) return '#2a9d8f';
      return '#1e2a3a';
    }})
    .attr('stroke-opacity', 0.6)
    .attr('stroke-width', d => Math.min((d.weight||1)*0.8+0.5, 4));

  // Nodes
  node = g.append('g').selectAll('circle').data(nodes).join('circle')
    .attr('r', d => TYPE_RADIUS[d.type] || 6)
    .attr('fill', d => TYPE_COLOR[d.type] || '#6b7a99')
    .attr('fill-opacity', 0.85)
    .attr('stroke', '#07090f')
    .attr('stroke-width', 1.5)
    .style('cursor','pointer')
    .call(d3.drag()
      .on('start', dragStart)
      .on('drag',  dragged)
      .on('end',   dragEnd))
    .on('mouseover', showTooltip)
    .on('mouseout',  hideTooltip)
    .on('click', nodeClick);

  // Labels for large nodes only
  label = g.append('g').selectAll('text')
    .data(nodes.filter(n => (TYPE_RADIUS[n.type]||6) >= 12))
    .join('text')
    .text(d => d.label)
    .attr('font-size', 9)
    .attr('fill', '#6b7a99')
    .attr('text-anchor','middle')
    .attr('dy', d => -(TYPE_RADIUS[d.type]||6) - 4)
    .style('pointer-events','none');

  simulation.on('tick', () => {{
    link
      .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
    node.attr('cx', d => d.x).attr('cy', d => d.y);
    label.attr('x', d => d.x).attr('y', d => d.y);
  }});

  document.getElementById('stats').textContent =
    `${{nodes.length}} nodes · ${{links.length}} edges`;
}}

function dragStart(e, d) {{
  if(!e.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x; d.fy = d.y;
}}
function dragged(e, d) {{ d.fx = e.x; d.fy = e.y; }}
function dragEnd(e, d) {{
  if(!e.active) simulation.alphaTarget(0);
  d.fx = null; d.fy = null;
}}

function showTooltip(e, d) {{
  const m = d.meta || {{}};
  let rows = '';
  if(m.age != null)     rows += `<div class="tt-row">Age: <span>${{m.age}}</span></div>`;
  if(m.country)         rows += `<div class="tt-row">Country: <span>${{m.country}}</span></div>`;
  if(m.city)            rows += `<div class="tt-row">City: <span>${{m.city}}</span></div>`;
  if(m.date)            rows += `<div class="tt-row">Missing: <span>${{m.date}}</span></div>`;
  if(m.count)           rows += `<div class="tt-row">Cases: <span>${{m.count}}</span></div>`;
  if(m.span_days!=null) rows += `<div class="tt-row">Span: <span>${{m.span_days}}d</span></div>`;
  if(m.score)           rows += `<div class="tt-row">Score: <span>${{m.score?.toFixed(2)}}</span></div>`;
  if(m.snippet)         rows += `<div class="tt-row" style="margin-top:6px;color:#aaa;font-size:10px">${{m.snippet.substring(0,120)}}...</div>`;
  if(m.url)             rows += `<div style="margin-top:8px"><a href="${{m.url}}" target="_blank">↗ View source</a></div>`;

  const tt = document.getElementById('tooltip');
  tt.innerHTML = `
    <div class="tt-name">${{d.label}}</div>
    <div class="tt-row" style="margin-bottom:6px;color:${{TYPE_COLOR[d.type]||'#6b7a99'}}">${{d.type.replace(/_/g,' ').toUpperCase()}}</div>
    ${{rows}}
  `;
  tt.style.display = 'block';
  tt.style.left = (e.clientX + 16)+'px';
  tt.style.top  = (e.clientY - 10)+'px';
}}

function hideTooltip() {{
  document.getElementById('tooltip').style.display = 'none';
}}

function nodeClick(e, d) {{
  if(d.meta?.url) window.open(d.meta.url, '_blank');
}}

function filterType(type) {{
  document.querySelectorAll('.ctrl-btn').forEach(b=>b.classList.remove('active'));
  event.target.classList.add('active');
  render(type);
}}

function searchNode(q) {{
  if(!q) {{ node?.attr('opacity',1); return; }}
  node?.attr('opacity', d =>
    d.label.toLowerCase().includes(q.toLowerCase()) ? 1 : 0.1);
}}

window.addEventListener('resize', () => {{
  simulation?.force('center', d3.forceCenter(
    window.innerWidth/2, window.innerHeight/2));
}});

init();
</script>
</body>
</html>"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Network Graph Builder")
    parser.add_argument("--db",              default=DB_PATH)
    parser.add_argument("--min-connections", type=int, default=1)
    parser.add_argument("--cluster-only",    action="store_true")
    parser.add_argument("--out",             default="output")
    args = parser.parse_args()

    Path(args.out).mkdir(parents=True, exist_ok=True)
    Path("analysis/output").mkdir(parents=True, exist_ok=True)

    graph = build_network(args.db, args.min_connections, args.cluster_only)

    json_path = "analysis/output/network.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(graph, f, default=str)
    print(f"JSON → {json_path}")

    html = build_network_html(graph)
    html_path = f"{args.out}/network.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML → {html_path}  ({round(os.path.getsize(html_path)/1024)}KB)")
    print(f"\nStats: {graph['stats']}")
