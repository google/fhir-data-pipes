#!/usr/bin/env python3
"""
Self-Contained Local DuckDB Web SQL Studio & Synthea-HIV Superset/PEPFAR Dashboard Server.

Unlike DuckDB's built-in `CALL start_ui()` extension (which proxies UI assets from
https://ui.duckdb.org and fails in offline, edge, or TLS-inspected environments),
this studio is 100% local and self-contained.

Usage:
  python3 studio/duckdb_studio.py /tmp/fhir-pipes-rs-out --port 8765
"""
import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import duckdb

HTML_SQL_VIEWER = r"""<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>DuckDB Local Web SQL Studio — fhir-pipes-rs</title>
  <script src="https://www.gstatic.com/antigravity/web/dev/tailwindcss.min.js"></script>
</head>
<body class="bg-slate-950 text-slate-100 antialiased h-screen flex flex-col overflow-hidden">

  <!-- Top Navbar -->
  <header class="h-14 border-b border-slate-800 bg-slate-900/80 px-5 flex items-center justify-between shrink-0">
    <div class="flex items-center gap-3">
      <span class="px-2.5 py-1 text-xs font-bold rounded-md bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
        DuckDB __DUCKDB_VERSION__
      </span>
      <h1 class="text-sm font-bold text-slate-100">
        fhir-pipes-rs Local Web SQL Studio
      </h1>
      <span class="text-xs text-slate-400 font-mono hidden sm:inline">(__WAREHOUSE_PATH__)</span>
    </div>
    <div class="flex items-center gap-2.5 text-xs">
      <a href="/dashboard" target="_blank" class="px-3.5 py-1.5 rounded-lg font-semibold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 hover:bg-emerald-500/30 transition">
        Open Interactive Synthea-HIV Dashboard ↗
      </a>
    </div>
  </header>

  <!-- Main Workspace -->
  <div class="flex-1 flex overflow-hidden">
    <!-- Left Sidebar: Schema & Tables Browser -->
    <aside class="w-72 border-r border-slate-800 bg-slate-900/50 flex flex-col shrink-0">
      <div class="p-3 border-b border-slate-800">
        <input id="table-filter" type="text" oninput="renderSidebar()" placeholder="Filter Parquet views..."
          class="w-full px-3 py-1.5 rounded-lg text-xs bg-slate-800 text-slate-100 border border-slate-700 focus:outline-none focus:border-emerald-500">
      </div>
      <div id="sidebar-tables" class="flex-1 overflow-y-auto p-2.5 space-y-4 text-xs"></div>
    </aside>

    <!-- Right Area: SQL Editor + Results Table -->
    <main class="flex-1 flex flex-col overflow-hidden">
      <!-- SQL Editor Pane -->
      <div class="p-4 border-b border-slate-800 bg-slate-900/30 space-y-3 shrink-0">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div class="flex flex-wrap items-center gap-1.5 text-xs">
            <span class="text-slate-400 font-medium mr-1">Superset &amp; PEPFAR Tutorial Queries:</span>
            <button onclick="setQuery(0)" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 cursor-pointer">1. Superset Chart 2: Gender Split</button>
            <button onclick="setQuery(1)" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 cursor-pointer">2. Superset Chart 3: HIV Tx (v2 Flat)</button>
            <button onclick="setQuery(2)" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 cursor-pointer">3. Superset Chart 3: HIV Tx (v1 LATERAL)</button>
            <button onclick="setQuery(3)" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 cursor-pointer">4. PEPFAR Viral Load &lt; 400k (Year/Sex)</button>
            <button onclick="setQuery(4)" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 cursor-pointer">5. HIV &amp; TB Care Cascade</button>
          </div>
          <div class="flex items-center gap-2">
            <button onclick="exportCsv()" class="px-3 py-1.5 rounded-lg text-xs font-medium bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 cursor-pointer">
              Export CSV
            </button>
            <button onclick="runQuery()" class="px-4 py-1.5 rounded-lg text-xs font-semibold bg-emerald-600 hover:bg-emerald-500 text-white shadow cursor-pointer">
              ▶ Run SQL (Cmd+Enter)
            </button>
          </div>
        </div>

        <textarea id="sql-input" rows="7" spellcheck="false"
          class="w-full p-3 rounded-xl bg-slate-950 border border-slate-800 font-mono text-xs text-emerald-300 focus:outline-none focus:border-emerald-500 leading-relaxed"></textarea>

        <div class="flex items-center justify-between text-xs">
          <div id="status-bar" class="text-slate-400 font-mono">Ready</div>
          <div class="text-slate-400">Limit max rows returned: <input id="row-limit" type="number" value="500" class="w-20 px-2 py-0.5 rounded bg-slate-800 border border-slate-700 text-slate-200 text-center font-mono"></div>
        </div>
      </div>

      <!-- Results Grid -->
      <div class="flex-1 overflow-auto p-4 bg-slate-950">
        <div id="error-box" class="hidden p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 font-mono text-xs whitespace-pre-wrap mb-4"></div>
        <div class="border border-slate-800 rounded-xl overflow-hidden">
          <table class="w-full text-left border-collapse text-xs">
            <thead id="result-thead" class="sticky top-0 bg-slate-900 border-b border-slate-800 text-slate-300 font-mono"></thead>
            <tbody id="result-tbody" class="divide-y divide-slate-800/70 font-mono"></tbody>
          </table>
        </div>
      </div>
    </main>
  </div>

  <script>
    let CATALOG = { flat: [], nested: [] };
    let LAST_RESULT = { columns: [], rows: [] };

    const PRESETS = [
      `-- Superset Tutorial Chart 1 & 2 (doc/docs/tutorials/add_dashboard.md):
-- Patient Count & Gender Split across the 79 Synthea-HIV Patients
SELECT
    gender,
    COUNT(gender) AS count_gender,
    ROUND(COUNT(gender) * 100.0 / SUM(COUNT(gender)) OVER (), 1) AS pct_of_cohort
FROM Patient
GROUP BY gender
ORDER BY count_gender DESC;`,
      `-- Superset Tutorial Chart 3 (doc/docs/tutorials/add_dashboard.md):
-- HIV Treatment Initiations by Year using SQL-on-FHIR v2 observation_flat
-- CIEL 1255% = ANTIRETROVIRAL PLAN, CIEL 1256% = START DRUGS
SELECT
    CAST(EXTRACT(YEAR FROM TRY_CAST(obs_date AS TIMESTAMP)) AS INT) AS tx_year,
    COUNT(*) AS tx_events,
    COUNT(DISTINCT patient_id) AS distinct_patients
FROM observation_flat
WHERE code_code LIKE '1255%'
  AND value_code LIKE '1256%'
  AND EXTRACT(YEAR FROM TRY_CAST(obs_date AS TIMESTAMP)) < 2023
GROUP BY 1
ORDER BY 1 ASC;`,
      `-- Superset Tutorial Chart 3 & query/test_duckdb.ipynb (Cell 20):
-- Same HIV Treatment Query directly over Nested Observation.parquet using LATERAL UNNEST
SELECT
    CAST(EXTRACT(YEAR FROM TRY_CAST(O.effective.dateTime AS TIMESTAMP)) AS INT) AS tx_year,
    COUNT(*) AS tx_events,
    COUNT(DISTINCT O.subject.PatientId) AS distinct_patients
FROM Observation AS O,
     LATERAL (SELECT unnest(O.code.coding) AS OCC),
     LATERAL (SELECT unnest(O.value.codeableConcept.coding) AS OVCC)
WHERE OCC.code LIKE '1255%'
  AND OVCC.code LIKE '1256%'
  AND EXTRACT(YEAR FROM TRY_CAST(O.effective.dateTime AS TIMESTAMP)) < 2023
GROUP BY 1
ORDER BY 1 ASC;`,
      `-- PEPFAR TX_PVLS Indicator (query/queries_large.ipynb Cell 20 & 27):
-- Patients with HIV Viral Load (CIEL/LOINC 856%) < 400,000 copies/mL by Year & Gender
WITH p AS (
    SELECT DISTINCT id, gender FROM patient_flat
)
SELECT
    CAST(EXTRACT(YEAR FROM TRY_CAST(o.obs_date AS TIMESTAMP)) AS INT) AS obs_year,
    p.gender,
    COUNT(DISTINCT o.patient_id) AS patients_below_400k,
    COUNT(*) AS viral_load_tests,
    ROUND(AVG(o.val_quantity), 1) AS mean_viral_load
FROM observation_flat o
JOIN p ON o.patient_id = p.id
WHERE o.code_code LIKE '856%'
  AND o.code_sys = 'http://loinc.org'
  AND o.val_quantity < 400000
GROUP BY 1, 2
ORDER BY obs_year DESC, gender;`,
      `-- Synthea-HIV Clinical Care Cascade (ART Plans, ARV Regimens, TB Prophylaxis/Treatment):
SELECT
    code_code,
    code_display AS hiv_tb_indicator,
    value_display AS regimen_or_action,
    COUNT(*) AS total_events,
    COUNT(DISTINCT patient_id) AS distinct_patients
FROM observation_flat
WHERE code_code LIKE '1255%'
   OR code_code LIKE '1088%'
   OR code_code LIKE '1268%'
GROUP BY 1, 2, 3
ORDER BY code_display, distinct_patients DESC;`
    ];

    async function loadCatalog() {
      const resp = await fetch('/api/catalog');
      CATALOG = await resp.json();
      renderSidebar();
    }

    function renderSidebar() {
      const q = (document.getElementById('table-filter').value || '').toLowerCase();
      const renderGroup = (title, items) => {
        const filtered = items.filter(t => t.name.toLowerCase().includes(q));
        return `
          <div>
            <div class="flex items-center justify-between px-2 mb-1.5">
              <span class="font-bold uppercase tracking-wider text-slate-400">${title}</span>
              <span class="px-1.5 py-0.5 rounded bg-slate-800 text-slate-300 font-mono">${filtered.length}</span>
            </div>
            <div class="space-y-1">
              ${filtered.map(t => `
                <div class="rounded-lg border border-slate-800/80 bg-slate-900/60 hover:border-slate-700 transition">
                  <div class="flex items-center justify-between px-2.5 py-1.5 cursor-pointer" onclick="previewTable('${t.name}')">
                    <span class="font-mono font-semibold text-emerald-400 truncate">${t.name}</span>
                    <span class="font-mono text-slate-400">${t.rows.toLocaleString()}</span>
                  </div>
                  <div class="px-2.5 pb-1.5 flex items-center justify-between text-slate-400">
                    <span>${t.cols.length} cols</span>
                    <button onclick="describeTable('${t.name}')" class="text-indigo-400 hover:underline cursor-pointer">Schema</button>
                  </div>
                </div>
              `).join('')}
            </div>
          </div>
        `;
      };

      document.getElementById('sidebar-tables').innerHTML =
        renderGroup('Materialized Views (v2)', CATALOG.flat) +
        renderGroup('Nested Parquet-on-FHIR (v1)', CATALOG.nested);
    }

    function previewTable(name) {
      document.getElementById('sql-input').value = `SELECT *\nFROM ${name}\nLIMIT 50;`;
      runQuery();
    }

    function describeTable(name) {
      document.getElementById('sql-input').value = `DESCRIBE ${name};`;
      runQuery();
    }

    function setQuery(idx) {
      document.getElementById('sql-input').value = PRESETS[idx];
      runQuery();
    }

    async function runQuery() {
      const sql = document.getElementById('sql-input').value;
      const limit = parseInt(document.getElementById('row-limit').value || '500', 10);
      const status = document.getElementById('status-bar');
      const errBox = document.getElementById('error-box');
      status.textContent = 'Running DuckDB query...';
      errBox.classList.add('hidden');

      const t0 = performance.now();
      try {
        const resp = await fetch('/api/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql, limit })
        });
        const data = await resp.json();
        const elapsed = (performance.now() - t0).toFixed(1);
        if (!data.ok) {
          errBox.textContent = data.error;
          errBox.classList.remove('hidden');
          status.textContent = `Query failed (${elapsed} ms)`;
          return;
        }
        LAST_RESULT = data;
        status.textContent = `Returned ${data.rows.length} rows in ${data.server_ms} ms (DuckDB engine) / ${elapsed} ms total`;
        document.getElementById('result-thead').innerHTML = `<tr>${data.columns.map(c => `<th class="py-2.5 px-3 font-semibold border-r border-slate-800/60">${c}</th>`).join('')}</tr>`;
        document.getElementById('result-tbody').innerHTML = data.rows.map(r => `
          <tr class="hover:bg-slate-900/60">
            ${r.map(cell => {
              const formatted = (cell !== null && typeof cell === 'object') ? JSON.stringify(cell) : (cell ?? 'NULL');
              return `<td class="py-2 px-3 border-r border-slate-900 max-w-md truncate" title="${String(formatted).replace(/"/g, '&quot;')}">${formatted}</td>`;
            }).join('')}
          </tr>
        `).join('');
      } catch (e) {
        errBox.textContent = String(e);
        errBox.classList.remove('hidden');
      }
    }

    function exportCsv() {
      if (!LAST_RESULT.columns.length) return;
      const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
      const csv = [
        LAST_RESULT.columns.map(esc).join(','),
        ...LAST_RESULT.rows.map(r => r.map(c => esc(typeof c === 'object' && c !== null ? JSON.stringify(c) : c)).join(','))
      ].join('\n');
      const blob = new Blob([csv], { type: 'text/csv' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'duckdb_query_results.csv';
      a.click();
    }

    document.addEventListener('keydown', e => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        runQuery();
      }
    });

    loadCatalog();
    setQuery(1);
  </script>
</body>
</html>
"""


def build_catalog(con: duckdb.DuckDBPyConnection):
    flat = []
    nested = []
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    for t in sorted(tables):
        cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        cols = [r[0] for r in con.execute(f"DESCRIBE {t}").fetchall()]
        item = {"name": t, "rows": cnt, "cols": cols}
        if t.endswith("_flat"):
            flat.append(item)
        else:
            nested.append(item)
    flat.sort(key=lambda x: -x["rows"])
    nested.sort(key=lambda x: -x["rows"])
    return {"flat": flat, "nested": nested}


class DuckDBStudioHandler(BaseHTTPRequestHandler):
    con: duckdb.DuckDBPyConnection = None
    catalog = None
    warehouse_dir = ""
    dashboard_html_path = Path(__file__).resolve().parent / "synthea_hiv_dashboard.html"

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            page = (
                HTML_SQL_VIEWER.replace("__DUCKDB_VERSION__", duckdb.__version__)
                .replace("__WAREHOUSE_PATH__", self.warehouse_dir)
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(page.encode("utf-8"))
        elif self.path == "/dashboard":
            if self.dashboard_html_path.exists():
                data = self.dashboard_html_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_error(404, "Dashboard HTML not found")
        elif self.path == "/api/catalog":
            payload = json.dumps(self.catalog).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        if self.path == "/api/query":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            sql = body.get("sql", "")
            limit = int(body.get("limit", 500))
            t0 = time.perf_counter()
            try:
                cur = self.con.execute(sql)
                cols = [d[0] for d in cur.description] if cur.description else []
                rows = cur.fetchmany(limit)
                ms = round((time.perf_counter() - t0) * 1000, 2)
                payload = json.dumps(
                    {"ok": True, "columns": cols, "rows": rows, "server_ms": ms},
                    default=str,
                )
                self.send_response(200)
            except Exception as e:
                payload = json.dumps({"ok": False, "error": str(e)})
                self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload.encode("utf-8"))
        else:
            self.send_error(404)


def main():
    parser = argparse.ArgumentParser(description="Local DuckDB Web SQL Studio & Superset HIV Dashboard Server")
    parser.add_argument("output_dir", nargs="?", default="/tmp/fhir-pipes-rs-out", help="fhir-pipes-rs output directory")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (default: 8765)")
    args = parser.parse_args()

    con = duckdb.connect()
    init_sql_path = os.path.join(args.output_dir, "init_duckdb.sql")
    if os.path.exists(init_sql_path):
        with open(init_sql_path, "r", encoding="utf-8") as f:
            con.execute(f.read())

    DuckDBStudioHandler.con = con
    DuckDBStudioHandler.warehouse_dir = args.output_dir
    DuckDBStudioHandler.catalog = build_catalog(con)

    server = HTTPServer(("127.0.0.1", args.port), DuckDBStudioHandler)
    print(f"============================================================")
    print(f"DuckDB Local Web SQL Studio:  http://localhost:{args.port}/")
    print(f"Rebuilt Superset Dashboard:   http://localhost:{args.port}/dashboard")
    print(f"Loaded Parquet warehouse:     {args.output_dir}/init_duckdb.sql")
    print(f"============================================================", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
