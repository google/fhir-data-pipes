# Experimental Tutorial: Zero-Docker `synthea-hiv` Analytics & Dashboards with `fhir-pipes-rs` and DuckDB

This tutorial demonstrates how the experimental Rust + Apache Arrow engine ([`experimental/fhir-pipes-rs`](../../../experimental/fhir-pipes-rs/README.md)) and **DuckDB** can replace the multi-container Docker stack in [Step 2 (Single Machine Deployment)](single_machine.md) and [Step 3 (Visualize Parquet DWH with Apache Superset)](add_dashboard.md).

In **two commands** (and under 1 second of ETL time), you will:
1. Transform the **81 Synthea-HIV FHIR Bundles (`46,497` resources)** into **17 lossless Nested Parquet-on-FHIR (v1) tables** and **12 flattened SQL-on-FHIR (v2) Materialized Views**.
2. Launch a self-contained **Local DuckDB Web SQL Studio & Rebuilt Superset/PEPFAR HIV Dashboard** on `http://localhost:8765`.

---

## 1. Prerequisites

From the repository root, navigate to `experimental/fhir-pipes-rs`:

```bash
cd experimental/fhir-pipes-rs

# Build the native Rust binary
cargo build --release

# Install the Python DuckDB package for the local Web SQL Studio
pip3 install duckdb
```

---

## 2. Run the `synthea-hiv` ETL Pipeline (~0.9 Seconds)

Run `fhir-pipes-rs etl` against the `synthea-hiv` sample data segments and the 12 `ViewDefinition` configs in `docker/config/views`:

```bash
./target/release/fhir-pipes-rs etl \
  --input ../../synthea-hiv/sample_data_segments \
  --views-dir ../../docker/config/views \
  --output /tmp/fhir-pipes-rs-out
```

### Verify Quality Checks (Matching `single_machine.md`)
```bash
python3 -c "
import duckdb
con = duckdb.connect()
con.execute(open('/tmp/fhir-pipes-rs-out/init_duckdb.sql').read())
print('Patient count:', con.execute('SELECT COUNT(*) FROM Patient').fetchone()[0])
print('Observation count:', con.execute('SELECT COUNT(*) FROM Observation').fetchone()[0])
"
# Patient count: 79
# Observation count: 17279
```

---

## 3. Launch the Local DuckDB Web SQL Studio & Rebuilt Superset Dashboard

Start the self-contained local studio server:

```bash
python3 studio/duckdb_studio.py /tmp/fhir-pipes-rs-out --port 8765
```

Open in your browser:
- **Local DuckDB Web SQL Studio**: `http://localhost:8765/`
  - Browse all 29 Parquet tables/views in the left sidebar, inspect schemas, and run the 5 pre-loaded Superset & PEPFAR HIV queries in `< 2 ms`.
- **Rebuilt Superset "Key Program Metrics" & PEPFAR HIV/TB Dashboard**: `http://localhost:8765/dashboard`
  - Interactive rebuild of all 3 Superset charts from [add_dashboard.md](add_dashboard.md) (**Patient Count Big Number**, **Gender Split Interactive Donut/Pie Chart**, **HIV Treatment Start `1255%`/`1256%` Bar Chart by Year**) plus the **PEPFAR `TX_PVLS` Viral Load (`< 400,000`)** and **HIV/TB Care Cascade** explorer.

For the full walkthrough—including scaling up to **119,981+ FHIR R4 resources** from MITRE's general Synthea dataset and running incremental merges—see [**`experimental/fhir-pipes-rs/TUTORIAL_SYNTHEA_HIV.md`**](../../../experimental/fhir-pipes-rs/TUTORIAL_SYNTHEA_HIV.md).
