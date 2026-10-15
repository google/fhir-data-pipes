# Tutorial: End-to-End `synthea-hiv` Analytics & Dashboards with `fhir-pipes-rs` and DuckDB

This tutorial walks through running the **Synthea-HIV** and **MITRE Synthea FHIR R4** datasets through `fhir-pipes-rs`, querying the resulting Parquet warehouse with **DuckDB**, and launching the **Local Web SQL Studio & Rebuilt Superset/PEPFAR HIV Dashboard**—all on a single laptop with **zero Docker containers, Flink clusters, or Spark Thrift Servers**.

It serves as a direct, lightweight counterpart to three existing tutorials/notebooks in this repository:
1. [**Single Machine Deployment Tutorial (`doc/docs/tutorials/single_machine.md`)**](../../doc/docs/tutorials/single_machine.md)
2. [**Visualize Parquet DWH with Apache Superset (`doc/docs/tutorials/add_dashboard.md`)**](../../doc/docs/tutorials/add_dashboard.md)
3. [**PEPFAR Indicator Queries & DuckDB Evaluation (`query/queries_large.ipynb` & `query/test_duckdb.ipynb`)**](../../query/queries_large.ipynb)

---

## 1. Workflow Comparison: Legacy Stack vs. `fhir-pipes-rs` + DuckDB

| Step | Legacy Tutorial (`single_machine.md` + `add_dashboard.md`) | `fhir-pipes-rs` + DuckDB Tutorial (This Guide) |
| :--- | :--- | :--- |
| **Infrastructure Required** | Docker Compose stack with HAPI FHIR, PostgreSQL, Spring Boot Pipeline Controller (`:8090`), Apache Flink, Apache Spark Thrift Server (`:10001`), and Apache Superset (`:8088`) (~6–8 GB RAM) | Single `fhir-pipes-rs` binary (~5 MB) + Python `duckdb` package (~50 MB RAM) |
| **ETL Execution (`synthea-hiv`)** | Upload bundles to HAPI $\rightarrow$ Trigger Full Run on `:8090` $\rightarrow$ Wait several minutes for Beam/Flink/Bunsen/Avro | `./target/release/fhir-pipes-rs etl ...` completes in **~0.9 seconds** (`46,497` resources) |
| **Query Engine Setup** | Inspect Docker bridge gateway IP $\rightarrow$ Configure `hive://hive@<ip>:10001` in Superset $\rightarrow$ Ignore connection creation error modal | Auto-generated `init_duckdb.sql` registers all **12 flat views** + **17 nested Parquet tables** in **<10 ms** |
| **HIV Indicator Query Speed** | 3–25 minutes in old `query/test_duckdb.ipynb` (because it ran `LATERAL UNNEST` over raw nested Parquet); ~6s on Spark | **0.8 ms – 1.4 ms** on pre-materialized `observation_flat.parquet` (**5.5 ms** even with `LATERAL UNNEST` on nested `Observation.parquet`) |

---

## 2. Prerequisites

From the repository root, navigate to `experimental/fhir-pipes-rs`:

```bash
cd experimental/fhir-pipes-rs

# 1. Build the release binary
cargo build --release

# 2. Ensure the Python duckdb package is installed for the local Web SQL Studio
pip3 install duckdb
```

---

## 3. Part 1: Run the `synthea-hiv` Pipeline & Verify Quality Checks

### Step 3.1: Run `fhir-pipes-rs etl` on `synthea-hiv`
Run the pipeline against the repository's 81 Synthea-HIV bundle files (`../../synthea-hiv/sample_data_segments`) and the 12 official SQL-on-FHIR v2 `ViewDefinition`s (`../../docker/config/views`):

```bash
./target/release/fhir-pipes-rs etl \
  --input ../../synthea-hiv/sample_data_segments \
  --views-dir ../../docker/config/views \
  --output /tmp/fhir-pipes-rs-out
```

In **~0.9 seconds**, `fhir-pipes-rs` produces:
- `/tmp/fhir-pipes-rs-out/parquet_on_fhir/<ResourceType>/<ResourceType>.parquet` — **17 Nested Parquet-on-FHIR (SQL-on-FHIR v1) tables** (`12.93 MB`).
- `/tmp/fhir-pipes-rs-out/materialized_views/<view_name>.parquet` — **12 Flattened Materialized View (SQL-on-FHIR v2) tables** (`46,843 rows`, `1.91 MB`).
- `/tmp/fhir-pipes-rs-out/materialized_views/schema.sql` — Relational `CREATE TABLE` DDL for PostgreSQL / DuckDB / SQLite.
- `/tmp/fhir-pipes-rs-out/init_duckdb.sql` — Auto-generated DuckDB view definitions for all 29 flat and nested Parquet tables.

### Step 3.2: Reproduce the Quality Checks from `single_machine.md`
In [`single_machine.md` (View and Analyze the Data)](../../doc/docs/tutorials/single_machine.md), the tutorial verifies that `Patient` has `79` rows and `Observation` has `17,279` rows. You can verify this immediately via CLI or DuckDB:

```bash
# Option A: Using the built-in Rust Parquet inspector (no DuckDB needed)
./target/release/fhir-pipes-rs inspect --file /tmp/fhir-pipes-rs-out/parquet_on_fhir/Patient/Patient.parquet --limit 2
./target/release/fhir-pipes-rs inspect --file /tmp/fhir-pipes-rs-out/parquet_on_fhir/Observation/Observation.parquet --limit 1

# Option B: Using DuckDB
python3 -c "
import duckdb
con = duckdb.connect()
con.execute(open('/tmp/fhir-pipes-rs-out/init_duckdb.sql').read())
print('Patient count:', con.execute('SELECT COUNT(*) FROM Patient').fetchone()[0])
print('Observation count:', con.execute('SELECT COUNT(*) FROM Observation').fetchone()[0])
"
```
Output:
```text
Patient count: 79
Observation count: 17279
```

---

## 4. Part 2: Launch the Local Web SQL Studio & Rebuilt Superset HIV Dashboard

> **Why a Self-Contained Studio instead of `duckdb -ui` (`CALL start_ui()`)?**  
> DuckDB's built-in `ui` extension starts a local reverse proxy on port `4213` that fetches its web application assets live from `https://ui.duckdb.org`. In offline clinics, air-gapped hospital servers, or corporate networks with TLS inspection, that external fetch fails (`Could not fetch '/' from 'https://ui.duckdb.org'`).  
> Our included [`studio/duckdb_studio.py`](./studio/duckdb_studio.py) is **100% self-contained on `localhost`** and pre-loads the `synthea-hiv` warehouse and tutorial queries automatically.

Start the local studio server:

```bash
python3 studio/duckdb_studio.py /tmp/fhir-pipes-rs-out --port 8765
```

Then open in your browser:
1. **Local DuckDB Web SQL Studio**: [**http://localhost:8765/**](http://localhost:8765/)
   - Browse all **12 Materialized Views** and **17 Nested Parquet-on-FHIR tables** in the left sidebar.
   - Click any table to preview `SELECT * FROM <table> LIMIT 50;` or click **Schema** to inspect column types.
   - Use the **5 preset buttons** at the top of the editor to run the exact Superset & PEPFAR HIV queries below, or write custom SQL (`Cmd+Enter` / `Ctrl+Enter`) and export to CSV.
2. **Rebuilt Superset "Key Program Metrics" & PEPFAR HIV/TB Dashboard**: [**http://localhost:8765/dashboard**](http://localhost:8765/dashboard)
   - Rebuilds all 3 charts from [`add_dashboard.md`](../../doc/docs/tutorials/add_dashboard.md) (**Patient Count Big Number**, **Interactive Gender Split Donut/Pie Chart**, and **HIV Treatment Initiations by Year Bar Chart**).
   - Adds the **PEPFAR `TX_PVLS` Viral Load (`< 400,000` copies/mL) by Year & Gender** chart from [`query/queries_large.ipynb`](../../query/queries_large.ipynb).
   - Adds interactive cross-filtering across **Antiretroviral Plans (`1255%`)**, **ARV Regimens (`1088%` / `1250%`)**, and **TB Co-Infection Regimens (`1268%` / `1270%`)**, plus a clickable **Patient 360° Longitudinal Viral Load Modal** for all 79 patients.

---

## 5. Part 3: The Superset & PEPFAR HIV SQL Queries Explained

Below are the exact queries from the legacy Superset tutorial and Jupyter notebooks translated to DuckDB over the `fhir-pipes-rs` outputs.

### Query 1: Gender Split (Superset Tutorial Chart 2)
```sql
SELECT
    gender,
    COUNT(gender) AS count_gender,
    ROUND(COUNT(gender) * 100.0 / SUM(COUNT(gender)) OVER (), 1) AS pct_of_cohort
FROM Patient
GROUP BY gender
ORDER BY count_gender DESC;
```
| gender | count_gender | pct_of_cohort |
| :--- | ---: | ---: |
| `female` | 41 | 51.9% |
| `male` | 38 | 48.1% |

### Query 2: HIV Treatment Initiations by Year (Superset Tutorial Chart 3)
In OpenMRS/CIEL terminology used by `synthea-hiv`, concept `1255AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA` is **ANTIRETROVIRAL PLAN** and value `1256AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA` is **START DRUGS**.

#### A. Using the Flattened Materialized View (`observation_flat` — 0.8 ms)
```sql
SELECT
    CAST(EXTRACT(YEAR FROM TRY_CAST(obs_date AS TIMESTAMP)) AS INT) AS tx_year,
    COUNT(*) AS tx_events,
    COUNT(DISTINCT patient_id) AS distinct_patients
FROM observation_flat
WHERE code_code LIKE '1255%'
  AND value_code LIKE '1256%'
  AND EXTRACT(YEAR FROM TRY_CAST(obs_date AS TIMESTAMP)) < 2023
GROUP BY 1
ORDER BY 1 ASC;
```

#### B. Using the Nested Parquet-on-FHIR Table (`Observation` with `LATERAL UNNEST` — 5.5 ms)
```sql
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
ORDER BY 1 ASC;
```

### Query 3: PEPFAR `TX_PVLS` — Patients with HIV Viral Load < 400,000 by Year & Gender (`queries_large.ipynb`)
```sql
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
ORDER BY obs_year DESC, gender;
```

### Query 4: HIV & Tuberculosis Co-Infection Care Cascade
```sql
SELECT
    code_code,
    code_display AS indicator,
    value_display AS regimen_or_action,
    COUNT(*) AS total_events,
    COUNT(DISTINCT patient_id) AS distinct_patients
FROM observation_flat
WHERE code_code LIKE '1255%'   -- Antiretroviral Plan
   OR code_code LIKE '1088%'   -- Current ARV Drugs Used for Treatment
   OR code_code LIKE '1268%'   -- Tuberculosis Treatment Plan
GROUP BY 1, 2, 3
ORDER BY indicator, distinct_patients DESC;
```

---

## 6. Part 4: Testing Incremental Sync & Deduplication

The repository includes an incremental test dataset in `../../synthea-hiv/sample_data_small` and `../../synthea-hiv/sample_data_small/incremental`. You can run a base + incremental merge in a single command using `--incremental-input`:

```bash
./target/release/fhir-pipes-rs etl \
  --input ../../synthea-hiv/sample_data_small \
  --incremental-input ../../synthea-hiv/sample_data_small/incremental \
  --views-dir ../../docker/config/views \
  --output /tmp/fhir-pipes-rs-incremental-out
```

`fhir-pipes-rs` automatically deduplicates records by `(resourceType, id)` keeping the latest `meta.lastUpdated` timestamp and removes any resources tagged with `ActionType.REMOVE`.

---

## 7. Part 5: Scaling Up to Larger Synthea FHIR R4 Datasets (100K–1M+ Resources)

To benchmark `fhir-pipes-rs` and DuckDB on a larger general-medicine dataset with **24 FHIR resource types** (including `Claim`, `ExplanationOfBenefit`, `Immunization`, `ImagingStudy`, `Device`, and `SupplyDelivery`):

### Step 7.1: Download MITRE's Pre-Generated Synthea FHIR R4 Archive (or Generate Custom Cohorts)
```bash
# Option A: Download MITRE's pre-generated 111-bundle FHIR R4 archive (~119,981 FHIR resources)
mkdir -p /tmp/synthea-large/fhir
curl -L -o /tmp/synthea-large/synthea_sample_data_fhir_latest.zip \
  https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_fhir_latest.zip
unzip -q /tmp/synthea-large/synthea_sample_data_fhir_latest.zip -d /tmp/synthea-large/fhir

# Option B: Or generate 1,000+ custom patients locally using the official Synthea Java JAR
curl -L -o /tmp/synthea-with-dependencies.jar \
  https://github.com/synthetichealth/synthea/releases/download/v4.0.0/synthea-with-dependencies.jar
java -jar /tmp/synthea-with-dependencies.jar -p 1000 \
  --exporter.fhir.export=true \
  --exporter.baseDirectory=/tmp/synthea-1k
```

### Step 7.2: Run `fhir-pipes-rs` on the 119,981-Resource Dataset (~3.7 Seconds)
```bash
./target/release/fhir-pipes-rs etl \
  --input /tmp/synthea-large/fhir \
  --views-dir ../../docker/config/views \
  --output /tmp/synthea-large-out
```
Output summary:
- **119,981 FHIR R4 resources** extracted from 111 Bundle files in **548.9 ms**
- **24 Nested Parquet-on-FHIR tables** (`30.60 MB` Snappy-compressed) written in **1.81 s**
- **12 SQL-on-FHIR v2 Materialized Views** (`132,941 flattened rows`, `4.31 MB`) written in **866.6 ms**
- **Total wall time**: **3.70 seconds** (`32,414 resources/sec`)

### Step 7.3: Query Both Flattened Views & Un-modeled Nested Resources (`Claim`) in DuckDB
Start the Web SQL Studio pointing at `/tmp/synthea-large-out`:
```bash
python3 studio/duckdb_studio.py /tmp/synthea-large-out --port 8765
```
Even though `docker/config/views/` does not define a flat `ViewDefinition` for `Claim` or `ExplanationOfBenefit`, you can immediately query the nested `Claim.parquet` table using DuckDB struct dot-notation:
```sql
SELECT
    status,
    use AS claim_use,
    COUNT(*) AS total_claims,
    ROUND(SUM(total.value), 2) AS total_billed_usd,
    ROUND(AVG(total.value), 2) AS avg_claim_usd
FROM Claim
GROUP BY status, use;
-- Result: 9,493 active claims totaling $18,334,991.95 ($1,931.42 average)
```
