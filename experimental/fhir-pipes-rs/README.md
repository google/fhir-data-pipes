# `fhir-pipes-rs` (Experimental Rust + Arrow PoC for `fhir-data-pipes`)

> **Status:** Experimental Proof-of-Concept & Community RFC  
> - **Detailed Architectural Analysis:** [**`ARCHITECTURE_RFC.md`**](./ARCHITECTURE_RFC.md)  
> - **Mobile / KMP & On-Device SLM RFC:** [**`MOBILE_KMP_RFC.md`**](./MOBILE_KMP_RFC.md)  
> - **Step-by-Step Synthea-HIV + DuckDB Studio Tutorial:** [**`TUTORIAL_SYNTHEA_HIV.md`**](./TUTORIAL_SYNTHEA_HIV.md) (also in MkDocs at [`doc/docs/tutorials/rust_synthea_hiv_duckdb.md`](../../doc/docs/tutorials/rust_synthea_hiv_duckdb.md))

`fhir-pipes-rs` is an experimental, single-binary Rust alternative to the Apache Beam / Apache Flink / HAPI FHIR / Cerner Bunsen / Avro pipeline in `fhir-data-pipes`.

It explores whether the core data engineering problem solved by `fhir-data-pipes`—transforming FHIR resources into **lossless Nested Parquet-on-FHIR (SQL-on-FHIR v1)** and **flattened Materialized Views (HL7 SQL-on-FHIR v2 `ViewDefinition`)** with **incremental deduplication**—can be achieved with dramatically simpler operations, lower memory footprint, and 10x–50x faster single-node throughput using **Rust + Apache Arrow (`arrow-rs`) + Apache Parquet (`parquet`) + Rayon**.

---

## 1. Quickstart: Run `synthea-hiv` ETL & Launch the Local Web SQL Studio

From this directory (`experimental/fhir-pipes-rs`), you can build the binary, run the full ETL pipeline against the repository's **81 Synthea-HIV FHIR Bundle files (46,497 resources)** and **12 official `ViewDefinition` configs**, and open the self-contained **Local DuckDB Web SQL Studio & Rebuilt Superset Dashboard** in under 60 seconds:

### Step 1: Build & Run Unit Tests
```bash
cd experimental/fhir-pipes-rs
cargo test
cargo build --release
```

### Step 2: Run ETL on `synthea-hiv` Sample Data (~0.9 Seconds)
```bash
./target/release/fhir-pipes-rs etl \
  --input ../../synthea-hiv/sample_data_segments \
  --views-dir ../../docker/config/views \
  --output /tmp/fhir-pipes-rs-out
```

### Step 3: Launch the Local DuckDB Web SQL Studio & Sample Dashboard
```bash
pip3 install duckdb
python3 studio/duckdb_studio.py /tmp/fhir-pipes-rs-out --port 8765
```

Open in your browser:
- **Local DuckDB Web SQL Studio**: [**http://localhost:8765/**](http://localhost:8765/)
  - Browse all **12 SQL-on-FHIR v2 Materialized Views** and **17 Nested Parquet-on-FHIR tables** in the left sidebar with row counts and schema inspection.
  - Click any of the **5 pre-loaded Superset & PEPFAR HIV tutorial queries** or run custom DuckDB SQL (`Cmd+Enter` / `Ctrl+Enter`) and export to CSV.
- **Interactive Sample Dashboard (Rebuilt from `add_dashboard.md` & `queries_large.ipynb`)**: [**http://localhost:8765/dashboard**](http://localhost:8765/dashboard)
  - Rebuilds all 3 charts from the [Apache Superset Tutorial (`doc/docs/tutorials/add_dashboard.md`)](../../doc/docs/tutorials/add_dashboard.md) (**Patient Count Big Number**, **Interactive Gender Split Donut/Pie Chart**, and **HIV Treatment Initiations `1255%`/`1256%` Bar Chart by Year**).
  - Adds the **PEPFAR `TX_PVLS` Viral Load (`< 400,000` copies/mL) by Year & Gender** chart from [`query/queries_large.ipynb`](../../query/queries_large.ipynb) and an interactive **3-Stage HIV/TB Care Cascade** (ART Plans, ARV Regimens, and TB Co-Infection Regimens) with **Patient 360° Longitudinal Viral Load** drill-downs.

> **Note on DuckDB's Built-in `ui` Extension (`CALL start_ui()`) vs. `studio/duckdb_studio.py`:**  
> DuckDB's `ui` extension starts a reverse proxy on `localhost:4213` that fetches its web application assets live from `https://ui.duckdb.org`, which fails in offline clinics, air-gapped hospital servers, or corporate networks with TLS inspection. [`studio/duckdb_studio.py`](./studio/duckdb_studio.py) is **100% self-contained on `localhost`** and FHIR-warehouse aware out of the box.

---

## 2. Examples: Running Synthea Data Through `fhir-pipes-rs` & Analyzing Results

For a complete step-by-step walkthrough, see [**`TUTORIAL_SYNTHEA_HIV.md`**](./TUTORIAL_SYNTHEA_HIV.md). Below are two end-to-end examples showing both the built-in `synthea-hiv` dataset and a larger 119,981-resource MITRE Synthea FHIR R4 dataset.

### Example A: Analyzing the Repository's `synthea-hiv` Dataset (46,497 Resources in 0.90s)

Running `./target/release/fhir-pipes-rs etl --input ../../synthea-hiv/sample_data_segments --views-dir ../../docker/config/views --output /tmp/fhir-pipes-rs-out` produces **17 Nested Parquet-on-FHIR tables** (`12.93 MB`) and **12 Materialized View Parquet tables** (`46,843 flattened rows`, `1.91 MB`) plus `/tmp/fhir-pipes-rs-out/init_duckdb.sql`.

You can analyze the output via the Web SQL Studio (`http://localhost:8765/`), the `duckdb` CLI (`duckdb -init /tmp/fhir-pipes-rs-out/init_duckdb.sql`), or Python:

#### 1. HIV Treatment Initiations by Year (Superset Tutorial Chart 3 — `0.8 ms` in DuckDB)
```sql
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
ORDER BY 1 ASC;
```

#### 2. PEPFAR `TX_PVLS` Viral Load Suppression (< 400,000 copies/mL) by Year & Gender (`1.4 ms` in DuckDB)
```sql
WITH p AS (
    SELECT DISTINCT id, gender FROM patient_flat
)
SELECT
    CAST(EXTRACT(YEAR FROM TRY_CAST(o.obs_date AS TIMESTAMP)) AS INT) AS obs_year,
    p.gender,
    COUNT(DISTINCT o.patient_id) AS patients_below_400k,
    ROUND(AVG(o.val_quantity), 1) AS mean_viral_load
FROM observation_flat o
JOIN p ON o.patient_id = p.id
WHERE o.code_code LIKE '856%'
  AND o.code_sys = 'http://loinc.org'
  AND o.val_quantity < 400000
GROUP BY 1, 2
ORDER BY obs_year DESC, gender;
```

---

### Example B: Downloading & Analyzing a Larger Synthea FHIR R4 Dataset (119,981 Resources in 3.70s)

You can download MITRE's pre-generated Synthea FHIR R4 archive (or generate custom 1,000–100,000+ patient populations with `synthea-with-dependencies.jar`) and run `fhir-pipes-rs` on it:

```bash
# 1. Download MITRE's 111-bundle Synthea FHIR R4 archive (~119,981 resources across 24 resource types)
mkdir -p /tmp/synthea-large/fhir
curl -L -o /tmp/synthea-large/synthea_sample_data_fhir_latest.zip \
  https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_fhir_latest.zip
unzip -q /tmp/synthea-large/synthea_sample_data_fhir_latest.zip -d /tmp/synthea-large/fhir

# 2. Run fhir-pipes-rs ETL (~3.70 seconds -> 32,414 resources/sec)
./target/release/fhir-pipes-rs etl \
  --input /tmp/synthea-large/fhir \
  --views-dir ../../docker/config/views \
  --output /tmp/synthea-large-out

# 3. Launch the Local DuckDB Web SQL Studio on the 119,981-resource warehouse
python3 studio/duckdb_studio.py /tmp/synthea-large-out --port 8765
```

#### Querying Both Flat Materialized Views (v2) and Un-modeled Nested Parquet Tables (v1)
1. **Multi-Table Cohort Join across `patient_flat`, `encounter_flat`, and `observation_flat` (SQL-on-FHIR v2)**:
   ```sql
   WITH patients AS (
       SELECT DISTINCT id, given, family, gender, CAST(birth_date AS DATE) AS birth_date
       FROM patient_flat
   )
   SELECT
       p.given || ' ' || p.family AS patient_name,
       p.gender,
       e.type_display AS encounter_type,
       o.code_display AS observation,
       COALESCE(CAST(o.val_quantity AS VARCHAR) || ' ' || COALESCE(o.val_quantity_unit, ''), o.value_display) AS result,
       o.obs_date
   FROM observation_flat o
   JOIN patients p ON o.patient_id = p.id
   LEFT JOIN encounter_flat e ON o.encounter_id = e.id
   ORDER BY o.obs_date DESC
   LIMIT 15;
   ```
2. **Querying Nested `Claim` Structs Directly (SQL-on-FHIR v1 Nested Parquet-on-FHIR)**:
   Even though `docker/config/views/` has no `ViewDefinition` for `Claim`, `fhir-pipes-rs` automatically writes `parquet_on_fhir/Claim/Claim.parquet` (`9,493` claims), which DuckDB can query directly using struct dot-notation:
   ```sql
   SELECT
       status,
       use AS claim_use,
       COUNT(*) AS total_claims,
       ROUND(SUM(total.value), 2) AS total_billed_usd,
       ROUND(AVG(total.value), 2) AS avg_claim_usd
   FROM Claim
   GROUP BY status, use;
   -- Result: 9,493 active claims totaling $18,334,991.95 ($1,931.42 avg)
   ```

---

### Example C: 1M+ FHIR Resource Scale Benchmark (`1,079,829` Resources / `3.3 GB` Raw JSON)

Across the three benchmark tiers (`46K`, `120K`, and `1.08M` FHIR R4 resources on Apple Silicon), `fhir-pipes-rs` scales linearly while compressing `3.3 GB` of raw FHIR JSON into a **`239.22 MB` Nested Parquet warehouse** (`13.8x` smaller) plus a **`33.02 MB` Flattened Materialized View warehouse** (`100x` smaller, `1,196,469` flattened rows):

| Dataset Tier | Input Files & Raw JSON Size | Active FHIR Resources | `[1/4]` Extract & Resolve URNs | `[2/4]` Deduplicate & Merge | `[3/4]` Write Nested Parquet (v1) | `[4/4]` Materialize 12 Views (v2) | Total End-to-End Time | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`synthea-hiv`** | 81 bundles (`191 MB`) | **46,497** | `0.11s` | `0.06s` | `0.38s` (17 tables, `12.9 MB`) | `0.09s` (`46,843` rows, `1.9 MB`) | **`0.64s`** | **72,651 res/sec** |
| **MITRE Synthea R4** | 111 bundles (`375 MB`) | **119,981** | `0.27s` | `0.38s` | `1.58s` (24 tables, `30.6 MB`) | `0.27s` (`132,941` rows, `4.3 MB`) | **`2.50s`** | **48,045 res/sec** |
| **1M+ Synthea R4 Cohort** | 999 bundles (`3.30 GB`) | **1,079,829** | `3.86s` | `6.57s` | `16.42s` (24 tables, `239.2 MB`) | `3.35s` (`1,196,469` rows, `33.0 MB`) | **`30.20s`** | **35,751 res/sec** |

#### DuckDB Analytical Query Latency on the 1,079,829-Resource Warehouse
| Query Type (over 1,079,829 FHIR Resources) | Target Parquet Table(s) | Rows Scanned | Median DuckDB Latency |
| :--- | :--- | :--- | :--- |
| **Warehouse Metadata & Row Count Check** | `Observation`, `observation_flat`, `diagnostic_report_flat`, `procedure_flat`, `Claim` | `1,547,541` | **`1.26 ms`** |
| **Nested Claim Financial Rollup (`SUM(total.value)` = `$165.01M`)** | `Claim.parquet` (Nested v1) | `85,437` | **`1.38 ms`** |
| **Top 10 LOINC Vital Signs & Lab Mean/SD (`GROUP BY code_code`)** | `observation_flat.parquet` (Flat v2) | `420,318` | **`14.50 ms`** |
| **3-Way Clinical Cohort Join (`patient_flat` $\bowtie$ `encounter_flat` $\bowtie$ `observation_flat`)** | 3 Flat v2 Parquet tables | `480,771` | **`38.95 ms`** |
| **Nested Struct/Array `LATERAL UNNEST(O.code.coding)` (`GROUP BY c.code`)** | `Observation.parquet` (Nested v1) | `416,745` | **`177.01 ms`** *(12.2x slower than `observation_flat`)* |

---

## 3. Built-in CLI Reference

```bash
# Full or incremental ETL
./target/release/fhir-pipes-rs etl \
  --input <FHIR_BUNDLES_OR_NDJSON_DIR> \
  --views-dir <VIEW_DEFINITIONS_DIR> \
  --output <OUTPUT_DIR> \
  [--incremental-input <DELTA_DIR>]

# Inspect schema and sample rows of any generated Parquet file without external tools
./target/release/fhir-pipes-rs inspect \
  --file /tmp/fhir-pipes-rs-out/materialized_views/patient_flat.parquet \
  --limit 3
```

---

## 4. What This Experiment Does Today vs. What Is Outstanding

To help guide community evaluation, below is a transparent breakdown of what is implemented in this PoC today versus what remains to reach full parity with the Java/Beam `fhir-data-pipes` codebase.

### A. Implemented in `fhir-pipes-rs` Today
| Area | Implemented Capability | Code Location |
| :--- | :--- | :--- |
| **Ingestion (Files)** | Parallel ingestion of FHIR `Bundle` JSON and NDJSON files/directories via `rayon`. | [`src/extractor.rs`](./src/extractor.rs) |
| **Intra-Bundle Reference Stitching** | Resolves `urn:uuid:...` references inside transaction/collection Bundles to `ResourceType/id` (matching `ReadJsonFn.updateResolvedRefIds`). | [`src/extractor.rs`](./src/extractor.rs) |
| **Incremental Deduplication & Tombstones** | Deduplicates resources across base + `--incremental-input` runs by `(resourceType, id)` using `meta.lastUpdated`, and filters deleted resources marked with `ActionType.REMOVE`. | [`src/merger.rs`](./src/merger.rs) |
| **Nested Parquet-on-FHIR (SQL-on-FHIR v1)** | Lossless nested Arrow/Parquet writer with FHIR choice-type splitting (`valueQuantity` $\rightarrow$ `value.quantity`), typed `Reference` ID enrichment (`patientId`, `encounterId`, etc.), and US-Core extension promotion (`birthsex`, `race`, `ethnicity`). | [`src/parquet_on_fhir.rs`](./src/parquet_on_fhir.rs) |
| **SQL-on-FHIR v2 Materialized Views (`ViewDefinition`)** | Native evaluator supporting `select`, nested `select`, `forEach`, `forEachOrNull`, `unionAll`, `where`, `%constant` substitution, cross-joins, Snappy Parquet output, and SQL `CREATE TABLE` DDL (`schema.sql`). Successfully runs all 12 official views in `docker/config/views/`. | [`src/view_definition.rs`](./src/view_definition.rs) |
| **Self-Contained Local DuckDB Studio & Superset Rebuild** | Auto-generates `init_duckdb.sql` and includes [`studio/duckdb_studio.py`](./studio/duckdb_studio.py) + [`studio/synthea_hiv_dashboard.html`](./studio/synthea_hiv_dashboard.html) for offline/local SQL exploration and interactive HIV/PEPFAR dashboards. | [`studio/`](./studio/) |

### B. Outstanding Capabilities (Not Yet Implemented in This PoC)
| Area | What Is Outstanding | Proposed Approach in Rust |
| :--- | :--- | :--- |
| **1. Network FHIR Sources (REST Search & Bulk `$export`)** | The PoC currently reads local `.json` / `.ndjson` files. It does not yet implement paginated HTTP FHIR Search (`_lastUpdated=gt...`, `_count`, `_summary=data`), Bulk Data `$export` job polling, or OAuth2 / Bearer / Basic auth token refresh (`FhirSearchUtil` / `BulkExportUtil`). | Add an async `reqwest` + `tokio` HTTP source module with retry/backoff, OAuth2 client-credentials flow, and a watermark state file (`last_sync.json`) for incremental `_lastUpdated` polling. |
| **2. Direct Relational DB Sinks (PostgreSQL / MySQL)** | The PoC generates `schema.sql` (`CREATE TABLE` DDL) and `.parquet` files for each `ViewDefinition`, whereas the Java pipeline also supports direct JDBC `INSERT`/`DELETE` into PostgreSQL (`JdbcWriter.java`). | Use `tokio-postgres` (`COPY ... FROM STDIN BINARY`) or DuckDB's `postgres_scanner` extension to bulk-upsert Arrow `RecordBatch`es directly into PostgreSQL at 10x JDBC batch speed. |
| **3. Full FHIR StructureDefinition Schema Registry** | `parquet_on_fhir.rs` infers the nested Arrow schema from the batch of resources combined with a curated table of FHIR choice-type prefixes and US-Core extensions, whereas Bunsen loads full HAPI `StructureDefinition` profiles. | Compile official HL7 FHIR R4/R5 `StructureDefinition` JSON bundles at build time (or startup) into static Arrow `Schema` definitions so every FHIR column is present even on empty batches and custom IG profiles are supported. |
| **4. Complete FHIRPath 2.0 Grammar** | `view_definition.rs` implements the core FHIRPath subset used by SQL-on-FHIR v2 (`getResourceKey()`, `getReferenceKey()`, `ofType()`, `where()`, `first()`, `$this`, boolean `and`/`or`/`not`, equality). Complex arithmetic, date math, string manipulation (`substring`, `matches`), or `extension('url')` functions are not yet implemented. | Integrate or expand a complete AST-based FHIRPath parser/evaluator in Rust to pass 100% of the official HL7 SQL-on-FHIR v2 test suite. |
| **5. Out-of-Core / Partitioned Lakehouse Merging** | `merger.rs` currently deduplicates resources in memory before writing Parquet, which easily handles ~1M–5M resources on a standard node, but very large multi-hundred-GB historical warehouses need out-of-core or partition-pruned merging. | Partition Parquet output by `resourceType` + hash/date bucket, or use `deltalake-rs` / DuckDB out-of-core `MERGE INTO` so incremental runs only rewrite affected Parquet partitions. |
| **6. Patient-Compartment Reference Backfilling** | When syncing in `Patient`-compartment mode, the Java pipeline searches for referenced `Organization` / `Practitioner` / `Location` resources outside the patient bundle. | Add a post-extraction reference resolution pass in the HTTP client to fetch missing shared administrative resources (`Organization`, `Practitioner`, `Location`). |
| **7. Cloud Object Store I/O (S3 / GCS / Azure)** | Currently reads/writes local POSIX filesystems. | Plug in the official Arrow `object_store` crate (`s3://`, `gs://`, `az://`) which integrates directly with `parquet::arrow::async_writer`. |

---

## 5. Questions for Community Feedback

We welcome feedback from `fhir-data-pipes` users, implementers, and maintainers on the following questions:

1. **CLI-First vs. Embedded Scheduler**: Does a fast, single-binary CLI (triggered via cron, Airflow, Dagster, GitHub Actions, or Docker) better fit your deployment model than the current embedded Spring Boot + Flink cluster controller?
2. **DuckDB / Parquet vs. Spark Thrift Server**: For sites currently running the Docker Compose stack (Flink + Spark Thrift Server + Hive Metastore), would querying Parquet via DuckDB or PostgreSQL materialization simplify your infrastructure?
3. **Prioritizing Next Milestones**: Which of the outstanding items above is highest priority for your deployment?
   - [ ] Async FHIR REST Search + OpenMRS / HAPI incremental sync (`_lastUpdated`)
   - [ ] FHIR Bulk Data `$export` client
   - [ ] Direct PostgreSQL sink (`COPY` / upsert)
   - [ ] Passing 100% of the HL7 SQL-on-FHIR v2 compliance test suite
   - [ ] Compile-time FHIR R4/R5 `StructureDefinition` Arrow schema generator
