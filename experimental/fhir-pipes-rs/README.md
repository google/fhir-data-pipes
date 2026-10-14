# `fhir-pipes-rs` (Experimental Rust + Arrow PoC for `fhir-data-pipes`)

> **Status:** Experimental Proof-of-Concept & Community RFC  
> **Detailed Architectural Analysis:** See [**`ARCHITECTURE_RFC.md`**](./ARCHITECTURE_RFC.md)

`fhir-pipes-rs` is an experimental, single-binary Rust alternative to the Apache Beam / Apache Flink / HAPI FHIR / Cerner Bunsen / Avro pipeline in `fhir-data-pipes`.

It explores whether the core data engineering problem solved by `fhir-data-pipes`—transforming FHIR resources into **lossless Nested Parquet-on-FHIR (SQL-on-FHIR v1)** and **flattened Materialized Views (HL7 SQL-on-FHIR v2 `ViewDefinition`)** with **incremental deduplication**—can be achieved with dramatically simpler operations, lower memory footprint, and 10x–50x faster single-node throughput using **Rust + Apache Arrow (`arrow-rs`) + Apache Parquet (`parquet`) + Rayon**.

---

## 1. Quickstart (Test It in 60 Seconds)

From this directory (`experimental/fhir-pipes-rs`), you can build and run the pipeline directly against the repository's existing **81 Synthea HIV FHIR Bundle files (46,497 resources)** and **12 official `ViewDefinition` configs**:

### Build & Run Unit Tests
```bash
cd experimental/fhir-pipes-rs
cargo test
cargo build --release
```

### Run End-to-End ETL on `synthea-hiv` Sample Data
```bash
./target/release/fhir-pipes-rs etl \
  --input ../../synthea-hiv/sample_data_segments \
  --views-dir ../../docker/config/views \
  --output /tmp/fhir-pipes-rs-out
```

### Example Output (~0.9–1.2 seconds total wall time)
```text
[1/4] Extracted 46497 FHIR resources from 81 files in 100.4ms
[2/4] Deduplicated & merged resources in 106.2ms: 46497 active (0 duplicates merged, 0 deleted tombstones removed)
[3/4] Wrote 17 Nested Parquet-on-FHIR tables (12.93 MB total) in 424.1ms:
      - AllergyIntolerance            16 rows |  9 top-level Arrow fields |     10.2 KB
      - CarePlan                     120 rows | 11 top-level Arrow fields |     39.4 KB
      - CareTeam                     120 rows |  9 top-level Arrow fields |     36.5 KB
      - Claim                       4028 rows | 16 top-level Arrow fields |   1135.4 KB
      - Condition                     84 rows | 10 top-level Arrow fields |     20.7 KB
      - DiagnosticReport            4006 rows | 10 top-level Arrow fields |    520.9 KB
      - DocumentReference           4006 rows | 11 top-level Arrow fields |   1713.2 KB
      - Encounter                   4006 rows | 12 top-level Arrow fields |    434.2 KB
      - ExplanationOfBenefit        4028 rows | 19 top-level Arrow fields |   2768.2 KB
      - Immunization                4103 rows |  9 top-level Arrow fields |    271.5 KB
      - Location                    1191 rows | 10 top-level Arrow fields |    115.6 KB
      - MedicationRequest             22 rows | 10 top-level Arrow fields |     13.0 KB
      - Observation                17279 rows | 10 top-level Arrow fields |    976.4 KB
      - Organization                1191 rows | 13 top-level Arrow fields |    144.8 KB
      - Patient                       79 rows | 20 top-level Arrow fields |     44.2 KB
      - Practitioner                1191 rows | 10 top-level Arrow fields |    123.9 KB
      - PractitionerRole            1191 rows |  9 top-level Arrow fields |    231.4 KB
      - Procedure                     33 rows | 10 top-level Arrow fields |     19.4 KB
      - Provenance                    79 rows |  6 top-level Arrow fields |   4778.5 KB
[4/4] Materialized 12 SQL-on-FHIR v2 ViewDefinitions (46843 flattened rows, 1.91 MB total) in 273.8ms:
      - condition_flat           (Condition         )       84 rows | 10 cols |     15.2 KB
      - diagnostic_report_flat   (DiagnosticReport  )    16024 rows | 14 cols |    337.6 KB
      - encounter_flat           (Encounter         )     4006 rows | 12 cols |    250.1 KB
      - immunization_flat        (Immunization      )     4103 rows | 13 cols |    280.7 KB
      - location_flat            (Location          )     1191 rows |  9 cols |     66.9 KB
      - medication_request_flat  (MedicationRequest )       22 rows | 15 cols |      7.0 KB
      - observation_flat         (Observation       )    17279 rows | 15 cols |    792.2 KB
      - organization_flat        (Organization      )     1191 rows |  9 cols |     62.1 KB
      - patient_flat             (Patient           )      528 rows | 12 cols |     16.7 KB
      - practitioner_flat        (Practitioner      )     1191 rows | 10 cols |     67.4 KB
      - practitioner_role_flat   (PractitionerRole  )     1191 rows | 12 cols |     47.8 KB
      - procedure_flat           (Procedure         )       33 rows | 12 cols |      8.2 KB

Done! Total end-to-end pipeline time: 900.93ms (throughput: 51610 resources/sec)
```

---

## 2. Querying the Output with DuckDB (No Spark Thrift Server Needed)

Every `etl` run automatically generates `/tmp/fhir-pipes-rs-out/init_duckdb.sql`, which registers all **12 flattened Materialized Views** and all **17 nested Parquet-on-FHIR tables** as zero-copy DuckDB views.

```bash
# Start interactive DuckDB CLI with all flat & nested views registered
duckdb -init /tmp/fhir-pipes-rs-out/init_duckdb.sql

# Or launch DuckDB's local web UI notebook
duckdb -ui -init /tmp/fhir-pipes-rs-out/init_duckdb.sql
```

### Example 1: Querying Flattened Materialized Views (SQL-on-FHIR v2)
```sql
WITH patients AS (
    SELECT DISTINCT id, given, family, gender, CAST(birth_date AS DATE) AS birth_date
    FROM patient_flat
)
SELECT
    p.id AS patient_id,
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

### Example 2: Querying Nested Parquet-on-FHIR Tables (SQL-on-FHIR v1)
When an analyst needs a FHIR field that was not included in a flat `ViewDefinition`, they can query the nested Parquet tables directly using struct dot-notation (`subject.patientId`, `value.quantity.value`) and `UNNEST()`:

```sql
SELECT
    id,
    subject.patientId AS patient_id,
    encounter.encounterId AS encounter_id,
    effective.dateTime AS effective_time,
    code.text AS code_text,
    value.quantity.value AS numeric_value,
    value.quantity.unit AS unit
FROM Observation
WHERE value.quantity.value IS NOT NULL
LIMIT 15;
```

### Example 3: Built-in CLI Parquet Inspector (No External Tools Needed)
```bash
./target/release/fhir-pipes-rs inspect \
  --file /tmp/fhir-pipes-rs-out/materialized_views/patient_flat.parquet \
  --limit 3
```

---

## 3. What This Experiment Does Today vs. What Is Outstanding

To help guide community evaluation, below is a transparent breakdown of what is implemented in this PoC today versus what remains to reach full parity with the Java/Beam `fhir-data-pipes` codebase.

### A. Implemented in `fhir-pipes-rs` Today
| Area | Implemented Capability | Code Location |
| :--- | :--- | :--- |
| **Ingestion (Files)** | Parallel ingestion of FHIR `Bundle` JSON and NDJSON files/directories via `rayon`. | [`src/extractor.rs`](./src/extractor.rs) |
| **Intra-Bundle Reference Stitching** | Resolves `urn:uuid:...` references inside transaction/collection Bundles to `ResourceType/id` (matching `ReadJsonFn.updateResolvedRefIds`). | [`src/extractor.rs`](./src/extractor.rs) |
| **Incremental Deduplication & Tombstones** | Deduplicates resources across base + `--incremental-input` runs by `(resourceType, id)` using `meta.lastUpdated`, and filters deleted resources marked with `ActionType.REMOVE`. | [`src/merger.rs`](./src/merger.rs) |
| **Nested Parquet-on-FHIR (SQL-on-FHIR v1)** | Lossless nested Arrow/Parquet writer with FHIR choice-type splitting (`valueQuantity` $\rightarrow$ `value.quantity`), typed `Reference` ID enrichment (`patientId`, `encounterId`, etc.), and US-Core extension promotion (`birthsex`, `race`, `ethnicity`). | [`src/parquet_on_fhir.rs`](./src/parquet_on_fhir.rs) |
| **SQL-on-FHIR v2 Materialized Views (`ViewDefinition`)** | Native evaluator supporting `select`, nested `select`, `forEach`, `forEachOrNull`, `unionAll`, `where`, `%constant` substitution, cross-joins, Snappy Parquet output, and SQL `CREATE TABLE` DDL (`schema.sql`). Successfully runs all 12 official views in `docker/config/views/`. | [`src/view_definition.rs`](./src/view_definition.rs) |
| **Zero-Daemon DuckDB Integration** | Auto-generates `init_duckdb.sql` to register all flat and nested Parquet tables as DuckDB views, plus `fhir-pipes-rs inspect` CLI command. | [`src/main.rs`](./src/main.rs) |

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

## 4. Questions for Community Feedback

We welcome feedback from `fhir-data-pipes` users, implementers, and maintainers on the following questions:

1. **CLI-First vs. Embedded Scheduler**: Does a fast, single-binary CLI (triggered via cron, Airflow, Dagster, GitHub Actions, or Docker) better fit your deployment model than the current embedded Spring Boot + Flink cluster controller?
2. **DuckDB / Parquet vs. Spark Thrift Server**: For sites currently running the Docker Compose stack (Flink + Spark Thrift Server + Hive Metastore), would querying Parquet via DuckDB or PostgreSQL materialization simplify your infrastructure?
3. **Prioritizing Next Milestones**: Which of the outstanding items above is highest priority for your deployment?
   - [ ] Async FHIR REST Search + OpenMRS / HAPI incremental sync (`_lastUpdated`)
   - [ ] FHIR Bulk Data `$export` client
   - [ ] Direct PostgreSQL sink (`COPY` / upsert)
   - [ ] Passing 100% of the HL7 SQL-on-FHIR v2 compliance test suite
   - [ ] Compile-time FHIR R4/R5 `StructureDefinition` Arrow schema generator
