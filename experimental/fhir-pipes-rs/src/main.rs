mod extractor;
mod merger;
mod parquet_on_fhir;
mod view_definition;

use anyhow::Result;
use clap::{Parser, Subcommand};
use extractor::{collect_input_files, group_by_resource_type, load_resources_parallel};
use merger::deduplicate_and_merge_resources;
use parquet_on_fhir::write_nested_parquet_for_resource_type;
use rayon::prelude::*;
use std::path::PathBuf;
use std::time::Instant;
#[cfg(test)]
use view_definition::CellValue;
use view_definition::ViewDefinition;

#[derive(Parser)]
#[command(
    name = "fhir-pipes-rs",
    version = "0.1.0",
    about = "Fast, single-binary Rust alternative to OHS fhir-data-pipes (FHIR -> Nested Parquet & SQL-on-FHIR v2 Materialized Views)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run full or incremental ETL: FHIR JSON/NDJSON -> Nested Parquet-on-FHIR + Materialized Views
    #[command(visible_alias = "run")]
    Etl {
        /// Input file or directory containing FHIR Bundle JSON or NDJSON files
        #[arg(short, long)]
        input: PathBuf,

        /// Directory containing SQL-on-FHIR v2 ViewDefinition JSON files
        #[arg(short, long, visible_alias = "views")]
        views_dir: Option<PathBuf>,

        /// Output directory for generated Parquet files and SQL schema
        #[arg(short, long)]
        output: PathBuf,

        /// Optional incremental delta input file or directory to merge with base input
        #[arg(long, visible_alias = "incremental")]
        incremental_input: Option<PathBuf>,
    },
    /// Inspect schema and sample rows of a generated Parquet table
    Inspect {
        /// Path to a .parquet file
        #[arg(short, long)]
        file: PathBuf,

        /// Number of sample rows to print
        #[arg(short, long, default_value_t = 5)]
        limit: usize,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Etl {
            input,
            views_dir,
            output,
            incremental_input,
        } => run_etl(&input, views_dir.as_deref(), &output, incremental_input.as_deref()),
        Commands::Inspect { file, limit } => inspect_parquet(&file, limit),
    }
}

fn run_etl(
    input_path: &std::path::Path,
    views_dir: Option<&std::path::Path>,
    output_dir: &std::path::Path,
    incremental_path: Option<&std::path::Path>,
) -> Result<()> {
    let total_start = Instant::now();

    // 1. Discover & Extract FHIR resources in parallel
    let extract_start = Instant::now();
    let files = collect_input_files(input_path)?;
    let base_resources = load_resources_parallel(&files)?;
    let extract_elapsed = extract_start.elapsed();

    println!(
        "[1/4] Extracted {} FHIR resources from {} files in {:.2?}",
        base_resources.len(),
        files.len(),
        extract_elapsed
    );

    // 2. Deduplicate & Merge (with optional incremental delta)
    let merge_start = Instant::now();
    let inc_resources = if let Some(inc_path) = incremental_path {
        let inc_files = collect_input_files(inc_path)?;
        load_resources_parallel(&inc_files)?
    } else {
        Vec::new()
    };
    let (resources, merge_stats) = deduplicate_and_merge_resources(base_resources, inc_resources);
    let merge_elapsed = merge_start.elapsed();
    println!(
        "[2/4] Deduplicated & merged resources in {:.2?}: {} active ({} duplicates merged, {} deleted tombstones removed)",
        merge_elapsed,
        merge_stats.final_active_records,
        merge_stats.duplicate_ids_merged,
        merge_stats.tombstones_removed
    );

    // 3. Write Nested Parquet-on-FHIR (SQL-on-FHIR v1) in parallel per resourceType
    let parquet_start = Instant::now();
    let total_active_resources = resources.len();
    let grouped = group_by_resource_type(resources);
    let mut resource_types: Vec<_> = grouped.keys().cloned().collect();
    resource_types.sort();

    let parquet_base_dir = output_dir.join("parquet_on_fhir");
    let nested_results: Result<Vec<(String, usize, usize, u64)>> = resource_types
        .par_iter()
        .map(|rtype| {
            let res_list = &grouped[rtype];
            let out_file = parquet_base_dir
                .join(rtype)
                .join(format!("{}.parquet", rtype));
            let (rows, cols) = write_nested_parquet_for_resource_type(res_list, &out_file)?;
            let size_bytes = std::fs::metadata(&out_file).map(|m| m.len()).unwrap_or(0);
            Ok((rtype.clone(), rows, cols, size_bytes))
        })
        .collect();
    let nested_summary = nested_results?;
    let parquet_elapsed = parquet_start.elapsed();

    let total_nested_bytes: u64 = nested_summary.iter().map(|(_, _, _, b)| *b).sum();
    println!(
        "[3/4] Wrote {} Nested Parquet-on-FHIR tables ({:.2} MB total) in {:.2?}:",
        nested_summary.len(),
        total_nested_bytes as f64 / (1024.0 * 1024.0),
        parquet_elapsed
    );
    for (rtype, rows, cols, bytes) in &nested_summary {
        println!(
            "      - {:<24} {:>7} rows | {:>2} top-level Arrow fields | {:>8.1} KB",
            rtype,
            rows,
            cols,
            *bytes as f64 / 1024.0
        );
    }

    // 4. Evaluate SQL-on-FHIR v2 ViewDefinitions & Write Materialized View Parquet + SQL DDL
    if let Some(vdir) = views_dir {
        let views_start = Instant::now();
        let views = ViewDefinition::load_all_from_dir(vdir)?;
        let views_out_dir = output_dir.join("materialized_views");
        std::fs::create_dir_all(&views_out_dir)?;

        // Write combined SQL DDL for Postgres / DuckDB / SQLite sinks
        let ddl_combined: Vec<String> = views.iter().map(|v| v.to_sql_ddl()).collect();
        std::fs::write(views_out_dir.join("schema.sql"), ddl_combined.join("\n\n"))?;

        let view_results: Result<Vec<(String, String, usize, usize, u64)>> = views
            .par_iter()
            .map(|vdef| {
                let res_slice = grouped
                    .get(&vdef.resource)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let flat_table = vdef.apply_to_resources(res_slice)?;
                let out_file = views_out_dir.join(format!("{}.parquet", vdef.name));
                let num_rows = flat_table.write_parquet(&out_file)?;
                let size_bytes = std::fs::metadata(&out_file).map(|m| m.len()).unwrap_or(0);
                Ok((
                    vdef.name.clone(),
                    vdef.resource.clone(),
                    num_rows,
                    flat_table.columns.len(),
                    size_bytes,
                ))
            })
            .collect();

        let view_summary = view_results?;
        let views_elapsed = views_start.elapsed();
        let total_view_rows: usize = view_summary.iter().map(|(_, _, r, _, _)| *r).sum();
        let total_view_bytes: u64 = view_summary.iter().map(|(_, _, _, _, b)| *b).sum();

        println!(
            "[4/4] Materialized {} SQL-on-FHIR v2 ViewDefinitions ({} flattened rows, {:.2} MB total) in {:.2?}:",
            view_summary.len(),
            total_view_rows,
            total_view_bytes as f64 / (1024.0 * 1024.0),
            views_elapsed
        );
        for (vname, rtype, rows, cols, bytes) in &view_summary {
            println!(
                "      - {:<24} ({:<18}) {:>8} rows | {:>2} cols | {:>8.1} KB",
                vname,
                rtype,
                rows,
                cols,
                *bytes as f64 / 1024.0
            );
        }

        // Generate DuckDB initialization script for zero-copy querying of both Nested & Flat Parquet
        let mut duckdb_sql = String::from(
            "-- Auto-generated by fhir-pipes-rs\n\
             -- Usage: duckdb -init init_duckdb.sql\n\n\
             -- 1. SQL-on-FHIR v2 Flattened Materialized Views\n",
        );
        for (vname, _, _, _, _) in &view_summary {
            let p = views_out_dir.join(format!("{}.parquet", vname));
            duckdb_sql.push_str(&format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}');\n",
                vname,
                p.display()
            ));
        }
        duckdb_sql.push_str("\n-- 2. Nested Parquet-on-FHIR (SQL-on-FHIR v1) Resource Tables\n");
        for (rtype, _, _, _) in &nested_summary {
            let p = parquet_base_dir
                .join(rtype)
                .join(format!("{}.parquet", rtype));
            duckdb_sql.push_str(&format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}');\n",
                rtype,
                p.display()
            ));
        }
        std::fs::write(output_dir.join("init_duckdb.sql"), duckdb_sql)?;
    }

    println!(
        "\nDone! Total end-to-end pipeline time: {:.2?} (throughput: {:.0} resources/sec)",
        total_start.elapsed(),
        total_active_resources as f64 / total_start.elapsed().as_secs_f64()
    );

    Ok(())
}

fn inspect_parquet(file_path: &std::path::Path, limit: usize) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let total_rows = builder.metadata().file_metadata().num_rows();
    let schema = builder.schema().clone();

    println!(
        "Parquet File: {} ({} total rows, {} columns)",
        file_path.display(),
        total_rows,
        schema.fields().len()
    );
    println!("Schema:");
    for field in schema.fields() {
        println!("  - {:<26} : {:?}", field.name(), field.data_type());
    }

    let mut reader = builder.with_batch_size(limit.max(1)).build()?;
    if let Some(batch) = reader.next() {
        let batch = batch?;
        let slice = batch.slice(0, limit.min(batch.num_rows()));
        let mut buf = Vec::new();
        {
            let mut writer = arrow_json::LineDelimitedWriter::new(&mut buf);
            writer.write(&slice)?;
            writer.finish()?;
        }
        println!("\nSample {} rows (JSON representation):", slice.num_rows());
        print!("{}", String::from_utf8_lossy(&buf));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_view_definition_evaluation_and_cross_join() -> Result<()> {
        let vdef_json = json!({
            "name": "patient_flat",
            "resource": "Patient",
            "select": [
                {
                    "column": [
                        { "path": "getResourceKey()", "name": "id" },
                        { "path": "gender", "name": "gender" },
                        { "path": "deceased.ofType(boolean)", "name": "is_deceased", "type": "boolean" },
                        { "path": "managingOrganization.getReferenceKey(Organization)", "name": "org_id" }
                    ]
                },
                {
                    "forEachOrNull": "name",
                    "column": [
                        { "path": "family", "name": "family" }
                    ],
                    "select": [
                        {
                            "forEachOrNull": "given",
                            "column": [
                                { "path": "$this", "name": "given" }
                            ]
                        }
                    ]
                }
            ]
        });

        let mut vdef: ViewDefinition = serde_json::from_value(vdef_json)?;
        vdef.validate_and_resolve_constants()?;

        let patient = json!({
            "resourceType": "Patient",
            "id": "pat-1",
            "gender": "female",
            "deceasedBoolean": false,
            "managingOrganization": { "reference": "Organization/org-99" },
            "name": [
                { "family": "Smith", "given": ["Alice", "Marie"] }
            ]
        });

        let table = vdef.apply_to_resources(&[patient])?;
        assert_eq!(table.columns.len(), 6);
        assert_eq!(table.rows.len(), 2); // 2 given names -> 2 cross-joined flat rows
        assert_eq!(table.rows[0][0], CellValue::Str("pat-1".to_string()));
        assert_eq!(table.rows[0][2], CellValue::Bool(false));
        assert_eq!(table.rows[0][3], CellValue::Str("org-99".to_string()));
        assert_eq!(table.rows[0][4], CellValue::Str("Smith".to_string()));
        assert_eq!(table.rows[0][5], CellValue::Str("Alice".to_string()));
        assert_eq!(table.rows[1][5], CellValue::Str("Marie".to_string()));
        Ok(())
    }

    #[test]
    fn test_parquet_on_fhir_transformation() {
        let obs = json!({
            "resourceType": "Observation",
            "id": "Observation/obs-1",
            "subject": { "reference": "Patient/pat-1" },
            "effectiveDateTime": "2024-01-15T10:00:00Z",
            "valueQuantity": {
                "value": 120.5,
                "unit": "mmHg"
            }
        });

        let transformed = parquet_on_fhir::transform_resource_for_parquet_on_fhir(&obs);
        assert_eq!(transformed["id"], "obs-1");
        assert_eq!(transformed["subject"]["patientId"], "pat-1");
        assert_eq!(transformed["subject"]["PatientId"], "pat-1");
        assert_eq!(transformed["effective"]["dateTime"], "2024-01-15T10:00:00Z");
        assert_eq!(transformed["value"]["quantity"]["value"], 120.5);
    }

    #[test]
    fn test_incremental_merge_and_tombstone_deletion() {
        let base = vec![
            json!({
                "resourceType": "Patient",
                "id": "p1",
                "gender": "male",
                "meta": { "lastUpdated": "2024-01-01T00:00:00Z" }
            }),
            json!({
                "resourceType": "Patient",
                "id": "p2",
                "gender": "female",
                "meta": { "lastUpdated": "2024-01-01T00:00:00Z" }
            }),
        ];
        let incremental = vec![
            // Update p1
            json!({
                "resourceType": "Patient",
                "id": "p1",
                "gender": "other",
                "meta": { "lastUpdated": "2024-02-01T00:00:00Z" }
            }),
            // Delete p2 via ActionType.REMOVE tag
            json!({
                "resourceType": "Patient",
                "id": "p2",
                "meta": {
                    "lastUpdated": "2024-02-01T00:00:00Z",
                    "tag": [{ "system": "http://terminology.hl7.org/CodeSystem/action-type", "code": "remove" }]
                }
            }),
        ];

        let (merged, stats) = deduplicate_and_merge_resources(base, incremental);
        assert_eq!(stats.duplicate_ids_merged, 2);
        assert_eq!(stats.tombstones_removed, 1);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["id"], "p1");
        assert_eq!(merged[0]["gender"], "other");
    }
}
