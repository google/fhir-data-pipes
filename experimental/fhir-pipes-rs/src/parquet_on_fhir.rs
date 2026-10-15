use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::WriterProperties,
};
use serde_json::{Map, Number, Value};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Known FHIR choice-type prefixes (`field[x]`) that Bunsen / SQL-on-FHIR v1 converts
/// from `prefixType` (e.g. `valueQuantity`, `effectiveDateTime`) into nested structs
/// (`value: { quantity: ... }`, `effective: { dateTime: ... }`).
const CHOICE_PREFIXES: &[&str] = &[
    "value",
    "effective",
    "deceased",
    "multipleBirth",
    "onset",
    "abatement",
    "occurrence",
    "performed",
    "medication",
    "dosageInstruction",
    "born",
    "age",
    "timing",
    "product",
    "asNeeded",
    " Serviced",
    "allowed",
    "used",
];

/// Transforms a raw FHIR JSON resource into the "Parquet-on-FHIR" (SQL-on-FHIR v1) nested structure
/// documented in `doc/schema.md`, and writes each resource type to a nested Apache Parquet file.
pub fn transform_resource_for_parquet_on_fhir(res: &Value) -> Value {
    let Some(obj) = res.as_object() else {
        return res.clone();
    };

    let mut out = Map::with_capacity(obj.len() + 4);

    // 1. Promote US-Core / standard top-level extensions (`birthsex`, `race`, `ethnicity`, etc.)
    if let Some(Value::Array(exts)) = obj.get("extension") {
        promote_top_level_extensions(exts, &mut out);
    }

    // 2. Transform all resource fields recursively
    for (k, v) in obj {
        if k == "extension" || k == "modifierExtension" || k == "contained" {
            continue;
        }
        // Split FHIR choice types e.g. `valueQuantity` -> `value: { quantity: ... }`
        if let Some((prefix, type_suffix)) = split_choice_field(k) {
            let nested_key = lowercase_first(type_suffix);
            let transformed_val = transform_value(v);
            let entry = out
                .entry(prefix.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(choice_map) = entry {
                choice_map.insert(nested_key, transformed_val);
            }
        } else {
            out.insert(k.clone(), transform_value(v));
        }
    }

    // Strip any `ResourceType/` prefix from top-level `id`
    if let Some(Value::String(id_str)) = out.get_mut("id") {
        if let Some((_, right)) = id_str.rsplit_once('/') {
            *id_str = right.to_string();
        }
    }

    Value::Object(out)
}

fn transform_value(val: &Value) -> Value {
    match val {
        Value::Object(map) => {
            let mut out = Map::with_capacity(map.len() + 2);

            // Check if this object is a FHIR Reference (contains `"reference": "Type/id"`)
            if let Some(Value::String(ref_str)) = map.get("reference") {
                if !ref_str.contains('?') {
                    if let Some((prefix, id_part)) = ref_str.rsplit_once('/') {
                        let res_type = prefix.rsplit('/').next().unwrap_or(prefix);
                        if !res_type.is_empty() && !id_part.is_empty() {
                            // Add both camelCase (`patientId`) and PascalCase (`PatientId`) for SQL compatibility
                            let camel_col = format!("{}Id", lowercase_first(res_type));
                            let pascal_col = format!("{}Id", res_type);
                            out.insert(camel_col, Value::String(id_part.to_string()));
                            out.insert(pascal_col, Value::String(id_part.to_string()));
                        }
                    }
                }
            }

            for (k, v) in map {
                if k == "extension" || k == "modifierExtension" {
                    continue;
                }
                if let Some((prefix, type_suffix)) = split_choice_field(k) {
                    let nested_key = lowercase_first(type_suffix);
                    let transformed_val = transform_value(v);
                    let entry = out
                        .entry(prefix.to_string())
                        .or_insert_with(|| Value::Object(Map::new()));
                    if let Value::Object(choice_map) = entry {
                        choice_map.insert(nested_key, transformed_val);
                    }
                } else {
                    out.insert(k.clone(), transform_value(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(transform_value).collect()),
        Value::Number(n) => {
            // Normalize numbers to f64 so Arrow schema inference doesn't conflict when one resource
            // has `0` (integer) and another has `0.5` (float) for FHIR decimal fields.
            if let Some(f) = n.as_f64() {
                Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or_else(|| val.clone())
            } else {
                val.clone()
            }
        }
        other => other.clone(),
    }
}

fn promote_top_level_extensions(exts: &[Value], out: &mut Map<String, Value>) {
    for ext in exts {
        let Some(ext_obj) = ext.as_object() else {
            continue;
        };
        let Some(url) = ext_obj.get("url").and_then(|u| u.as_str()) else {
            continue;
        };
        let short_name = url.rsplit('/').next().unwrap_or(url);
        let field_name = short_name
            .strip_prefix("us-core-")
            .or_else(|| short_name.strip_prefix("patient-"))
            .unwrap_or(short_name);

        // Simple extension with `value[x]`
        let mut found_simple = false;
        for (k, v) in ext_obj {
            if k.starts_with("value") && k != "value" {
                out.insert(field_name.to_string(), transform_value(v));
                found_simple = true;
                break;
            }
        }
        if found_simple {
            continue;
        }

        // Complex extension (e.g. `us-core-race`, `us-core-ethnicity`) with nested `extension` array
        if let Some(Value::Array(sub_exts)) = ext_obj.get("extension") {
            let mut complex_map = Map::new();
            for sub in sub_exts {
                let Some(sub_obj) = sub.as_object() else {
                    continue;
                };
                let Some(sub_url) = sub_obj.get("url").and_then(|u| u.as_str()) else {
                    continue;
                };
                for (k, v) in sub_obj {
                    if k.starts_with("value") && k != "value" {
                        let tv = transform_value(v);
                        if sub_url == "ombCategory" || sub_url == "detailed" {
                            let arr = complex_map
                                .entry(sub_url.to_string())
                                .or_insert_with(|| Value::Array(Vec::new()));
                            if let Value::Array(vec) = arr {
                                vec.push(tv);
                            }
                        } else {
                            complex_map.insert(sub_url.to_string(), tv);
                        }
                        break;
                    }
                }
            }
            if !complex_map.is_empty() {
                out.insert(field_name.to_string(), Value::Object(complex_map));
            }
        }
    }
}

fn split_choice_field(key: &str) -> Option<(&str, &str)> {
    for &prefix in CHOICE_PREFIXES {
        if let Some(rest) = key.strip_prefix(prefix) {
            if let Some(first_char) = rest.chars().next() {
                if first_char.is_ascii_uppercase() {
                    return Some((prefix, rest));
                }
            }
        }
    }
    None
}

fn lowercase_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Converts a slice of FHIR resources of the same `resourceType` into nested Arrow `RecordBatch`es
/// and writes them to a Snappy-compressed Parquet file in 4,096-row chunks.
pub fn write_nested_parquet_for_resource_type(
    resources: &[Value],
    output_path: &Path,
) -> Result<(usize, usize)> {
    use rayon::prelude::*;

    if resources.is_empty() {
        return Ok((0, 0));
    }
    let transformed: Vec<Value> = resources
        .par_iter()
        .map(transform_resource_for_parquet_on_fhir)
        .collect();

    // Sample up to 25,000 transformed records for schema inference (or all if smaller)
    let schema_sample_len = transformed.len().min(25_000);
    let schema = infer_json_schema_from_iterator(transformed[..schema_sample_len].iter().map(Ok))
        .context("Failed to infer nested Arrow schema from FHIR resources")?;
    let schema_ref = Arc::new(schema);

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create Parquet file {}", output_path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema_ref.clone(), Some(props))?;

    const CHUNK_SIZE: usize = 4_096;
    let mut total_rows = 0usize;
    let mut decoder = ReaderBuilder::new(schema_ref.clone())
        .with_batch_size(CHUNK_SIZE)
        .build_decoder()?;

    for chunk in transformed.chunks(CHUNK_SIZE) {
        decoder.serialize(chunk)?;
        if let Some(batch) = decoder.flush()? {
            total_rows += batch.num_rows();
            writer.write(&batch)?;
        }
    }
    writer.close()?;

    Ok((total_rows, schema_ref.fields().len()))
}

#[allow(dead_code)]
pub fn read_parquet_batches(parquet_path: &Path) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}
