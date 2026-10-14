use anyhow::{Context, Result};
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// Recursively collects all `.json` and `.ndjson` files from a path (file or directory).
pub fn collect_input_files(input_path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if input_path.is_file() {
        files.push(input_path.to_path_buf());
    } else if input_path.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(input_path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        entries.sort();
        for entry in entries {
            if entry.is_dir() {
                files.extend(collect_input_files(&entry)?);
            } else if let Some(ext) = entry.extension().and_then(|s| s.to_str()) {
                if ext.eq_ignore_ascii_case("json") || ext.eq_ignore_ascii_case("ndjson") {
                    files.push(entry);
                }
            }
        }
    }
    Ok(files)
}

/// Loads all FHIR resources from a list of `.json` (Bundle or single resource) or `.ndjson` files
/// in parallel using Rayon, resolving intra-Bundle `urn:uuid:` references just like
/// `ReadJsonFn.updateResolvedRefIds` in `fhir-data-pipes`.
pub fn load_resources_parallel(files: &[PathBuf]) -> Result<Vec<Value>> {
    let per_file: Result<Vec<Vec<Value>>> = files
        .par_iter()
        .map(|path| load_resources_from_file(path))
        .collect();
    Ok(per_file?.into_iter().flatten().collect())
}

pub fn load_resources_from_file(path: &Path) -> Result<Vec<Value>> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if ext == "ndjson" {
        let file = File::open(path)
            .with_context(|| format!("Failed to open NDJSON file {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut resources = Vec::new();
        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let val: Value = serde_json::from_str(trimmed)
                .with_context(|| format!("Invalid JSON line in {}", path.display()))?;
            extract_from_parsed_value(val, &mut resources);
        }
        Ok(resources)
    } else {
        let bytes = std::fs::read(path)
            .with_context(|| format!("Failed to read JSON file {}", path.display()))?;
        let val: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("Invalid JSON in {}", path.display()))?;
        let mut resources = Vec::new();
        extract_from_parsed_value(val, &mut resources);
        Ok(resources)
    }
}

fn extract_from_parsed_value(val: Value, out: &mut Vec<Value>) {
    let rtype = val
        .get("resourceType")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if rtype == "Bundle" {
        if let Some(entries) = val.get("entry").and_then(|e| e.as_array()) {
            // 1st pass: Build fullUrl (urn:uuid:...) -> "ResourceType/id" map
            let mut urn_map: HashMap<String, String> = HashMap::with_capacity(entries.len());
            for entry in entries {
                if let (Some(full_url), Some(res)) = (
                    entry.get("fullUrl").and_then(|u| u.as_str()),
                    entry.get("resource"),
                ) {
                    if let (Some(res_type), Some(id)) = (
                        res.get("resourceType").and_then(|t| t.as_str()),
                        res.get("id").and_then(|i| i.as_str()),
                    ) {
                        urn_map.insert(full_url.to_string(), format!("{}/{}", res_type, id));
                    }
                }
            }
            // 2nd pass: Extract each resource and rewrite URN references
            for entry in entries {
                if let Some(mut res) = entry.get("resource").cloned() {
                    if !urn_map.is_empty() {
                        rewrite_urn_references(&mut res, &urn_map);
                    }
                    out.push(res);
                }
            }
        }
    } else if !rtype.is_empty() {
        out.push(val);
    }
}

fn rewrite_urn_references(val: &mut Value, urn_map: &HashMap<String, String>) {
    match val {
        Value::Object(map) => {
            if let Some(Value::String(ref_str)) = map.get_mut("reference") {
                if ref_str.starts_with("urn:") {
                    if let Some(resolved) = urn_map.get(ref_str.as_str()) {
                        *ref_str = resolved.clone();
                    }
                }
            }
            for (_, v) in map.iter_mut() {
                rewrite_urn_references(v, urn_map);
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                rewrite_urn_references(item, urn_map);
            }
        }
        _ => {}
    }
}

/// Extracts `(id, last_updated, is_deleted)` metadata from a FHIR resource JSON object
/// for incremental deduplication and tombstone handling.
pub fn extract_resource_meta(res: &Value) -> Option<(String, String, bool)> {
    let id = res.get("id")?.as_str()?.to_string();
    let meta = res.get("meta").and_then(|m| m.as_object());
    let last_updated = meta
        .and_then(|m| m.get("lastUpdated"))
        .and_then(|l| l.as_str())
        .unwrap_or("")
        .to_string();
    let is_deleted = meta
        .and_then(|m| m.get("tag"))
        .and_then(|t| t.as_array())
        .map(|tags| {
            tags.iter().any(|tag| {
                let code = tag.get("code").and_then(|c| c.as_str()).unwrap_or("");
                code.eq_ignore_ascii_case("remove") || code.eq_ignore_ascii_case("DELETE")
            })
        })
        .unwrap_or(false);
    Some((id, last_updated, is_deleted))
}

#[allow(dead_code)]
pub fn group_by_resource_type(resources: Vec<Value>) -> HashMap<String, Vec<Value>> {
    let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
    for res in resources {
        if let Some(rt) = res.get("resourceType").and_then(|v| v.as_str()) {
            grouped.entry(rt.to_string()).or_default().push(res);
        }
    }
    grouped
}

#[allow(dead_code)]
pub fn ensure_meta_object(map: &mut Map<String, Value>) {
    if !map.contains_key("meta") {
        map.insert("meta".to_string(), Value::Object(Map::new()));
    }
}
