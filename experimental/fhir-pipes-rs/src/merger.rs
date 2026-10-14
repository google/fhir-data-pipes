use crate::extractor::extract_resource_meta;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct MergeStats {
    pub total_input_records: usize,
    pub duplicate_ids_merged: usize,
    pub tombstones_removed: usize,
    pub final_active_records: usize,
}

/// Deduplicates and merges FHIR resources across a base dataset and an incremental update,
/// matching the exact semantics of `ParquetMerger.java` in `fhir-data-pipes` (without Flink/Beam):
/// - Groups by `(resourceType, id)`
/// - Keeps the version with the lexicographically greatest `meta.lastUpdated` (ties broken by newer stream)
/// - Filters out deleted records tagged with `ActionType.REMOVE` (`meta.tag.code == "remove"`).
pub fn deduplicate_and_merge_resources(
    base_resources: Vec<Value>,
    incremental_resources: Vec<Value>,
) -> (Vec<Value>, MergeStats) {
    let mut stats = MergeStats {
        total_input_records: base_resources.len() + incremental_resources.len(),
        ..Default::default()
    };

    // Map key: (resourceType, id) -> (last_updated, is_deleted, Value)
    let mut latest_map: HashMap<(String, String), (String, bool, Value)> =
        HashMap::with_capacity(stats.total_input_records);

    for res in base_resources.into_iter().chain(incremental_resources) {
        let rtype = res
            .get("resourceType")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if rtype.is_empty() {
            continue;
        }
        let Some((id, last_updated, is_deleted)) = extract_resource_meta(&res) else {
            continue;
        };
        let key = (rtype, id);
        if let Some((existing_ts, _, _)) = latest_map.get(&key) {
            stats.duplicate_ids_merged += 1;
            // `<=` means if timestamps are equal or empty, the later (incremental) record wins
            if existing_ts.as_str() <= last_updated.as_str() {
                latest_map.insert(key, (last_updated, is_deleted, res));
            }
        } else {
            latest_map.insert(key, (last_updated, is_deleted, res));
        }
    }

    let mut merged: Vec<Value> = Vec::with_capacity(latest_map.len());
    for (_, (_, is_deleted, res)) in latest_map {
        if is_deleted {
            stats.tombstones_removed += 1;
        } else {
            merged.push(res);
        }
    }

    // Sort deterministically by (resourceType, id)
    merged.sort_by(|a, b| {
        let rt_a = a.get("resourceType").and_then(|v| v.as_str()).unwrap_or("");
        let rt_b = b.get("resourceType").and_then(|v| v.as_str()).unwrap_or("");
        let id_a = a.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let id_b = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
        (rt_a, id_a).cmp(&(rt_b, id_b))
    });

    stats.final_active_records = merged.len();
    (merged, stats)
}
