use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{
    builder::{BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder},
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewDefinition {
    pub name: String,
    pub resource: String,
    #[serde(default)]
    pub fhir_version: Vec<String>,
    #[serde(default)]
    pub select: Vec<Select>,
    #[serde(default)]
    pub r#where: Vec<WhereClause>,
    #[serde(default)]
    pub constant: Vec<ConstantDef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Select {
    #[serde(default)]
    pub column: Vec<ColumnDef>,
    #[serde(default)]
    pub select: Vec<Select>,
    pub for_each: Option<String>,
    pub for_each_or_null: Option<String>,
    #[serde(default)]
    pub union_all: Vec<Select>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDef {
    pub path: String,
    pub name: String,
    pub r#type: Option<String>,
    #[serde(default)]
    pub collection: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WhereClause {
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConstantDef {
    pub name: String,
    pub value_string: Option<String>,
    pub value_code: Option<String>,
    pub value_uri: Option<String>,
    pub value_url: Option<String>,
    pub value_canonical: Option<String>,
    pub value_id: Option<String>,
    pub value_uuid: Option<String>,
    pub value_oid: Option<String>,
    pub value_date: Option<String>,
    pub value_date_time: Option<String>,
    pub value_time: Option<String>,
    pub value_boolean: Option<bool>,
    pub value_integer: Option<i64>,
    pub value_decimal: Option<f64>,
}

impl ConstantDef {
    pub fn to_fhirpath_literal(&self) -> Result<String> {
        for s in [
            &self.value_string,
            &self.value_code,
            &self.value_uri,
            &self.value_url,
            &self.value_canonical,
            &self.value_id,
            &self.value_uuid,
            &self.value_oid,
            &self.value_date,
            &self.value_date_time,
            &self.value_time,
        ]
        .into_iter()
        .flatten()
        {
            return Ok(format!("'{}'", s));
        }
        if let Some(b) = self.value_boolean {
            return Ok(b.to_string());
        }
        if let Some(i) = self.value_integer {
            return Ok(i.to_string());
        }
        if let Some(d) = self.value_decimal {
            return Ok(d.to_string());
        }
        bail!("Constant {} has no supported value[x] set", self.name)
    }
}

/// Represents a cell value in a flattened materialized view row.
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Str(String),
    StrList(Vec<String>),
}

/// Ordered list of columns and flattened rows produced by applying a `ViewDefinition`.
#[derive(Debug, Clone)]
pub struct FlatTable {
    pub columns: Vec<ColumnDef>,
    pub rows: Vec<Vec<CellValue>>,
}

impl ViewDefinition {
    pub fn from_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("Failed to read ViewDefinition {}", path.display()))?;
        let mut vdef: ViewDefinition = serde_json::from_slice(&bytes)
            .with_context(|| format!("Failed to parse ViewDefinition {}", path.display()))?;
        vdef.validate_and_resolve_constants()?;
        Ok(vdef)
    }

    pub fn load_all_from_dir(dir: &Path) -> Result<Vec<Self>> {
        let mut views = Vec::new();
        let mut entries: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        entries.sort();
        for path in entries {
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                views.push(Self::from_file(&path)?);
            }
        }
        Ok(views)
    }

    pub fn validate_and_resolve_constants(&mut self) -> Result<()> {
        if self.name.is_empty() || self.resource.is_empty() {
            bail!("ViewDefinition must have non-empty 'name' and 'resource'");
        }
        let mut const_map = HashMap::new();
        for c in &self.constant {
            const_map.insert(format!("%{}", c.name), c.to_fhirpath_literal()?);
        }
        if !const_map.is_empty() {
            for w in &mut self.r#where {
                w.path = replace_constants(&w.path, &const_map);
            }
            for s in &mut self.select {
                replace_constants_in_select(s, &const_map);
            }
        }
        Ok(())
    }

    /// Returns the ordered schema of all columns defined in this `ViewDefinition`.
    pub fn all_columns(&self) -> Vec<ColumnDef> {
        let mut cols = Vec::new();
        collect_columns_from_selects(&self.select, &mut cols);
        cols
    }

    /// Applies this `ViewDefinition` to a slice of FHIR resources (filtering by `resourceType` and `where`) in parallel.
    pub fn apply_to_resources(&self, resources: &[Value]) -> Result<FlatTable> {
        use rayon::prelude::*;

        let columns = self.all_columns();
        let nested_rows: Result<Vec<Vec<Vec<CellValue>>>> = resources
            .par_iter()
            .filter_map(|res| {
                let rtype = res
                    .get("resourceType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if rtype != self.resource {
                    return None;
                }
                match self.satisfies_where(res) {
                    Ok(true) => Some(apply_all_selects(Some(res), res, &self.select)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect();

        let all_rows: Vec<Vec<CellValue>> = nested_rows?.into_iter().flatten().collect();

        Ok(FlatTable {
            columns,
            rows: all_rows,
        })
    }

    fn satisfies_where(&self, root: &Value) -> Result<bool> {
        for w in &self.r#where {
            let vals = evaluate_fhirpath(root, root, &w.path)?;
            if vals.is_empty() {
                return Ok(false);
            }
            match &vals[0] {
                Value::Bool(true) => continue,
                Value::Bool(false) => return Ok(false),
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// Generates standard SQL `CREATE TABLE` DDL for relational sinks (PostgreSQL / SQLite / DuckDB).
    pub fn to_sql_ddl(&self) -> String {
        let cols = self.all_columns();
        let col_defs: Vec<String> = cols
            .iter()
            .map(|c| {
                let sql_type = match c.r#type.as_deref() {
                    Some("boolean") => "BOOLEAN",
                    Some("integer") | Some("unsignedInt") | Some("positiveInt") => "INTEGER",
                    Some("integer64") => "BIGINT",
                    Some("decimal") => "DOUBLE PRECISION",
                    Some("date") => "DATE",
                    Some("dateTime") | Some("instant") => "TIMESTAMP WITH TIME ZONE",
                    _ => "TEXT",
                };
                format!("  {} {}", c.name, sql_type)
            })
            .collect();
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n);\nCREATE INDEX IF NOT EXISTS idx_{}_id ON {} (id);",
            self.name,
            col_defs.join(",\n"),
            self.name,
            self.name
        )
    }
}

fn replace_constants(path: &str, const_map: &HashMap<String, String>) -> String {
    let mut out = path.to_string();
    for (k, v) in const_map {
        out = out.replace(k, v);
    }
    out
}

fn replace_constants_in_select(select: &mut Select, const_map: &HashMap<String, String>) {
    for c in &mut select.column {
        c.path = replace_constants(&c.path, const_map);
    }
    if let Some(fe) = &mut select.for_each {
        *fe = replace_constants(fe, const_map);
    }
    if let Some(feon) = &mut select.for_each_or_null {
        *feon = replace_constants(feon, const_map);
    }
    for s in &mut select.select {
        replace_constants_in_select(s, const_map);
    }
    for u in &mut select.union_all {
        replace_constants_in_select(u, const_map);
    }
}

fn collect_columns_from_selects(selects: &[Select], out: &mut Vec<ColumnDef>) {
    for s in selects {
        collect_columns_from_one_select(s, out);
    }
}

fn collect_columns_from_one_select(select: &Select, out: &mut Vec<ColumnDef>) {
    for c in &select.column {
        out.push(c.clone());
    }
    collect_columns_from_selects(&select.select, out);
    if let Some(first_union) = select.union_all.first() {
        collect_columns_from_one_select(first_union, out);
    }
}

fn apply_all_selects(
    elem: Option<&Value>,
    root: &Value,
    selects: &[Select],
) -> Result<Vec<Vec<CellValue>>> {
    if selects.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows_per_select = Vec::with_capacity(selects.len());
    for s in selects {
        rows_per_select.push(apply_select(elem, root, s)?);
    }
    Ok(cross_join_all(rows_per_select))
}

fn cross_join_all(rows_per_select: Vec<Vec<Vec<CellValue>>>) -> Vec<Vec<CellValue>> {
    let mut current: Vec<Vec<CellValue>> = Vec::new();
    for (idx, rows) in rows_per_select.into_iter().enumerate() {
        if rows.is_empty() {
            return Vec::new();
        }
        if idx == 0 {
            current = rows;
        } else {
            let mut next = Vec::with_capacity(current.len() * rows.len());
            for left in &current {
                for right in &rows {
                    let mut combined = Vec::with_capacity(left.len() + right.len());
                    combined.extend_from_slice(left);
                    combined.extend_from_slice(right);
                    next.push(combined);
                }
            }
            current = next;
        }
    }
    current
}

fn apply_select(
    elem: Option<&Value>,
    root: &Value,
    select: &Select,
) -> Result<Vec<Vec<CellValue>>> {
    let (for_each_path, keep_null) = if let Some(fe) = &select.for_each {
        (Some(fe.as_str()), false)
    } else if let Some(feon) = &select.for_each_or_null {
        (Some(feon.as_str()), true)
    } else {
        (None, false)
    };

    let targets: Vec<Value> = match (elem, for_each_path) {
        (Some(e), Some(path)) if !path.is_empty() => evaluate_fhirpath(e, root, path)?,
        (Some(e), _) => vec![e.clone()],
        (None, _) => Vec::new(),
    };

    let mut all_rows = Vec::new();
    for t in &targets {
        let rows = apply_column_select_union(Some(t), root, select)?;
        all_rows.extend(rows);
    }

    if all_rows.is_empty() && (keep_null || (elem.is_none() && for_each_path.is_none())) {
        let null_rows = apply_column_select_union(None, root, select)?;
        all_rows.extend(null_rows);
    }

    Ok(all_rows)
}

fn apply_column_select_union(
    elem: Option<&Value>,
    root: &Value,
    select: &Select,
) -> Result<Vec<Vec<CellValue>>> {
    let mut parts: Vec<Vec<Vec<CellValue>>> = Vec::new();

    if !select.column.is_empty() {
        let row = apply_columns(elem, root, &select.column)?;
        parts.push(vec![row]);
    }

    if !select.select.is_empty() {
        let sub_rows = apply_all_selects(elem, root, &select.select)?;
        if sub_rows.is_empty() {
            return Ok(Vec::new());
        }
        parts.push(sub_rows);
    }

    if !select.union_all.is_empty() {
        let mut union_rows = Vec::new();
        if elem.is_none() {
            if let Some(first) = select.union_all.first() {
                union_rows.extend(apply_select(None, root, first)?);
            }
        } else {
            for u in &select.union_all {
                union_rows.extend(apply_select(elem, root, u)?);
            }
        }
        if union_rows.is_empty() {
            return Ok(Vec::new());
        }
        parts.push(union_rows);
    }

    Ok(cross_join_all(parts))
}

fn apply_columns(
    elem: Option<&Value>,
    root: &Value,
    columns: &[ColumnDef],
) -> Result<Vec<CellValue>> {
    let mut row = Vec::with_capacity(columns.len());
    for col in columns {
        let Some(e) = elem else {
            row.push(CellValue::Null);
            continue;
        };
        let vals = evaluate_fhirpath(e, root, &col.path)?;
        row.push(convert_to_cell_value(&vals, col)?);
    }
    Ok(row)
}

fn convert_to_cell_value(vals: &[Value], col: &ColumnDef) -> Result<CellValue> {
    if vals.is_empty() {
        return Ok(CellValue::Null);
    }
    if col.collection {
        let list: Vec<String> = vals.iter().filter_map(value_to_string).collect();
        if list.is_empty() {
            return Ok(CellValue::Null);
        }
        return Ok(CellValue::StrList(list));
    }
    let first = &vals[0];
    match col.r#type.as_deref() {
        Some("boolean") => match first {
            Value::Bool(b) => Ok(CellValue::Bool(*b)),
            Value::String(s) => Ok(CellValue::Bool(s.eq_ignore_ascii_case("true"))),
            _ => Ok(CellValue::Null),
        },
        Some("integer") | Some("unsignedInt") | Some("positiveInt") => match first {
            Value::Number(n) => Ok(n
                .as_i64()
                .map(|i| CellValue::Int32(i as i32))
                .unwrap_or(CellValue::Null)),
            Value::String(s) => Ok(s
                .parse::<i32>()
                .map(CellValue::Int32)
                .unwrap_or(CellValue::Null)),
            _ => Ok(CellValue::Null),
        },
        Some("integer64") => match first {
            Value::Number(n) => Ok(n.as_i64().map(CellValue::Int64).unwrap_or(CellValue::Null)),
            Value::String(s) => Ok(s
                .parse::<i64>()
                .map(CellValue::Int64)
                .unwrap_or(CellValue::Null)),
            _ => Ok(CellValue::Null),
        },
        Some("decimal") => match first {
            Value::Number(n) => Ok(n
                .as_f64()
                .map(CellValue::Float64)
                .unwrap_or(CellValue::Null)),
            Value::String(s) => Ok(s
                .parse::<f64>()
                .map(CellValue::Float64)
                .unwrap_or(CellValue::Null)),
            _ => Ok(CellValue::Null),
        },
        _ => Ok(value_to_string(first)
            .map(CellValue::Str)
            .unwrap_or(CellValue::Null)),
    }
}

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

/// Evaluates the subset of FHIRPath used by SQL-on-FHIR v2 `ViewDefinition`s directly on `serde_json::Value`.
pub fn evaluate_fhirpath(elem: &Value, root: &Value, raw_path: &str) -> Result<Vec<Value>> {
    let path = raw_path.trim();
    if path.is_empty() || path == "$this" {
        return Ok(vec![elem.clone()]);
    }
    if path == "getResourceKey()" {
        if let Some(id) = root.get("id").and_then(|v| v.as_str()) {
            return Ok(vec![Value::String(strip_resource_prefix(id).to_string())]);
        }
        return Ok(Vec::new());
    }

    // Check for top-level boolean comparison in `where` clauses (e.g. `status = 'final'`)
    if let Some((lhs, op, rhs)) = split_comparison(path) {
        let left_vals = evaluate_fhirpath(elem, root, lhs)?;
        let right_lit = rhs.trim().trim_matches('\'').trim_matches('"');
        let matched = left_vals.iter().any(|v| {
            let s = value_to_string(v).unwrap_or_default();
            if op == "=" {
                s == right_lit
            } else {
                s != right_lit
            }
        });
        return Ok(vec![Value::Bool(matched)]);
    }

    let segments = split_path_segments(path);
    let mut current: Vec<Value> = vec![elem.clone()];

    let mut i = 0;
    while i < segments.len() {
        let seg = segments[i];
        if seg == "$this" {
            i += 1;
            continue;
        }
        if seg == "getResourceKey()" {
            let mut next = Vec::new();
            for item in &current {
                if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                    next.push(Value::String(strip_resource_prefix(id).to_string()));
                }
            }
            current = next;
            i += 1;
            continue;
        }
        if let Some(target_type) = parse_fn_arg(seg, "getReferenceKey") {
            let mut next = Vec::new();
            for item in &current {
                if let Some(id) = extract_reference_key(item, target_type) {
                    next.push(Value::String(id));
                }
            }
            current = next;
            i += 1;
            continue;
        }
        if seg == "first()" {
            if let Some(first) = current.into_iter().next() {
                current = vec![first];
            } else {
                current = Vec::new();
            }
            i += 1;
            continue;
        }
        if let Some(where_expr) = parse_fn_arg(seg, "where") {
            let mut next = Vec::new();
            for item in current {
                let cond = evaluate_fhirpath(&item, root, where_expr)?;
                if matches!(cond.first(), Some(Value::Bool(true))) {
                    next.push(item);
                }
            }
            current = next;
            i += 1;
            continue;
        }

        // Check if the NEXT segment is `ofType(TypeName)` -> e.g. `effective.ofType(dateTime)`
        if i + 1 < segments.len() {
            if let Some(type_arg) = parse_fn_arg(segments[i + 1], "ofType") {
                let choice_field = format!("{}{}", seg, capitalize_first(type_arg));
                let nested_type_field = lowercase_first(type_arg);
                let mut next = Vec::new();
                for item in &current {
                    if let Value::Object(map) = item {
                        if let Some(val) = map.get(&choice_field) {
                            push_flattened(val, &mut next);
                        } else if let Some(Value::Object(choice_obj)) = map.get(seg) {
                            if let Some(val) = choice_obj.get(&nested_type_field) {
                                push_flattened(val, &mut next);
                            }
                        }
                    }
                }
                current = next;
                i += 2;
                continue;
            }
        }

        // Normal field step
        let mut next = Vec::new();
        for item in &current {
            if let Value::Object(map) = item {
                if let Some(val) = map.get(seg) {
                    push_flattened(val, &mut next);
                }
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
        i += 1;
    }

    Ok(current)
}

fn push_flattened(val: &Value, out: &mut Vec<Value>) {
    match val {
        Value::Array(arr) => {
            for item in arr {
                out.push(item.clone());
            }
        }
        Value::Null => {}
        other => out.push(other.clone()),
    }
}

fn extract_reference_key(ref_obj: &Value, expected_type: &str) -> Option<String> {
    let ref_str = match ref_obj {
        Value::Object(map) => map.get("reference").and_then(|v| v.as_str())?,
        Value::String(s) => s.as_str(),
        _ => return None,
    };

    // Ignore unresolved query references like "Practitioner?identifier=..."
    if ref_str.contains('?') {
        return None;
    }

    // Parse optional resourceType and idPart from e.g. "Patient/123" or "https://.../Patient/123"
    if let Some((prefix, id_part)) = ref_str.rsplit_once('/') {
        let res_type = prefix.rsplit('/').next().unwrap_or(prefix);
        if expected_type.is_empty() || res_type == expected_type {
            return Some(id_part.to_string());
        }
        None
    } else if expected_type.is_empty() {
        Some(strip_resource_prefix(ref_str).to_string())
    } else {
        None
    }
}

fn strip_resource_prefix(id: &str) -> &str {
    if let Some(stripped) = id.strip_prefix("urn:uuid:") {
        return stripped;
    }
    if let Some((_, right)) = id.rsplit_once('/') {
        return right;
    }
    id
}

fn split_path_segments(path: &str) -> Vec<&str> {
    let mut segments = Vec::new();
    let mut depth = 0usize;
    let mut in_quotes = false;
    let mut start = 0usize;
    for (idx, ch) in path.char_indices() {
        match ch {
            '\'' | '"' => in_quotes = !in_quotes,
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes => depth = depth.saturating_sub(1),
            '.' if depth == 0 && !in_quotes => {
                segments.push(path[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    let tail = path[start..].trim();
    if !tail.is_empty() {
        segments.push(tail);
    }
    segments
}

fn parse_fn_arg<'a>(seg: &'a str, fn_name: &str) -> Option<&'a str> {
    let rest = seg.strip_prefix(fn_name)?.trim();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    Some(inner.trim())
}

fn split_comparison(expr: &str) -> Option<(&str, &str, &str)> {
    let mut depth = 0usize;
    let mut in_quotes = false;
    let bytes = expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => in_quotes = !in_quotes,
            b'(' if !in_quotes => depth += 1,
            b')' if !in_quotes => depth = depth.saturating_sub(1),
            b'!' if depth == 0 && !in_quotes && i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                return Some((&expr[..i], "!=", &expr[i + 2..]));
            }
            b'=' if depth == 0 && !in_quotes => {
                return Some((&expr[..i], "=", &expr[i + 1..]));
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn lowercase_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

impl FlatTable {
    /// Converts the flattened view rows into an Apache Arrow `RecordBatch`.
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        let num_rows = self.rows.len();

        for (col_idx, col) in self.columns.iter().enumerate() {
            if col.collection {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for row in &self.rows {
                    match &row[col_idx] {
                        CellValue::StrList(items) => {
                            for item in items {
                                builder.values().append_value(item);
                            }
                            builder.append(true);
                        }
                        _ => builder.append(false),
                    }
                }
                fields.push(Field::new(
                    &col.name,
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ));
                arrays.push(Arc::new(builder.finish()));
                continue;
            }

            match col.r#type.as_deref() {
                Some("boolean") => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row in &self.rows {
                        match &row[col_idx] {
                            CellValue::Bool(b) => builder.append_value(*b),
                            _ => builder.append_null(),
                        }
                    }
                    fields.push(Field::new(&col.name, DataType::Boolean, true));
                    arrays.push(Arc::new(builder.finish()));
                }
                Some("integer") | Some("unsignedInt") | Some("positiveInt") => {
                    let mut builder = Int32Builder::with_capacity(num_rows);
                    for row in &self.rows {
                        match &row[col_idx] {
                            CellValue::Int32(v) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    fields.push(Field::new(&col.name, DataType::Int32, true));
                    arrays.push(Arc::new(builder.finish()));
                }
                Some("integer64") => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row in &self.rows {
                        match &row[col_idx] {
                            CellValue::Int64(v) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    fields.push(Field::new(&col.name, DataType::Int64, true));
                    arrays.push(Arc::new(builder.finish()));
                }
                Some("decimal") => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row in &self.rows {
                        match &row[col_idx] {
                            CellValue::Float64(v) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    fields.push(Field::new(&col.name, DataType::Float64, true));
                    arrays.push(Arc::new(builder.finish()));
                }
                _ => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    for row in &self.rows {
                        match &row[col_idx] {
                            CellValue::Str(s) => builder.append_value(s),
                            _ => builder.append_null(),
                        }
                    }
                    fields.push(Field::new(&col.name, DataType::Utf8, true));
                    arrays.push(Arc::new(builder.finish()));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| anyhow!("Failed to construct Arrow RecordBatch: {}", e))
    }

    /// Writes the materialized view directly to a Snappy-compressed Apache Parquet file.
    pub fn write_parquet(&self, output_path: &Path) -> Result<usize> {
        let batch = self.to_record_batch()?;
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create Parquet file {}", output_path.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(batch.num_rows())
    }
}
