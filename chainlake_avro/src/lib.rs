use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use apache_avro::{to_avro_datum, types::Value, Schema};
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use rayon::prelude::*;

// ---------------------------------------------------------------------------
// Field-kind enum (matches rpcstream's scalar_type + repeated flag)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum FieldKind {
    String,       // scalar_type="string", repeated=false
    Long,         // scalar_type="int64",  repeated=false
    Bool,         // scalar_type="bool",   repeated=false
    StringArray,  // scalar_type="string", repeated=true
    LongArray,    // scalar_type="int64",  repeated=true  (uncommon but handled)
    BoolArray,    // scalar_type="bool",   repeated=true
}

#[derive(Clone, Debug)]
struct FieldSpec {
    name: String,
    kind: FieldKind,
}

// ---------------------------------------------------------------------------
// Schema entry cache
// ---------------------------------------------------------------------------

struct SchemaEntry {
    schema: Schema,
    fields: Vec<FieldSpec>,
}

// Schema is a pure Rust data tree with no Rc / raw-pointer members.
unsafe impl Send for SchemaEntry {}
unsafe impl Sync for SchemaEntry {}

static SCHEMA_CACHE: Lazy<RwLock<HashMap<i64, Arc<SchemaEntry>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

// ---------------------------------------------------------------------------
// Cached reference to Python's json.dumps (avoids repeated sys.modules look-ups)
// ---------------------------------------------------------------------------

static JSON_DUMPS: Lazy<Py<PyAny>> = Lazy::new(|| {
    Python::with_gil(|py| {
        py.import_bound("json")
            .and_then(|m| m.getattr("dumps"))
            .expect("failed to cache json.dumps")
            .unbind()
    })
});

// ---------------------------------------------------------------------------
// Rust-native value representation (after GIL extraction, before encoding)
// ---------------------------------------------------------------------------

enum ExtractedValue {
    Str(String),
    Long(i64),
    Bool(bool),
    StrArr(Vec<String>),
    LongArr(Vec<i64>),
    BoolArr(Vec<bool>),
}

fn to_avro_value(opt: Option<ExtractedValue>) -> Value {
    match opt {
        None => Value::Union(0, Box::new(Value::Null)),
        Some(ExtractedValue::Str(s)) => Value::Union(1, Box::new(Value::String(s))),
        Some(ExtractedValue::Long(n)) => Value::Union(1, Box::new(Value::Long(n))),
        Some(ExtractedValue::Bool(b)) => Value::Union(1, Box::new(Value::Boolean(b))),
        Some(ExtractedValue::StrArr(v)) => Value::Union(
            1,
            Box::new(Value::Array(v.into_iter().map(Value::String).collect())),
        ),
        Some(ExtractedValue::LongArr(v)) => Value::Union(
            1,
            Box::new(Value::Array(v.into_iter().map(Value::Long).collect())),
        ),
        Some(ExtractedValue::BoolArr(v)) => Value::Union(
            1,
            Box::new(Value::Array(v.into_iter().map(Value::Boolean).collect())),
        ),
    }
}

// ---------------------------------------------------------------------------
// GIL-held helpers: Python → Rust value extraction
// ---------------------------------------------------------------------------

// Mirrors normalize_scalar("string", value): dict/list → compact JSON, else str().
fn extract_string(json_dumps: &Bound<'_, PyAny>, obj: &Bound<'_, PyAny>) -> PyResult<String> {
    if obj.is_instance_of::<PyDict>() || obj.is_instance_of::<PyList>() {
        json_dumps.call1((obj,))?.extract::<String>()
    } else {
        Ok(obj.str()?.to_str()?.to_owned())
    }
}

fn extract_value(
    json_dumps: &Bound<'_, PyAny>,
    obj: &Bound<'_, PyAny>,
    kind: &FieldKind,
) -> PyResult<Option<ExtractedValue>> {
    if obj.is_none() {
        return Ok(None);
    }
    let v = match kind {
        FieldKind::String => ExtractedValue::Str(extract_string(json_dumps, obj)?),
        FieldKind::Long => ExtractedValue::Long(obj.extract::<i64>()?),
        FieldKind::Bool => ExtractedValue::Bool(obj.extract::<bool>()?),
        FieldKind::StringArray => {
            let items: Vec<String> = if let Ok(lst) = obj.downcast::<PyList>() {
                lst.iter()
                    .map(|item| extract_string(json_dumps, &item))
                    .collect::<PyResult<_>>()?
            } else {
                // scalar wrapped as single-element array
                vec![extract_string(json_dumps, obj)?]
            };
            ExtractedValue::StrArr(items)
        }
        FieldKind::LongArray => {
            let items: Vec<i64> = if let Ok(lst) = obj.downcast::<PyList>() {
                lst.iter()
                    .map(|item| item.extract::<i64>())
                    .collect::<PyResult<_>>()?
            } else {
                vec![obj.extract::<i64>()?]
            };
            ExtractedValue::LongArr(items)
        }
        FieldKind::BoolArray => {
            let items: Vec<bool> = if let Ok(lst) = obj.downcast::<PyList>() {
                lst.iter()
                    .map(|item| item.extract::<bool>())
                    .collect::<PyResult<_>>()?
            } else {
                vec![obj.extract::<bool>()?]
            };
            ExtractedValue::BoolArr(items)
        }
    };
    Ok(Some(v))
}

// Extracts one Python dict into a Vec of Option<ExtractedValue> (one per schema field).
// Absent / None values become None (encoded as Avro null by to_avro_value).
fn extract_row(
    py: Python<'_>,
    row: &Bound<'_, PyDict>,
    fields: &[FieldSpec],
) -> PyResult<Vec<Option<ExtractedValue>>> {
    let json_dumps = JSON_DUMPS.bind(py);
    fields
        .iter()
        .map(|field| match row.get_item(&field.name)? {
            None => Ok(None),
            Some(v) => extract_value(json_dumps, &v, &field.kind),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// GIL-free: Avro encode one row → Confluent wire-format bytes
// ---------------------------------------------------------------------------

fn encode_row(
    entry: &SchemaEntry,
    schema_id: i64,
    extracted: Vec<Option<ExtractedValue>>,
) -> Result<Vec<u8>, String> {
    let record_fields: Vec<(String, Value)> = entry
        .fields
        .iter()
        .zip(extracted)
        .map(|(spec, val)| (spec.name.clone(), to_avro_value(val)))
        .collect();

    let avro_bytes = to_avro_datum(&entry.schema, Value::Record(record_fields))
        .map_err(|e| format!("avro_encode error: {}", e))?;

    // Confluent Schema Registry wire format: magic(1) + schema_id(4 BE) + payload
    let mut buf = Vec::with_capacity(5 + avro_bytes.len());
    buf.push(0u8);
    buf.extend_from_slice(&(schema_id as i32).to_be_bytes());
    buf.extend_from_slice(&avro_bytes);
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Public Python API
// ---------------------------------------------------------------------------

/// Register an Avro schema so encode_batch can use it.
///
/// schema_id       – Confluent Schema Registry integer ID for this schema.
/// avro_schema_json – The full Avro JSON schema string (same as passed to AvroSerializer).
/// field_specs     – [(name, scalar_type, repeated), ...] in schema field order.
///                   scalar_type must be "string", "int64", or "bool".
#[pyfunction]
fn register_schema(
    schema_id: i64,
    avro_schema_json: &str,
    field_specs: Vec<(String, String, bool)>,
) -> PyResult<()> {
    let schema = Schema::parse_str(avro_schema_json).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("invalid Avro schema: {}", e))
    })?;

    let fields = field_specs
        .into_iter()
        .map(|(name, scalar_type, repeated)| {
            let kind = match (scalar_type.as_str(), repeated) {
                ("string", false) => FieldKind::String,
                ("string", true) => FieldKind::StringArray,
                ("int64", false) => FieldKind::Long,
                ("int64", true) => FieldKind::LongArray,
                ("bool", false) => FieldKind::Bool,
                ("bool", true) => FieldKind::BoolArray,
                _ => FieldKind::String, // safe fallback
            };
            FieldSpec { name, kind }
        })
        .collect();

    SCHEMA_CACHE
        .write()
        .unwrap()
        .insert(schema_id, Arc::new(SchemaEntry { schema, fields }));
    Ok(())
}

/// Encode a batch of rows for a single schema and return Confluent wire-format bytes.
///
/// schema_id – ID previously registered via register_schema().
/// rows      – list of dicts, each already prepared by _prepare_row_only()
///             (id and ingest_timestamp set, kafka_partition_key removed).
///
/// Returns list[bytes], one entry per input row, in input order.
///
/// GIL strategy:
///   Phase 1 (GIL held)    – extract Python dicts into Rust-native values.
///   Phase 2 (GIL released) – rayon parallel Avro encode (all threads run freely).
///   Phase 3 (GIL acquired) – wrap each Vec<u8> in PyBytes so Python always
///                             receives bytes, not list[int] (PyO3 ≤0.20 quirk).
#[pyfunction]
fn encode_batch(
    py: Python<'_>,
    schema_id: i64,
    rows: &Bound<'_, PyList>,
) -> PyResult<Vec<Py<PyBytes>>> {
    // Cheap Arc clone; drops the RwLock read guard before touching Python objects.
    let entry: Arc<SchemaEntry> = {
        let cache = SCHEMA_CACHE.read().unwrap();
        cache.get(&schema_id).cloned().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "schema_id {} not registered; call register_schema() first",
                schema_id
            ))
        })?
    };

    // Phase 1: GIL held — extract all Python values into Rust types
    let extracted: Vec<Vec<Option<ExtractedValue>>> = rows
        .iter()
        .map(|item| {
            let row = item.downcast::<PyDict>()?;
            extract_row(py, row, &entry.fields)
        })
        .collect::<PyResult<_>>()?;

    // Phase 2: GIL released — true parallel Avro encoding with rayon
    let results: Vec<Result<Vec<u8>, String>> = py.allow_threads(|| {
        extracted
            .into_par_iter()
            .map(|row| encode_row(&entry, schema_id, row))
            .collect()
    });

    // Propagate first encoding error (rare: only fires on schema/value mismatch).
    // Explicitly wrap as PyBytes so Python always receives bytes, not list[int].
    results.into_iter().map(|r| {
        r.map_err(|msg| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(msg))
         .map(|v| PyBytes::new_bound(py, &v).unbind())
    }).collect()
}

#[pymodule]
fn chainlake_avro(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(register_schema, m)?)?;
    m.add_function(wrap_pyfunction!(encode_batch, m)?)?;
    Ok(())
}
