use std::sync::Arc;

use arrow::{
    array::{make_array_from_raw, ArrayRef},
    datatypes::Field,
    datatypes::Schema,
    ffi,
    record_batch::RecordBatch,
};
use datafusion::scalar::ScalarValue;
use pyo3::{libc::uintptr_t, prelude::*};

use crate::{errors, types::PyDataType};

/// converts a pyarrow Array into a Rust Array
pub fn to_rust(ob: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let (array_pointer, schema_pointer) =
        ffi::ArrowArray::into_raw(unsafe { ffi::ArrowArray::empty() });

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    ob.call_method1(
        "_export_to_c",
        (array_pointer as uintptr_t, schema_pointer as uintptr_t),
    )?;

    let array = unsafe { make_array_from_raw(array_pointer, schema_pointer) }
        .map_err(|e| errors::DataFusionError::from(e))?;
    Ok(array)
}

pub fn to_rust_batch(batch: &PyAny) -> PyResult<RecordBatch> {
    let schema = batch.getattr("schema")?;
    let names = schema.getattr("names")?.extract::<Vec<String>>()?;

    let fields = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let field = schema.call_method1("field", (i,))?;
            let nullable = field.getattr("nullable")?.extract::<bool>()?;
            let py_data_type = field.getattr("type")?;
            let data_type = py_data_type.extract::<PyDataType>()?.data_type;
            Ok(Field::new(name, data_type, nullable))
        })
        .collect::<PyResult<_>>()?;

    let schema = Arc::new(Schema::new(fields));

    let arrays = (0..names.len())
        .map(|i| {
            let array = batch.call_method1("column", (i,))?;
            to_rust(array)
        })
        .collect::<PyResult<_>>()?;

    let batch =
        RecordBatch::try_new(schema, arrays).map_err(|e| errors::DataFusionError::from(e))?;
    Ok(batch)
}

/// converts a pyarrow Scalar into a Rust Scalar
pub fn to_rust_scalar(ob: &PyAny) -> PyResult<ScalarValue> {
    let t = ob
        .getattr("__class__")?
        .getattr("__name__")?
        .extract::<&str>()?;

    let p = ob.call_method0("as_py")?;

    Ok(match t {
        "Int8Scalar" => ScalarValue::Int8(Some(p.extract::<i8>()?)),
        "Int16Scalar" => ScalarValue::Int16(Some(p.extract::<i16>()?)),
        "Int32Scalar" => ScalarValue::Int32(Some(p.extract::<i32>()?)),
        "Int64Scalar" => ScalarValue::Int64(Some(p.extract::<i64>()?)),
        "UInt8Scalar" => ScalarValue::UInt8(Some(p.extract::<u8>()?)),
        "UInt16Scalar" => ScalarValue::UInt16(Some(p.extract::<u16>()?)),
        "UInt32Scalar" => ScalarValue::UInt32(Some(p.extract::<u32>()?)),
        "UInt64Scalar" => ScalarValue::UInt64(Some(p.extract::<u64>()?)),
        "FloatScalar" => ScalarValue::Float32(Some(p.extract::<f32>()?)),
        "DoubleScalar" => ScalarValue::Float64(Some(p.extract::<f64>()?)),
        "BooleanScalar" => ScalarValue::Boolean(Some(p.extract::<bool>()?)),
        "StringScalar" => ScalarValue::Utf8(Some(p.extract::<String>()?)),
        "LargeStringScalar" => ScalarValue::LargeUtf8(Some(p.extract::<String>()?)),
        other => {
            return Err(errors::DataFusionError::Common(format!(
                "Type \"{}\"not yet implemented",
                other
            ))
            .into())
        }
    })
}
