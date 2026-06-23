// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

#[cfg(test)]
use arrow::array::StructArray;
use arrow::array::{Array, ArrayRef, cast::AsArray};
#[cfg(test)]
use arrow_schema::Fields;
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use datafusion_common::exec_datafusion_err;
use datafusion_common::{DataFusionError, Result};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use parquet_variant::{Variant, VariantPath, VariantPathElement};
use parquet_variant_compute::{VariantArray, VariantType};

#[cfg(test)]
use parquet_variant_compute::VariantArrayBuilder;

#[cfg(test)]
use parquet_variant_json::JsonToVariant;

pub fn try_field_as_variant_array(field: &Field) -> Result<()> {
    ensure(
        matches!(field.extension_type(), VariantType),
        "field does not have extension type VariantType",
    )?;

    let variant_type = VariantType;
    variant_type.supports_data_type(field.data_type())?;

    Ok(())
}

pub fn _try_field_as_binary(field: &Field) -> Result<()> {
    match field.data_type() {
        DataType::Binary | DataType::BinaryView | DataType::LargeBinary => {}
        unsupported => {
            return exec_err!("expected binary field, got {unsupported} field");
        }
    }

    Ok(())
}

pub fn try_field_as_string(field: &Field) -> Result<()> {
    match field.data_type() {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {}
        unsupported => {
            return exec_err!("expected string field, got {unsupported} field");
        }
    }

    Ok(())
}

pub fn try_parse_variant_scalar(scalar: &ScalarValue) -> Result<VariantArray> {
    let v = match scalar {
        ScalarValue::Struct(v) => v,
        unsupported => {
            return exec_err!(
                "expected variant scalar value, got data type: {}",
                unsupported.data_type()
            );
        }
    };

    VariantArray::try_new(v.as_ref()).map_err(Into::into)
}

pub fn try_parse_binary_scalar(scalar: &ScalarValue) -> Result<Option<&Vec<u8>>> {
    let b = match scalar {
        ScalarValue::Binary(b)
        | ScalarValue::BinaryView(b)
        | ScalarValue::LargeBinary(b) => b,
        unsupported => {
            return exec_err!(
                "expected binary scalar value, got data type: {}",
                unsupported.data_type()
            );
        }
    };

    Ok(b.as_ref())
}

pub fn try_parse_binary_columnar(array: &Arc<dyn Array>) -> Result<Vec<Option<&[u8]>>> {
    if let Some(binary_array) = array.as_binary_opt::<i32>() {
        return Ok(binary_array.into_iter().collect::<Vec<_>>());
    }

    if let Some(binary_view_array) = array.as_binary_view_opt() {
        return Ok(binary_view_array.into_iter().collect::<Vec<_>>());
    }

    if let Some(large_binary_array) = array.as_binary_opt::<i64>() {
        return Ok(large_binary_array.into_iter().collect::<Vec<_>>());
    }

    Err(exec_datafusion_err!("expected binary array"))
}

pub fn try_parse_string_scalar(scalar: &ScalarValue) -> Result<Option<&String>> {
    let b = match scalar {
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => s,
        unsupported => {
            return exec_err!(
                "expected binary scalar value, got data type: {}",
                unsupported.data_type()
            );
        }
    };

    Ok(b.as_ref())
}

pub fn try_parse_string_columnar(array: &Arc<dyn Array>) -> Result<Vec<Option<&str>>> {
    if let Some(string_array) = array.as_string_opt::<i32>() {
        return Ok(string_array.into_iter().collect::<Vec<_>>());
    }

    if let Some(string_view_array) = array.as_string_view_opt() {
        return Ok(string_view_array.into_iter().collect::<Vec<_>>());
    }

    if let Some(large_string_array) = array.as_string_opt::<i64>() {
        return Ok(large_string_array.into_iter().collect::<Vec<_>>());
    }

    Err(exec_datafusion_err!("expected string array"))
}

pub fn variant_get_single_value<T>(
    variant_array: &VariantArray,
    index: usize,
    path: &VariantPath<'_>,
    extract: for<'m, 'v> fn(Variant<'m, 'v>) -> Result<Option<T>>,
) -> Result<Option<T>> {
    let Some(variant) = variant_array.iter().nth(index).flatten() else {
        return Ok(None);
    };

    let Some(value) = variant.get_path(path) else {
        return Ok(None);
    };

    extract(value)
}

pub fn variant_get_array_values<T>(
    variant_array: &VariantArray,
    path: &VariantPath<'_>,
    extract: for<'m, 'v> fn(Variant<'m, 'v>) -> Result<Option<T>>,
) -> Result<Vec<Option<T>>> {
    variant_array
        .iter()
        .map(|maybe_variant| {
            let Some(variant) = maybe_variant else {
                return Ok(None);
            };

            let Some(value) = variant.get_path(path) else {
                return Ok(None);
            };

            extract(value)
        })
        .collect()
}

/// Build a [`VariantPath`] from a scalar value.
///
/// - **String** scalars use dot-notation parsing (e.g. `'a.b.c'` → path `[a, b, c]`)
/// - **List** scalars treat each element as a single field name
///   (e.g. `['a.b', 'c']` → path `[a.b, c]`), which is critical for keys that
///   contain dots such as OTEL attribute keys like `http.response.status_code`.
pub(crate) fn path_from_scalar(scalar: &ScalarValue) -> Result<VariantPath<'static>> {
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => {
            let parsed = VariantPath::try_from(s.as_str())
                .map_err(Into::<DataFusionError>::into)?;
            Ok(to_owned_path(&parsed))
        }
        ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None) => Ok(VariantPath::default()),
        ScalarValue::List(list_arr) => {
            if list_arr.is_null(0) {
                return Ok(VariantPath::default());
            }

            path_from_list_values(&list_arr.value(0))
        }
        other => exec_err!(
            "path must be a string or list of strings, got {}",
            other.data_type()
        ),
    }
}

fn path_from_list_values(values: &ArrayRef) -> Result<VariantPath<'static>> {
    let strings = try_parse_string_columnar(values)?;
    let elements = strings
        .iter()
        .map(|s| VariantPathElement::field(s.unwrap_or_default().to_string()))
        .collect();

    Ok(VariantPath::new(elements))
}

fn to_owned_path(path: &VariantPath<'_>) -> VariantPath<'static> {
    let elements = path
        .path()
        .iter()
        .map(|elem| match elem {
            VariantPathElement::Field { name } => {
                VariantPathElement::field(name.to_string())
            }
            VariantPathElement::Index { index } => VariantPathElement::index(*index),
        })
        .collect();

    VariantPath::new(elements)
}

pub fn invoke_variant_get_typed<T>(
    args: &ScalarFunctionArgs,
    scalar_from_option: fn(Option<T>) -> ScalarValue,
    array_from_values: fn(Vec<Option<T>>) -> ArrayRef,
    extract: for<'m, 'v> fn(Variant<'m, 'v>) -> Result<Option<T>>,
) -> Result<ColumnarValue> {
    let (variant_arg, path_arg) = match args.args.as_slice() {
        [variant_arg, path_arg] => (variant_arg, path_arg),
        _ => return exec_err!("expected 2 arguments"),
    };

    let variant_field = args
        .arg_fields
        .first()
        .ok_or_else(|| exec_datafusion_err!("expected argument field"))?;

    try_field_as_variant_array(variant_field.as_ref())?;

    let out = match (variant_arg, path_arg) {
        (ColumnarValue::Array(variant_array), ColumnarValue::Scalar(path_scalar)) => {
            let path = path_from_scalar(path_scalar)?;
            let variant_array = VariantArray::try_new(variant_array.as_ref())?;
            let values = variant_get_array_values(&variant_array, &path, extract)?;
            ColumnarValue::Array(array_from_values(values))
        }
        (ColumnarValue::Scalar(scalar_variant), ColumnarValue::Scalar(path_scalar)) => {
            let ScalarValue::Struct(variant_array) = scalar_variant else {
                return exec_err!("expected struct array");
            };

            let path = path_from_scalar(path_scalar)?;
            let variant_array = VariantArray::try_new(variant_array.as_ref())?;
            let value = variant_get_single_value(&variant_array, 0, &path, extract)?;

            ColumnarValue::Scalar(scalar_from_option(value))
        }
        (ColumnarValue::Array(variant_array), ColumnarValue::Array(paths)) => {
            if variant_array.len() != paths.len() {
                return exec_err!(
                    "expected variant array and paths to be of same length"
                );
            }

            let paths = try_parse_string_columnar(paths)?;
            let variant_array = VariantArray::try_new(variant_array.as_ref())?;

            let values = (0..variant_array.len())
                .map(|i| {
                    let path_str = paths[i].unwrap_or_default();
                    let path = VariantPath::try_from(path_str)
                        .map_err(Into::<DataFusionError>::into)?;

                    variant_get_single_value(&variant_array, i, &path, extract)
                })
                .collect::<Result<_>>()?;

            ColumnarValue::Array(array_from_values(values))
        }
        (ColumnarValue::Scalar(scalar_variant), ColumnarValue::Array(paths)) => {
            let ScalarValue::Struct(variant_array) = scalar_variant else {
                return exec_err!("expected struct array");
            };

            let variant_array = VariantArray::try_new(variant_array.as_ref())?;
            let paths = try_parse_string_columnar(paths)?;

            let values = paths
                .iter()
                .map(|path_str| {
                    let path_str = path_str.unwrap_or_default();
                    let path = VariantPath::try_from(path_str)
                        .map_err(Into::<DataFusionError>::into)?;
                    variant_get_single_value(&variant_array, 0, &path, extract)
                })
                .collect::<Result<_>>()?;

            ColumnarValue::Array(array_from_values(values))
        }
    };

    Ok(out)
}

/// This is similar to anyhow's ensure! macro
/// If the `pred` fails, it will return a DataFusionError
pub fn ensure(pred: bool, err_msg: &str) -> Result<()> {
    if !pred {
        return exec_err!("{}", err_msg);
    }

    Ok(())
}

// test related methods

#[cfg(test)]
pub fn build_variant_array_from_json(value: &serde_json::Value) -> VariantArray {
    let json_str = value.to_string();
    let mut builder = VariantArrayBuilder::new(1);
    builder.append_json(json_str.as_str()).unwrap();

    builder.build()
}

#[cfg(test)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "called with literal serde_json::json! values in tests"
)]
pub fn variant_scalar_from_json(json: serde_json::Value) -> ScalarValue {
    let mut builder = VariantArrayBuilder::new(1);
    builder.append_json(json.to_string().as_str()).unwrap();
    ScalarValue::Struct(Arc::new(builder.build().into()))
}

#[cfg(test)]
pub fn variant_array_from_json_rows(json_rows: &[serde_json::Value]) -> ArrayRef {
    let mut builder = VariantArrayBuilder::new(json_rows.len());
    for value in json_rows {
        builder.append_json(value.to_string().as_str()).unwrap();
    }
    let variant_array: StructArray = builder.build().into();
    Arc::new(variant_array) as ArrayRef
}

#[cfg(test)]
pub fn standard_variant_get_arg_fields() -> Vec<Arc<Field>> {
    vec![
        Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        ),
        Arc::new(Field::new("path", DataType::Utf8, true)),
    ]
}

#[cfg(test)]
pub fn build_variant_get_args(
    variant_input: ColumnarValue,
    path: ColumnarValue,
    return_data_type: DataType,
    arg_fields: Vec<Arc<Field>>,
) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args: vec![variant_input, path],
        return_field: Arc::new(Field::new("result", return_data_type, true)),
        arg_fields,
        number_rows: Default::default(),
        config_options: Default::default(),
    }
}

#[cfg(test)]
pub fn build_variant_array_from_json_array(
    jsons: &[Option<serde_json::Value>],
) -> VariantArray {
    let mut builder = VariantArrayBuilder::new(jsons.len());

    jsons.iter().for_each(|v| match v.as_ref() {
        Some(json) => builder.append_json(json.to_string().as_str()).unwrap(),
        None => builder.append_null(),
    });

    builder.build()
}
