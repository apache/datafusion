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

//! Spark-compatible Parquet conversion routines.
//!
//! [`spark_parquet_convert`] mirrors Spark's vectorized Parquet reader's
//! per-column type conversion. It is invoked by [`crate::parquet::cast_column::SparkCastColumnExpr`]
//! to handle nested types (Struct, List, Map) and the few primitive
//! conversions that Arrow's `cast` does not get right for Spark semantics
//! (e.g. INT96 timezone relabeling, FixedSizeBinary(16) UUID rendering).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, ListArray, MapArray,
    StringArray, StructArray, cast::AsArray, new_null_array, types::Int32Type,
    types::TimestampMicrosecondType,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{CastOptions, can_cast_types, cast_with_options, take};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use arrow::util::display::FormatOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::options::SparkParquetOptions;

const TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");

const PARQUET_CAST_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

/// Spark-compatible Parquet conversion. Defers to Arrow's `cast` where that is
/// known to be compatible with Spark, and applies custom logic for the cases
/// where Spark's vectorized Parquet reader differs (struct field selection,
/// list/map adaptation, INT96 timezone handling, FixedSizeBinary(16) UUIDs).
pub fn spark_parquet_convert(
    arg: ColumnarValue,
    data_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(parquet_convert_array(
            array,
            data_type,
            parquet_options,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            // Normally a CAST(scalar) is folded on the Spark JVM side. However,
            // for some cases (e.g. scalar subqueries) Spark does not fold the
            // cast, so we have to handle it here.
            let array = scalar.to_array()?;
            let scalar = ScalarValue::try_from_array(
                &parquet_convert_array(array, data_type, parquet_options)?,
                0,
            )?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

fn parquet_convert_array(
    array: ArrayRef,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> Result<ArrayRef> {
    use DataType::*;
    let from_type = array.data_type().clone();

    // Strip dictionary encoding before converting; the result is rebuilt as a
    // dictionary if the target type is itself dictionary-encoded.
    let array = match &from_type {
        Dictionary(key_type, value_type)
            if key_type.as_ref() == &Int32
                && (value_type.as_ref() == &Utf8
                    || value_type.as_ref() == &LargeUtf8) =>
        {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected a dictionary array");

            let casted_dictionary = DictionaryArray::<Int32Type>::new(
                dict_array.keys().clone(),
                parquet_convert_array(
                    Arc::clone(dict_array.values()),
                    to_type,
                    parquet_options,
                )?,
            );

            let casted_result = match to_type {
                Dictionary(_, _) => Arc::new(casted_dictionary.clone()),
                _ => take(casted_dictionary.values().as_ref(), dict_array.keys(), None)?,
            };
            return Ok(casted_result);
        }
        _ => array,
    };
    let from_type = array.data_type();

    match (from_type, to_type) {
        (Struct(_), Struct(_)) => parquet_convert_struct_to_struct(
            array.as_struct(),
            from_type,
            to_type,
            parquet_options,
        ),
        (List(_), List(to_inner_type)) => {
            let list_arr: &ListArray = array.as_list();
            let cast_field = parquet_convert_array(
                Arc::clone(list_arr.values()),
                to_inner_type.data_type(),
                parquet_options,
            )?;

            Ok(Arc::new(ListArray::new(
                Arc::clone(to_inner_type),
                list_arr.offsets().clone(),
                cast_field,
                list_arr.nulls().cloned(),
            )))
        }
        // INT96 columns surface as `Timestamp(Microsecond, None)` after the
        // first arrow-rs read pass, but Spark stores them as
        // `Timestamp(Microsecond, Some("UTC"))`. The values are identical;
        // only the timezone metadata is different, so reinterpret rather
        // than convert.
        (
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Microsecond, Some(tz)),
        ) => Ok(Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .reinterpret_cast::<TimestampMicrosecondType>()
                .with_timezone(Arc::clone(tz)),
        )),
        (Map(_, ordered_from), Map(_, ordered_to)) if ordered_from == ordered_to => {
            parquet_convert_map_to_map(
                array.as_map(),
                to_type,
                parquet_options,
                *ordered_to,
            )
        }
        // Iceberg stores UUIDs as 16-byte fixed binary but Spark surfaces them
        // as their string representation. Arrow does not support casting
        // FixedSizeBinary to Utf8, so we render the UUIDs ourselves.
        (FixedSizeBinary(16), Utf8) => {
            let binary_array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("Expected a FixedSizeBinaryArray");

            let string_array: StringArray = binary_array
                .iter()
                .map(|opt_bytes| {
                    opt_bytes.map(|bytes| {
                        let uuid = uuid::Uuid::from_bytes(
                            bytes.try_into().expect("Expected 16 bytes"),
                        );
                        uuid.to_string()
                    })
                })
                .collect();

            Ok(Arc::new(string_array))
        }
        // If Arrow cast supports the cast, delegate to it.
        _ if can_cast_types(from_type, to_type) => {
            Ok(cast_with_options(&array, to_type, &PARQUET_CAST_OPTIONS)?)
        }
        _ => Ok(array),
    }
}

/// Read the Parquet field id from arrow-rs's `PARQUET_FIELD_ID_META_KEY`.
fn field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

/// Cast between struct types based on logic in
/// `org.apache.spark.sql.catalyst.expressions.Cast#castStruct`.
fn parquet_convert_struct_to_struct(
    array: &StructArray,
    from_type: &DataType,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> Result<ArrayRef> {
    let (DataType::Struct(from_fields), DataType::Struct(to_fields)) =
        (from_type, to_type)
    else {
        return Err(DataFusionError::Internal(format!(
            "parquet_convert_struct_to_struct expected struct types, got {from_type} -> {to_type}"
        )));
    };

    // Match `from` (file) fields to `to` (logical) fields. Mirrors Spark's
    // `clipParquetGroupFields`: when the logical struct carries Parquet field IDs
    // anywhere, ID-bearing logical fields match ONLY by ID; non-ID-bearing fields
    // fall back to name match. When no logical field carries an ID, fall back to
    // name match across the board.
    let should_match_by_id =
        parquet_options.use_field_id && to_fields.iter().any(|f| field_id(f).is_some());

    let from_id_to_index: HashMap<i32, usize> = if should_match_by_id {
        let mut map = HashMap::new();
        for (i, field) in from_fields.iter().enumerate() {
            if let Some(id) = field_id(field) {
                map.entry(id).or_insert(i);
            }
        }
        map
    } else {
        HashMap::new()
    };

    let normalize_name = |name: &str| -> String {
        if parquet_options.case_sensitive {
            name.to_string()
        } else {
            name.to_lowercase()
        }
    };
    let mut field_name_to_index_map = HashMap::new();
    for (i, field) in from_fields.iter().enumerate() {
        field_name_to_index_map.insert(normalize_name(field.name()), i);
    }

    let mut field_overlap = false;
    let mut cast_fields: Vec<ArrayRef> = Vec::with_capacity(to_fields.len());
    for to_field in to_fields.iter() {
        let from_index = match (should_match_by_id, field_id(to_field)) {
            // Spark treats a missing ID match as a missing column rather than
            // falling back to name match.
            (true, Some(id)) => from_id_to_index.get(&id).copied(),
            _ => field_name_to_index_map
                .get(&normalize_name(to_field.name()))
                .copied(),
        };

        if let Some(from_index) = from_index {
            cast_fields.push(parquet_convert_array(
                Arc::clone(array.column(from_index)),
                to_field.data_type(),
                parquet_options,
            )?);
            field_overlap = true;
        } else {
            cast_fields.push(new_null_array(to_field.data_type(), array.len()));
        }
    }

    // When the file's struct contains none of the requested fields, the
    // returned validity buffer depends on Spark's
    // `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing` (SPARK-53535,
    // Spark 4.1+). Legacy mode marks the whole column null; the new default
    // preserves the file's parent-row nullness so non-null parents materialize
    // as a struct of all-null fields.
    let nulls =
        if !field_overlap && parquet_options.return_null_struct_if_all_fields_missing {
            Some(NullBuffer::new_null(array.len()))
        } else {
            array.nulls().cloned()
        };

    Ok(Arc::new(StructArray::new(
        to_fields.clone(),
        cast_fields,
        nulls,
    )))
}

/// Cast a map type to another map type. Recursively calls
/// [`parquet_convert_array`] for the keys and values rather than relying on
/// Arrow's cast, so nested type adaptations propagate.
fn parquet_convert_map_to_map(
    from: &MapArray,
    to_data_type: &DataType,
    parquet_options: &SparkParquetOptions,
    to_ordered: bool,
) -> Result<ArrayRef> {
    let DataType::Map(entries_field, _) = to_data_type else {
        return Err(DataFusionError::Internal(format!(
            "Expected MapType. Got: {to_data_type}"
        )));
    };

    let key_field = key_field(entries_field).ok_or_else(|| {
        DataFusionError::Internal("map is missing key field".to_string())
    })?;
    let value_field = value_field(entries_field).ok_or_else(|| {
        DataFusionError::Internal("map is missing value field".to_string())
    })?;

    let key_array = parquet_convert_array(
        Arc::clone(from.keys()),
        key_field.data_type(),
        parquet_options,
    )?;
    let value_array = parquet_convert_array(
        Arc::clone(from.values()),
        value_field.data_type(),
        parquet_options,
    )?;

    Ok(Arc::new(MapArray::new(
        Arc::clone(entries_field),
        from.offsets().clone(),
        StructArray::new(
            Fields::from(vec![key_field, value_field]),
            vec![key_array, value_array],
            from.entries().nulls().cloned(),
        ),
        from.nulls().cloned(),
        to_ordered,
    )))
}

/// Returns the key field from a map's entries struct.
fn key_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.first().cloned()
    } else {
        None
    }
}

/// Returns the value field from a map's entries struct.
fn value_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.get(1).cloned()
    } else {
        None
    }
}
