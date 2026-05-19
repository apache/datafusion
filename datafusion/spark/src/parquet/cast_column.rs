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

//! [`SparkCastColumnExpr`] adapts a single column read from a Parquet file to
//! the type expected by the query, applying Spark-compatible conversion logic
//! for nested types and a handful of primitive cases that Arrow's cast does
//! not get right for Spark.

use std::fmt::{self, Display};
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, StructArray, make_array,
};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use super::options::SparkParquetOptions;
use super::parquet_support::spark_parquet_convert;

/// True if two data types are structurally equivalent (same buffer layout)
/// but may differ in the *names* of nested fields. Used to detect cases
/// where we only need to relabel an array's metadata rather than convert it.
fn types_differ_only_in_field_names(physical: &DataType, logical: &DataType) -> bool {
    match (physical, logical) {
        (DataType::List(pf), DataType::List(lf)) => {
            pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::LargeList(pf), DataType::LargeList(lf)) => {
            pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::Map(pf, p_sorted), DataType::Map(lf, l_sorted)) => {
            p_sorted == l_sorted
                && pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::Struct(pfields), DataType::Struct(lfields)) => {
            // For Struct types, field names are semantically meaningful (they
            // identify different columns), so we require name equality here.
            // This distinguishes from List/Map wrapper field names ("item" vs
            // "element") which are purely cosmetic.
            pfields.len() == lfields.len()
                && pfields.iter().zip(lfields.iter()).all(|(pf, lf)| {
                    pf.name() == lf.name()
                        && pf.is_nullable() == lf.is_nullable()
                        && (pf.data_type() == lf.data_type()
                            || types_differ_only_in_field_names(
                                pf.data_type(),
                                lf.data_type(),
                            ))
                })
        }
        _ => false,
    }
}

/// Recursively relabel an array so its `DataType` matches `target_type`.
/// Only changes metadata (field names, nullability flags in nested fields).
/// Does NOT change the underlying buffer data.
fn relabel_array(array: ArrayRef, target_type: &DataType) -> ArrayRef {
    if array.data_type() == target_type {
        return array;
    }
    match target_type {
        DataType::List(target_field) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values =
                relabel_array(Arc::clone(list.values()), target_field.data_type());
            Arc::new(ListArray::new(
                Arc::clone(target_field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(target_field) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values =
                relabel_array(Arc::clone(list.values()), target_field.data_type());
            Arc::new(LargeListArray::new(
                Arc::clone(target_field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        DataType::Map(target_entries_field, sorted) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = relabel_array(
                Arc::new(map.entries().clone()),
                target_entries_field.data_type(),
            );
            let entries_struct = entries.as_any().downcast_ref::<StructArray>().unwrap();
            Arc::new(MapArray::new(
                Arc::clone(target_entries_field),
                map.offsets().clone(),
                entries_struct.clone(),
                map.nulls().cloned(),
                *sorted,
            ))
        }
        DataType::Struct(target_fields) => {
            let struct_arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let columns: Vec<ArrayRef> = target_fields
                .iter()
                .zip(struct_arr.columns())
                .map(|(tf, col)| relabel_array(Arc::clone(col), tf.data_type()))
                .collect();
            Arc::new(StructArray::new(
                target_fields.clone(),
                columns,
                struct_arr.nulls().cloned(),
            ))
        }
        // Primitive types - shallow swap is safe.
        _ => {
            let data = array.to_data();
            let new_data = data
                .into_builder()
                .data_type(target_type.clone())
                .build()
                .expect("relabel_array: data layout must be compatible");
            make_array(new_data)
        }
    }
}

/// A column-level cast that adapts a Parquet column to its requested type
/// using Spark semantics for nested types and a handful of primitive cases
/// that Arrow's cast does not handle correctly for Spark (e.g. timestamp
/// micros → millis preserving Spark's truncation semantics, or struct/list/map
/// adaptation that follows Spark's `clipParquetGroupFields`).
#[derive(Debug, Clone, Eq)]
pub struct SparkCastColumnExpr {
    /// The physical expression producing the value to cast.
    expr: Arc<dyn PhysicalExpr>,
    /// The physical field of the input column.
    input_physical_field: FieldRef,
    /// The field type required by the query.
    target_field: FieldRef,
    /// Options forwarded to Arrow cast.
    cast_options: CastOptions<'static>,
    /// Spark parquet options for complex nested type conversions.
    /// When present, enables [`spark_parquet_convert`] as a fallback.
    parquet_options: Option<SparkParquetOptions>,
    /// Pre-computed `input_physical_field.data_type() == target_field.data_type()`.
    /// Skips the per-batch recursive `DataType` comparison in [`Self::evaluate`]
    /// for the common pass-through case (the input is structurally identical
    /// to the target and only needs a metadata relabel — but the relabel
    /// itself is detected separately by [`types_differ_only_in_field_names`]).
    types_match: bool,
}

// Manually derive `PartialEq`/`Hash` because `Arc<dyn PhysicalExpr>` does not
// implement them by default for the trait object.
impl PartialEq for SparkCastColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_physical_field.eq(&other.input_physical_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
            && self.parquet_options.eq(&other.parquet_options)
    }
}

impl Hash for SparkCastColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_physical_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
        self.parquet_options.hash(state);
    }
}

impl SparkCastColumnExpr {
    /// Create a new [`SparkCastColumnExpr`].
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        physical_field: FieldRef,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        let types_match = physical_field.data_type() == target_field.data_type();
        Self {
            expr,
            input_physical_field: physical_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
            parquet_options: None,
            types_match,
        }
    }

    /// Attach Spark parquet options to enable complex nested type conversions.
    pub fn with_parquet_options(mut self, options: SparkParquetOptions) -> Self {
        self.parquet_options = Some(options);
        self
    }
}

impl Display for SparkCastColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SPARK_CAST_COLUMN({} AS {})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for SparkCastColumnExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;

        if self.types_match {
            return Ok(value);
        }

        let input_physical_field = self.input_physical_field.data_type();
        let target_field = self.target_field.data_type();

        match (input_physical_field, target_field) {
            // Nested types that differ only in field names (e.g., List
            // element named "item" vs "element", or Map entries named
            // "key_value" vs "entries"). Re-label the array so the DataType
            // metadata matches the logical schema.
            (physical, logical)
                if physical != logical
                    && types_differ_only_in_field_names(physical, logical) =>
            {
                match value {
                    ColumnarValue::Array(array) => {
                        let relabeled = relabel_array(array, logical);
                        Ok(ColumnarValue::Array(relabeled))
                    }
                    other => Ok(other),
                }
            }
            // Fallback: use spark_parquet_convert for complex nested type
            // conversions (e.g., List<Struct{a,b,c}> → List<Struct{a,c}>,
            // map field selection, etc.).
            _ => {
                if let Some(parquet_options) = &self.parquet_options {
                    let converted =
                        spark_parquet_convert(value, target_field, parquet_options)?;
                    Ok(converted)
                } else {
                    Ok(value)
                }
            }
        }
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().expect("SparkCastColumnExpr child");
        let mut new_expr = Self::new(
            child,
            Arc::clone(&self.input_physical_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
        );
        if let Some(opts) = &self.parquet_options {
            new_expr = new_expr.with_parquet_options(opts.clone());
        }
        Ok(Arc::new(new_expr))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int32Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, TimeUnit};
    use datafusion_physical_expr::expressions::Column;

    #[test]
    fn test_relabel_list_field_name() {
        // Physical: List(Field("item", Int32))
        // Logical:  List(Field("element", Int32))
        let physical_field = Arc::new(Field::new("item", DataType::Int32, true));
        let logical_field = Arc::new(Field::new("element", DataType::Int32, true));

        let values = Int32Array::from(vec![1, 2, 3]);
        let list = ListArray::new(
            physical_field,
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(values),
            None,
        );
        let array: ArrayRef = Arc::new(list);

        let target_type = DataType::List(Arc::clone(&logical_field));
        let result = relabel_array(array, &target_type);
        assert_eq!(result.data_type(), &target_type);
    }

    #[test]
    fn test_relabel_map_entries_field_name() {
        // Physical: Map(Field("key_value", Struct{key, value}))
        // Logical:  Map(Field("entries", Struct{key, value}))
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let struct_fields =
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]);

        let physical_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(struct_fields.clone()),
            false,
        ));
        let logical_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(struct_fields.clone()),
            false,
        ));

        let keys = StringArray::from(vec!["a", "b"]);
        let values = Int32Array::from(vec![1, 2]);
        let entries =
            StructArray::new(struct_fields, vec![Arc::new(keys), Arc::new(values)], None);
        let map = MapArray::new(
            physical_entries_field,
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        );
        let array: ArrayRef = Arc::new(map);

        let target_type = DataType::Map(logical_entries_field, false);
        let result = relabel_array(array, &target_type);
        assert_eq!(result.data_type(), &target_type);
    }

    #[test]
    fn test_evaluate_micros_to_millis_array() {
        use crate::parquet::options::EvalMode;

        let input_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let schema = Schema::new(vec![Arc::clone(&input_field)]);

        let target_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ));

        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));

        let cast_expr =
            SparkCastColumnExpr::new(col_expr, input_field, target_field, None)
                .with_parquet_options(SparkParquetOptions::new(
                    EvalMode::Legacy,
                    "UTC",
                    false,
                ));

        let micros_array: TimestampMicrosecondArray =
            vec![Some(1_000_000), Some(2_000_000), None].into();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(micros_array)]).unwrap();

        let result = cast_expr.evaluate(&batch).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let millis_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("Expected TimestampMillisecondArray");
                assert_eq!(millis_array.value(0), 1000);
                assert_eq!(millis_array.value(1), 2000);
                assert!(millis_array.is_null(2));
            }
            _ => panic!("Expected Array result"),
        }
    }
}
