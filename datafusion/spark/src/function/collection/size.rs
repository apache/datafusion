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

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, Int32Array, Int32Builder,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `size` function.
///
/// Returns the number of elements in an array or the number of key-value pairs in a map.
/// Returns null for null input.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSize {
    signature: Signature,
}

impl Default for SparkSize {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSize {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSize {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return plan_err!("size expects exactly 1 argument");
        }

        let input_field = &args.arg_fields[0];

        match input_field.data_type() {
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Null => {}
            dt => {
                return plan_err!(
                    "size function requires array or map types, got: {}",
                    dt
                );
            }
        }

        let mut out_nullable = input_field.is_nullable();

        let scala_null_present = args
            .scalar_arguments
            .iter()
            .any(|opt_s| opt_s.is_some_and(|sv| sv.is_null()));
        if scala_null_present {
            out_nullable = true;
        }

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int32,
            out_nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("size expects exactly 1 argument");
        }
        make_scalar_function(spark_size_inner, vec![])(&args.args)
    }
}

fn spark_size_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = &args[0];

    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            let mut builder = Int32Builder::with_capacity(list_array.len());
            for i in 0..list_array.len() {
                if list_array.is_null(i) {
                    builder.append_null();
                } else {
                    let len = list_array.value(i).len();
                    builder.append_value(len as i32)
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            let mut builder = Int32Builder::with_capacity(list_array.len());
            for i in 0..list_array.len() {
                if list_array.is_null(i) {
                    builder.append_null();
                } else {
                    let len = list_array.value(i).len();
                    builder.append_value(len as i32)
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::FixedSizeList(_, size) => {
            let list_array: &FixedSizeListArray = array.as_fixed_size_list();
            let fixed_size = *size;
            let result: Int32Array = (0..list_array.len())
                .map(|i| {
                    if list_array.is_null(i) {
                        None
                    } else {
                        Some(fixed_size)
                    }
                })
                .collect();

            Ok(Arc::new(result))
        }
        DataType::Map(_, _) => {
            let map_array = array.as_map();
            let mut builder = Int32Builder::with_capacity(map_array.len());

            for i in 0..map_array.len() {
                if map_array.is_null(i) {
                    builder.append_null();
                } else {
                    let len = map_array.value(i).len();
                    builder.append_value(len as i32)
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::Null => Ok(Arc::new(Int32Array::new_null(array.len()))),
        dt => {
            plan_err!("size function does not support type: {}", dt)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, MapArray, StringArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Fields};
    use datafusion_common::ScalarValue;
    use datafusion_expr::ReturnFieldArgs;

    #[test]
    fn test_size_nullability() {
        let size_fn = SparkSize::new();

        // Non-nullable list input -> non-nullable output
        let non_nullable_list = Arc::new(Field::new(
            "col",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ));
        let out = size_fn
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_list)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(!out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Int32);

        // Nullable list output -> nullable output
        let nullable_list = Arc::new(Field::new(
            "col",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        let out = size_fn
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_list)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(out.is_nullable());
    }

    #[test]
    fn test_size_with_null_scalar() {
        let size_fn = SparkSize::new();

        let non_nullable_list = Arc::new(Field::new(
            "col",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ));

        // With null scalar argument
        let null_scalar = ScalarValue::List(Arc::new(ListArray::new_null(
            Arc::new(Field::new("item", DataType::Int32, true)),
            1,
        )));
        let out = size_fn
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_list)],
                scalar_arguments: &[Some(&null_scalar)],
            })
            .unwrap();

        assert!(out.is_nullable());
    }

    #[test]
    fn test_size_list_array() -> Result<()> {
        // Create a list array: [[1, 2, 3], [4, 5], null, []]
        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = OffsetBuffer::new(vec![0, 3, 5, 5, 5].into());
        let nulls = NullBuffer::from(vec![true, true, false, true]);
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            Arc::new(values),
            Some(nulls),
        );

        let result = spark_size_inner(&[Arc::new(list_array)])?;
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), 3); // [1, 2, 3]
        assert_eq!(result.value(1), 2); // [4, 5]
        assert!(result.is_null(2)); // null
        assert_eq!(result.value(3), 0); // []

        Ok(())
    }

    #[test]
    fn test_size_map_array() -> Result<()> {
        // Create a map array with entries
        let keys = StringArray::from(vec!["a", "b", "c", "d"]);
        let values = Int32Array::from(vec![1, 2, 3, 4]);

        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        ));

        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(values) as ArrayRef,
            ),
        ]);

        // Map with 3 rows: {a:1, b:2}, {c:3}, null
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 4].into());
        let nulls = NullBuffer::from(vec![true, true, false]);
        let map_array =
            MapArray::new(entries_field, offsets, entries, Some(nulls), false);

        let result = spark_size_inner(&[Arc::new(map_array)])?;
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 2); // {a:1, b:2}
        assert_eq!(result.value(1), 1); // {c:3}
        assert!(result.is_null(2)); // null

        Ok(())
    }

    #[test]
    fn test_size_fixed_size_list() -> Result<()> {
        // Create a fixed size list of size 3
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let nulls = NullBuffer::from(vec![true, true, false]);
        let list_array = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            3,
            Arc::new(values),
            Some(nulls),
        );

        let result = spark_size_inner(&[Arc::new(list_array)])?;
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 3);
        assert_eq!(result.value(1), 3);
        assert!(result.is_null(2));

        Ok(())
    }
}
