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

//! get field of a `ListArray`

use crate::PhysicalExpr;
use arrow::array::Array;
use arrow::array::{ListArray, StructArray};
use arrow::compute::concat;

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    field_util::get_indexed_field as get_data_type_field, ColumnarValue,
};
use std::convert::TryInto;
use std::fmt::Debug;
use std::{any::Any, sync::Arc};

/// expression to get a field of a struct array.
#[derive(Debug)]
pub struct GetIndexedFieldExpr {
    arg: Arc<dyn PhysicalExpr>,
    key: ScalarValue,
}

impl GetIndexedFieldExpr {
    /// Create new get field expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, key: ScalarValue) -> Self {
        Self { arg, key }
    }

    /// Get the input key
    pub fn key(&self) -> &ScalarValue {
        &self.key
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for GetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).[{}]", self.arg, self.key)
    }
}

impl PhysicalExpr for GetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(1);
        match (array.data_type(), &self.key) {
            (DataType::List(_) | DataType::Struct(_), _) if self.key.is_null() => {
                let scalar_null: ScalarValue = array.data_type().try_into()?;
                Ok(ColumnarValue::Scalar(scalar_null))
            }
            (DataType::List(_), ScalarValue::Int64(Some(i))) => {
                let as_list_array =
                    array.as_any().downcast_ref::<ListArray>().unwrap();

                if *i < 1 || as_list_array.is_empty() {
                    let scalar_null: ScalarValue = array.data_type().try_into()?;
                    return Ok(ColumnarValue::Scalar(scalar_null))
                }

                let sliced_array: Vec<Arc<dyn Array>> = as_list_array
                    .iter()
                    .filter_map(|o| match o {
                        Some(list) => if *i as usize > list.len() {
                            None
                        } else {
                            Some(list.slice((*i -1) as usize, 1))
                        },
                        None => None
                    })
                    .collect();

                // concat requires input of at least one array
                if sliced_array.is_empty() {
                    let scalar_null: ScalarValue = array.data_type().try_into()?;
                    Ok(ColumnarValue::Scalar(scalar_null))
                } else {
                    let vec = sliced_array.iter().map(|a| a.as_ref()).collect::<Vec<&dyn Array>>();
                    let iter = concat(vec.as_slice()).unwrap();

                    Ok(ColumnarValue::Array(iter))
                }
            }
            (DataType::Struct(_), ScalarValue::Utf8(Some(k))) => {
                let as_struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                match as_struct_array.column_by_name(k) {
                    None => Err(DataFusionError::Execution(format!("get indexed field {} not found in struct", k))),
                    Some(col) => Ok(ColumnarValue::Array(col.clone()))
                }
            }
            (DataType::List(_), key) => Err(DataFusionError::Execution(format!("get indexed field is only possible on lists with int64 indexes. Tried with {:?} index", key))),
            (DataType::Struct(_), key) => Err(DataFusionError::Execution(format!("get indexed field is only possible on struct with utf8 indexes. Tried with {:?} index", key))),
            (dt, key) => Err(DataFusionError::Execution(format!("get indexed field is only possible on lists with int64 indexes or struct with utf8 indexes. Tried {:?} with {:?} index", dt, key))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};
    use arrow::array::GenericListArray;
    use arrow::array::{
        Int64Array, Int64Builder, ListBuilder, StringBuilder, StructArray, StructBuilder,
    };
    use arrow::{array::StringArray, datatypes::Field};
    use datafusion_common::Result;

    fn build_utf8_lists(list_of_lists: Vec<Vec<Option<&str>>>) -> GenericListArray<i32> {
        let builder = StringBuilder::with_capacity(list_of_lists.len(), 1024);
        let mut lb = ListBuilder::new(builder);
        for values in list_of_lists {
            let builder = lb.values();
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(v) => builder.append_value(v),
                }
            }
            lb.append(true);
        }

        lb.finish()
    }

    fn get_indexed_field_test(
        list_of_lists: Vec<Vec<Option<&str>>>,
        index: i64,
        expected: Vec<Option<&str>>,
    ) -> Result<()> {
        let schema = list_schema("l");
        let list_col = build_utf8_lists(list_of_lists);
        let expr = col("l", &schema).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_col)])?;
        let key = ScalarValue::Int64(Some(index));
        let expr = Arc::new(GetIndexedFieldExpr::new(expr, key));
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("failed to downcast to StringArray");
        let expected = &StringArray::from(expected);
        assert_eq!(expected, result);
        Ok(())
    }

    fn list_schema(col: &str) -> Schema {
        Schema::new(vec![Field::new(
            col,
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            true,
        )])
    }

    #[test]
    fn get_indexed_field_list() -> Result<()> {
        let list_of_lists = vec![
            vec![Some("a"), Some("b"), None],
            vec![None, Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];
        let expected_list = vec![
            vec![Some("a"), None, Some("e")],
            vec![Some("b"), Some("c"), None],
            vec![None, Some("d"), Some("f")],
        ];

        for (i, expected) in expected_list.into_iter().enumerate() {
            get_indexed_field_test(list_of_lists.clone(), (i + 1) as i64, expected)?;
        }
        Ok(())
    }

    #[test]
    fn get_indexed_field_empty_list() -> Result<()> {
        let schema = list_schema("l");
        let builder = StringBuilder::new();
        let mut lb = ListBuilder::new(builder);
        let expr = col("l", &schema).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(lb.finish())])?;
        let key = ScalarValue::Int64(Some(1));
        let expr = Arc::new(GetIndexedFieldExpr::new(expr, key));
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        assert!(result.is_empty());
        Ok(())
    }

    fn get_indexed_field_test_failure(
        schema: Schema,
        expr: Arc<dyn PhysicalExpr>,
        key: ScalarValue,
        expected: &str,
    ) -> Result<()> {
        let builder = StringBuilder::with_capacity(3, 1024);
        let mut lb = ListBuilder::new(builder);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(lb.finish())])?;
        let expr = Arc::new(GetIndexedFieldExpr::new(expr, key));
        let r = expr.evaluate(&batch).map(|_| ());
        assert!(r.is_err());
        assert_eq!(format!("{}", r.unwrap_err()), expected);
        Ok(())
    }

    #[test]
    fn get_indexed_field_invalid_scalar() -> Result<()> {
        let schema = list_schema("l");
        let expr = lit("a");
        get_indexed_field_test_failure(schema, expr,  ScalarValue::Int64(Some(0)), "Execution error: get indexed field is only possible on lists with int64 indexes or struct with utf8 indexes. Tried Utf8 with Int64(0) index")
    }

    #[test]
    fn get_indexed_field_invalid_list_index() -> Result<()> {
        let schema = list_schema("l");
        let expr = col("l", &schema).unwrap();
        get_indexed_field_test_failure(schema, expr,  ScalarValue::Int8(Some(0)), "Execution error: get indexed field is only possible on lists with int64 indexes. Tried with Int8(0) index")
    }

    fn build_struct(
        fields: Vec<Field>,
        list_of_tuples: Vec<(Option<i64>, Vec<Option<&str>>)>,
    ) -> StructArray {
        let foo_builder = Int64Array::builder(list_of_tuples.len());
        let str_builder = StringBuilder::with_capacity(list_of_tuples.len(), 1024);
        let bar_builder = ListBuilder::new(str_builder);
        let mut builder = StructBuilder::new(
            fields,
            vec![Box::new(foo_builder), Box::new(bar_builder)],
        );
        for (int_value, list_value) in list_of_tuples {
            let fb = builder.field_builder::<Int64Builder>(0).unwrap();
            match int_value {
                None => fb.append_null(),
                Some(v) => fb.append_value(v),
            };
            builder.append(true);
            let lb = builder
                .field_builder::<ListBuilder<StringBuilder>>(1)
                .unwrap();
            for str_value in list_value {
                match str_value {
                    None => lb.values().append_null(),
                    Some(v) => lb.values().append_value(v),
                };
            }
            lb.append(true);
        }
        builder.finish()
    }

    fn get_indexed_field_mixed_test(
        list_of_tuples: Vec<(Option<i64>, Vec<Option<&str>>)>,
        expected_strings: Vec<Vec<Option<&str>>>,
        expected_ints: Vec<Option<i64>>,
    ) -> Result<()> {
        let struct_col = "s";
        let fields = vec![
            Field::new("foo", DataType::Int64, true),
            Field::new(
                "bar",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ];
        let schema = Schema::new(vec![Field::new(
            struct_col,
            DataType::Struct(fields.clone()),
            true,
        )]);
        let struct_col = build_struct(fields, list_of_tuples.clone());

        let struct_col_expr = col("s", &schema).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_col)])?;

        let int_field_key = ScalarValue::Utf8(Some("foo".to_string()));
        let get_field_expr = Arc::new(GetIndexedFieldExpr::new(
            struct_col_expr.clone(),
            int_field_key,
        ));
        let result = get_field_expr
            .evaluate(&batch)?
            .into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("failed to downcast to Int64Array");
        let expected = &Int64Array::from(expected_ints);
        assert_eq!(expected, result);

        let list_field_key = ScalarValue::Utf8(Some("bar".to_string()));
        let get_list_expr =
            Arc::new(GetIndexedFieldExpr::new(struct_col_expr, list_field_key));
        let result = get_list_expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap_or_else(|| panic!("failed to downcast to ListArray : {:?}", result));
        let expected =
            &build_utf8_lists(list_of_tuples.into_iter().map(|t| t.1).collect());
        assert_eq!(expected, result);

        for (i, expected) in expected_strings.into_iter().enumerate() {
            let get_nested_str_expr = Arc::new(GetIndexedFieldExpr::new(
                get_list_expr.clone(),
                ScalarValue::Int64(Some((i + 1) as i64)),
            ));
            let result = get_nested_str_expr
                .evaluate(&batch)?
                .into_array(batch.num_rows());
            let result = result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| {
                    panic!("failed to downcast to StringArray : {:?}", result)
                });
            let expected = &StringArray::from(expected);
            assert_eq!(expected, result);
        }
        Ok(())
    }

    #[test]
    fn get_indexed_field_struct() -> Result<()> {
        let list_of_structs = vec![
            (Some(10), vec![Some("a"), Some("b"), None]),
            (Some(15), vec![None, Some("c"), Some("d")]),
            (None, vec![Some("e"), None, Some("f")]),
        ];

        let expected_list = vec![
            vec![Some("a"), None, Some("e")],
            vec![Some("b"), Some("c"), None],
            vec![None, Some("d"), Some("f")],
        ];

        let expected_ints = vec![Some(10), Some(15), None];

        get_indexed_field_mixed_test(
            list_of_structs.clone(),
            expected_list,
            expected_ints,
        )?;
        Ok(())
    }
}
