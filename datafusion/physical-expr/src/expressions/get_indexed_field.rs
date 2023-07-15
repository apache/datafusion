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

use crate::array_expressions::{array_element, array_slice};
use crate::physical_expr::down_cast_any_ref;
use crate::struct_expressions::struct_extract;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_expr::{
    field_util::get_indexed_field as get_data_type_field, ColumnarValue,
};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

/// expression to get a field of a struct array.
#[derive(Debug, Hash)]
pub struct GetIndexedFieldExpr {
    arg: Arc<dyn PhysicalExpr>,
    key: Arc<dyn PhysicalExpr>,
    extra_key: Option<Arc<dyn PhysicalExpr>>,
}

impl GetIndexedFieldExpr {
    /// Create new get field expression
    pub fn new(
        arg: Arc<dyn PhysicalExpr>,
        key: Arc<dyn PhysicalExpr>,
        extra_key: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            arg,
            key,
            extra_key,
        }
    }

    /// Get the input key
    pub fn key(&self) -> &Arc<dyn PhysicalExpr> {
        &self.key
    }

    /// Get the input extra key
    pub fn extra_key(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.extra_key
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for GetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(extra_key) = &self.extra_key {
            write!(f, "({}).[{}:{}]", self.arg, self.key, extra_key)
        } else {
            write!(f, "({}).[{}]", self.arg, self.key)
        }
    }
}

impl PhysicalExpr for GetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let arg_dt = self.arg.data_type(input_schema)?;
        let key_dt = self.key.data_type(input_schema)?;
        let extra_key_dt = if let Some(extra_key) = &self.extra_key {
            Some(extra_key.data_type(input_schema)?)
        } else {
            None
        };
        get_data_type_field(&arg_dt, &key_dt, &extra_key_dt)
            .map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let arg_dt = self.arg.data_type(input_schema)?;
        let key_dt = self.key.data_type(input_schema)?;
        let extra_key_dt = if let Some(extra_key) = &self.extra_key {
            Some(extra_key.data_type(input_schema)?)
        } else {
            None
        };
        get_data_type_field(&arg_dt, &key_dt, &extra_key_dt).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(1);
        let key = self.key.evaluate(batch)?.into_array(1);
        if let Some(extra_key) = &self.extra_key {
            let extra_key = extra_key.evaluate(batch)?.into_array(1);
            match (array.data_type(), key.data_type(), extra_key.data_type()) {
                (DataType::List(_), DataType::Int64, DataType::Int64)
                | (DataType::List(_), DataType::Int64, DataType::Null)
                | (DataType::List(_), DataType::Null, DataType::Int64)
                | (DataType::List(_), DataType::Null, DataType::Null) => Ok(ColumnarValue::Array(array_slice(&[
                    array, key, extra_key
                ])?)),
                (DataType::List(_), key, extra_key) => Err(DataFusionError::Execution(
                    format!("get indexed field is only possible on lists with int64 indexes. \
                             Tried with {key:?} and {extra_key:?} indices"))),
                (dt, key, extra_key) => Err(DataFusionError::Execution(
                    format!("get indexed field is only possible on lists with int64 indexes or struct \
                             with utf8 indexes. Tried {dt:?} with {key:?} and {extra_key:?} indices"))),
            }
        } else {
            match (array.data_type(), key.data_type()) {
                (DataType::List(_), DataType::Int64)
                | (DataType::List(_), DataType::Null) => Ok(ColumnarValue::Array(array_element(&[
                    array, key
                ])?)),
                (DataType::Struct(_), DataType::Utf8) => Ok(ColumnarValue::Array(struct_extract(&[
                    array, key
                ])?)),
                (DataType::Struct(_), key) => Err(DataFusionError::Execution(
                    format!("get indexed field is only possible on struct with utf8 indexes. \
                             Tried with {key:?} index"))),
                (dt, key) => Err(DataFusionError::Execution(
                    format!("get indexed field is only possible on lists with int64 indexes or struct \
                             with utf8 indexes. Tried {dt:?} with {key:?} index"))),
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(GetIndexedFieldExpr::new(
            children[0].clone(),
            self.key.clone(),
            self.extra_key.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for GetIndexedFieldExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                if let Some(extra_key) = &self.extra_key {
                    self.arg.eq(&x.arg)
                        && self.key.eq(&x.key)
                        && extra_key.eq(&x.extra_key)
                } else {
                    self.arg.eq(&x.arg) && self.key.eq(&x.key)
                }
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};
    use arrow::array::{ArrayRef, Float64Array, GenericListArray, PrimitiveBuilder};
    use arrow::array::{
        Int64Array, Int64Builder, ListBuilder, StringBuilder, StructArray, StructBuilder,
    };
    use arrow::datatypes::{Float64Type, Int64Type};
    use arrow::{array::StringArray, datatypes::Field};
    use datafusion_common::cast::{as_int64_array, as_list_array, as_string_array};
    use datafusion_common::Result;

    fn build_list_arguments(
        list_of_lists: Vec<Vec<Option<&str>>>,
        list_of_keys: Vec<Option<i64>>,
        list_of_extra_keys: Vec<Option<i64>>,
    ) -> (GenericListArray<i32>, Int64Array, Int64Array) {
        let builder = StringBuilder::with_capacity(list_of_lists.len(), 1024);
        let mut list_builder = ListBuilder::new(builder);
        for values in list_of_lists {
            let builder = list_builder.values();
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(v) => builder.append_value(v),
                }
            }
            list_builder.append(true);
        }

        let key_array = Int64Array::from(list_of_keys);
        let extra_key_array = Int64Array::from(list_of_extra_keys);
        (list_builder.finish(), key_array, extra_key_array)
    }

    fn get_indexed_field_test(
        list_of_lists: Vec<Vec<Option<&str>>>,
        list_of_keys: Vec<Option<i64>>,
        list_of_extra_keys: Vec<Option<i64>>,
        expected: Vec<Option<&str>>,
    ) -> Result<()> {
        let schema = list_schema(&["l", "k", "ek"]);
        let (list_col, key_col, extra_key_col) =
            build_list_arguments(list_of_lists, list_of_keys, list_of_extra_keys);
        let expr = col("l", &schema).unwrap();
        let key = col("k", &schema).unwrap();
        let extra_key = col("ek", &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(list_col),
                Arc::new(key_col),
                Arc::new(extra_key_col),
            ],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new(expr, key, Some(extra_key)));
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = as_string_array(&result).expect("failed to downcast to StringArray");
        let expected = &StringArray::from(expected);
        assert_eq!(expected, result);
        Ok(())
    }

    fn list_schema(cols: &[&str]) -> Schema {
        Schema::new(vec![
            Field::new_list(cols[0], Field::new("item", DataType::Utf8, true), true),
            Field::new(cols[1], DataType::Int64, true),
            Field::new(cols[2], DataType::Int64, true),
        ])
    }

    #[test]
    fn get_indexed_field_list() -> Result<()> {
        let list_of_lists = vec![
            vec![Some("a"), Some("b"), None],
            vec![None, Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];
        let list_of_keys = vec![Some(1), Some(2), None];
        let list_of_extra_keys = vec![Some(2), None, Some(3)];
        let expected_list = vec![
            vec![Some("a"), None, Some("e")],
            vec![Some("b"), Some("c"), None],
            vec![None, Some("d"), Some("f")],
        ];

        for expected in expected_list.into_iter() {
            get_indexed_field_test(
                list_of_lists.clone(),
                list_of_keys.clone(),
                list_of_extra_keys.clone(),
                expected,
            )?;
        }
        Ok(())
    }

    #[test]
    fn get_indexed_field_empty_list() -> Result<()> {
        let schema = list_schema(&["l", "k", "ek"]);
        let builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(builder);
        let key_array = Int64Array::from(vec![Some(1), Some(2), None]);
        let expr = col("l", &schema).unwrap();
        let key = col("k", &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_builder.finish()), Arc::new(key_array)],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new(expr, key, None));
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        assert!(result.is_empty());
        Ok(())
    }

    fn get_indexed_field_test_failure(
        schema: Schema,
        expr: Arc<dyn PhysicalExpr>,
        key_expr: Arc<dyn PhysicalExpr>,
        key: Option<i64>,
        expected: &str,
    ) -> Result<()> {
        let builder = StringBuilder::with_capacity(3, 1024);
        let mut list_builder = ListBuilder::new(builder);
        let key_array = Int64Array::from(vec![key]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_builder.finish()), Arc::new(key_array)],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new(expr, key_expr, None));
        let r = expr.evaluate(&batch).map(|_| ());
        assert!(r.is_err());
        assert_eq!(format!("{}", r.unwrap_err()), expected);
        Ok(())
    }

    #[test]
    fn get_indexed_field_invalid_scalar() -> Result<()> {
        let schema = list_schema(&["l", "k", "ek"]);
        let expr = lit("a");
        let key_expr = lit("k");
        get_indexed_field_test_failure(
            schema, expr, key_expr, Some(0),
            "Execution error: get indexed field is only possible on lists with int64 indexes or \
             struct with utf8 indexes. Tried Utf8 with Int64(0) index")
    }

    #[test]
    fn get_indexed_field_invalid_list_index() -> Result<()> {
        let schema = list_schema(&["l", "k", "ek"]);
        let expr = col("l", &schema).unwrap();
        let key_expr = col("k", &schema).unwrap();
        get_indexed_field_test_failure(
            schema, expr, key_expr, Some(0),
            "Execution error: get indexed field is only possible on lists with int64 indexes. \
             Tried with Int8(0) index")
    }

    fn build_struct_arguments(
        fields: Vec<Field>,
        list_of_tuples: Vec<(Option<i64>, Vec<Option<&str>>)>,
    ) -> (StructArray, StringArray, StringArray, Int64Array) {
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

        let foo_key_array = StringArray::from(vec![Some("foo")]);
        let bar_key_array = StringArray::from(vec![Some("bar")]);
        let list_key_array = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        (
            builder.finish(),
            foo_key_array,
            bar_key_array,
            list_key_array,
        )
    }

    fn get_indexed_field_mixed_test(
        list_of_tuples: Vec<(Option<i64>, Vec<Option<&str>>)>,
        expected_strings: Vec<Vec<Option<&str>>>,
        expected_ints: Vec<Option<i64>>,
    ) -> Result<()> {
        let struct_col_name = "s";
        let fields = vec![
            Field::new("foo", DataType::Int64, true),
            Field::new_list("bar", Field::new("item", DataType::Utf8, true), true),
        ];
        let key_col_name = "i";
        let utf8_schema = Schema::new(vec![
            Field::new(
                struct_col_name,
                DataType::Struct(fields.clone().into()),
                true,
            ),
            Field::new(key_col_name, DataType::Utf8, true),
        ]);
        let int64_schema = Schema::new(vec![
            Field::new(
                struct_col_name,
                DataType::Struct(fields.clone().into()),
                true,
            ),
            Field::new(key_col_name, DataType::Int64, true),
        ]);
        let (struct_col, foo_key_col, bar_key_col, list_key_col) =
            build_struct_arguments(fields, list_of_tuples.clone());

        let struct_col_expr = col("s", &utf8_schema).unwrap();
        let key_col_expr = col("i", &utf8_schema).unwrap();
        let foo_batch = RecordBatch::try_new(
            Arc::new(utf8_schema.clone()),
            vec![Arc::new(struct_col.clone()), Arc::new(foo_key_col)],
        )?;
        let bar_batch = RecordBatch::try_new(
            Arc::new(utf8_schema.clone()),
            vec![Arc::new(struct_col.clone()), Arc::new(bar_key_col)],
        )?;
        let list_batch = RecordBatch::try_new(
            Arc::new(int64_schema),
            vec![Arc::new(struct_col.clone()), Arc::new(list_key_col)],
        )?;

        let get_field_expr = Arc::new(GetIndexedFieldExpr::new(
            struct_col_expr.clone(),
            key_col_expr.clone(),
            None,
        ));
        let result = get_field_expr
            .evaluate(&foo_batch)?
            .into_array(foo_batch.num_rows());
        let result = as_int64_array(&result)?;
        let expected = &Int64Array::from(expected_ints);
        assert_eq!(expected, result);

        let get_list_expr = Arc::new(GetIndexedFieldExpr::new(
            struct_col_expr,
            key_col_expr.clone(),
            None,
        ));
        let result = get_list_expr
            .evaluate(&bar_batch)?
            .into_array(bar_batch.num_rows());
        let result = as_list_array(&result)?;
        let (expected, _, _) = &build_list_arguments(
            list_of_tuples.into_iter().map(|t| t.1).collect(),
            vec![],
            vec![],
        );
        assert_eq!(expected, result);

        for expected in expected_strings.into_iter() {
            let get_nested_str_expr = Arc::new(GetIndexedFieldExpr::new(
                get_list_expr.clone(),
                key_col_expr.clone(),
                None,
            ));
            let result = get_nested_str_expr
                .evaluate(&list_batch)?
                .into_array(list_batch.num_rows());
            let result = as_string_array(&result)?;
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

    #[test]
    fn get_indexed_field_list_out_of_bounds() {
        let fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new_list("a", Field::new("item", DataType::Float64, true), true),
            Field::new("k", DataType::Int64, true),
        ];

        let schema = Schema::new(fields);
        let mut int_builder = PrimitiveBuilder::<Int64Type>::new();
        int_builder.append_value(1);

        let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Float64Type>::new());
        list_builder.values().append_value(1.0);
        list_builder.values().append_null();
        list_builder.values().append_value(3.0);
        list_builder.append(true);

        let key_array = Int64Array::from(vec![Some(0), Some(1), Some(3), Some(100)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(int_builder.finish()),
                Arc::new(list_builder.finish()),
                Arc::new(key_array),
            ],
        )
        .unwrap();

        let col_a = col("a", &schema).unwrap();
        let col_k = col("k", &schema).unwrap();
        // out of bounds index
        verify_index_evaluation(
            &batch,
            col_a.clone(),
            col_k.clone(),
            float64_array(None),
        );

        /*verify_index_evaluation(&batch, col_a.clone(), 1, float64_array(Some(1.0)));
        verify_index_evaluation(&batch, col_a.clone(), 2, float64_array(None));
        verify_index_evaluation(&batch, col_a.clone(), 3, float64_array(Some(3.0)));

        // out of bounds index
        verify_index_evaluation(&batch, col_a.clone(), 100, float64_array(None));*/
    }

    fn verify_index_evaluation(
        batch: &RecordBatch,
        arg: Arc<dyn PhysicalExpr>,
        key: Arc<dyn PhysicalExpr>,
        expected_result: ArrayRef,
    ) {
        let expr = Arc::new(GetIndexedFieldExpr::new(arg, key, None));
        let result = expr.evaluate(batch).unwrap().into_array(batch.num_rows());
        assert!(
            result == expected_result.clone(),
            "result: {result:?} != expected result: {expected_result:?}"
        );
        assert_eq!(result.data_type(), &DataType::Float64);
    }

    fn float64_array(value: Option<f64>) -> ArrayRef {
        match value {
            Some(v) => Arc::new(Float64Array::from_value(v, 1)),
            None => {
                let mut b = PrimitiveBuilder::<Float64Type>::new();
                b.append_null();
                Arc::new(b.finish())
            }
        }
    }
}
