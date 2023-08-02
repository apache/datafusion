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
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{cast::as_struct_array, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    field_util::get_indexed_field as get_data_type_field, ColumnarValue,
};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

/// Key of `GetIndexedFieldExpr`.
/// This structure is needed to separate the responsibilities of the key for `DataType::List` and `DataType::Struct`.
/// If we use index with `DataType::List`, then we use the `list_key` argument with `struct_key` equal to `None`.
/// If we use index with `DataType::Struct`, then we use the `struct_key` argument with `list_key` equal to `None`.
/// `list_key` can be any expression, unlike `struct_key` which can only be `ScalarValue::Utf8`.
#[derive(Clone, Hash, Debug)]
pub struct GetIndexedFieldExprKey {
    /// The key expression for `DataType::List`
    list_key: Option<Arc<dyn PhysicalExpr>>,
    /// The key expression for `DataType::Struct`
    struct_key: Option<ScalarValue>,
}

impl GetIndexedFieldExprKey {
    /// Create new get field expression key
    pub fn new(
        list_key: Option<Arc<dyn PhysicalExpr>>,
        struct_key: Option<ScalarValue>,
    ) -> Self {
        Self {
            list_key,
            struct_key,
        }
    }

    /// Get the key expression for `DataType::List`
    pub fn list_key(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.list_key
    }

    /// Get the key expression for `DataType::Struct`
    pub fn struct_key(&self) -> &Option<ScalarValue> {
        &self.struct_key
    }
}

impl std::fmt::Display for GetIndexedFieldExprKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(list_key) = &self.list_key {
            write!(f, "{}", list_key)
        } else {
            write!(f, "{}", self.struct_key.clone().unwrap())
        }
    }
}

impl PartialEq<dyn Any> for GetIndexedFieldExprKey {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                if let Some(list_key) = &self.list_key {
                    list_key.eq(&x.list_key.clone().unwrap())
                } else {
                    self.struct_key
                        .clone()
                        .unwrap()
                        .eq(&x.struct_key.clone().unwrap())
                }
            })
            .unwrap_or(false)
    }
}

/// Expression to get a field of a struct array.
#[derive(Debug, Hash)]
pub struct GetIndexedFieldExpr {
    /// The expression to find
    arg: Arc<dyn PhysicalExpr>,
    /// The key statement
    key: GetIndexedFieldExprKey,
    /// The extra key (it can be used only with `DataType::List`)
    extra_key: Option<Arc<dyn PhysicalExpr>>,
}

impl GetIndexedFieldExpr {
    /// Create new get field expression
    pub fn new(
        arg: Arc<dyn PhysicalExpr>,
        key: GetIndexedFieldExprKey,
        extra_key: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            arg,
            key,
            extra_key,
        }
    }

    /// Get the input key
    pub fn key(&self) -> &GetIndexedFieldExprKey {
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
        let key = if let Some(list_key) = &self.key.list_key {
            (Some(list_key.data_type(input_schema)?), None)
        } else {
            (None, self.key.struct_key.clone())
        };
        let extra_key_dt = if let Some(extra_key) = &self.extra_key {
            Some(extra_key.data_type(input_schema)?)
        } else {
            None
        };
        get_data_type_field(&arg_dt, &key, &extra_key_dt).map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let arg_dt = self.arg.data_type(input_schema)?;
        let key = if let Some(list_key) = &self.key.list_key {
            (Some(list_key.data_type(input_schema)?), None)
        } else {
            (None, self.key.struct_key.clone())
        };
        let extra_key_dt = if let Some(extra_key) = &self.extra_key {
            Some(extra_key.data_type(input_schema)?)
        } else {
            None
        };
        get_data_type_field(&arg_dt, &key, &extra_key_dt).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(batch.num_rows());
        if let Some(extra_key) = &self.extra_key {
            let list_key = self
                .key
                .list_key
                .clone()
                .unwrap()
                .evaluate(batch)?
                .into_array(batch.num_rows());
            let extra_key = extra_key.evaluate(batch)?.into_array(batch.num_rows());
            match (array.data_type(), list_key.data_type(), extra_key.data_type()) {
                (DataType::List(_), DataType::Int64, DataType::Int64) => Ok(ColumnarValue::Array(array_slice(&[
                    array, list_key, extra_key
                ])?)),
                (DataType::List(_), key, extra_key) => Err(DataFusionError::Execution(
                    format!("get indexed field is only possible on lists with int64 indexes. \
                             Tried with {key:?} and {extra_key:?} indices"))),
                (dt, key, extra_key) => Err(DataFusionError::Execution(
                    format!("get indexed field is only possible on lists with int64 indexes or struct \
                             with utf8 indexes. Tried {dt:?} with {key:?} and {extra_key:?} indices"))),
            }
        } else if let Some(list_key) = &self.key.list_key {
            let list_key = list_key.evaluate(batch)?.into_array(batch.num_rows());
            match (array.data_type(), list_key.data_type()) {
                    (DataType::List(_), DataType::Int64) => Ok(ColumnarValue::Array(array_element(&[
                        array, list_key
                    ])?)),
                    (DataType::List(_), key) => Err(DataFusionError::Execution(
                        format!("get indexed field is only possible on lists with int64 indexes. \
                            Tried with {key:?} index"))),
                    (dt, key) => Err(DataFusionError::Execution(
                                format!("get indexed field is only possible on lists with int64 indexes or struct \
                                         with utf8 indexes. Tried {dt:?} with {key:?} index"))),
                }
        } else {
            let struct_key = self.key.struct_key.clone().unwrap();
            match (array.data_type(), struct_key) {
                    (DataType::Struct(_), ScalarValue::Utf8(Some(k))) => {
                        let as_struct_array = as_struct_array(&array)?;
                        match as_struct_array.column_by_name(&k) {
                            None => Err(DataFusionError::Execution(
                                format!("get indexed field {k} not found in struct"))),
                            Some(col) => Ok(ColumnarValue::Array(col.clone()))
                        }
                    }
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
    use crate::expressions::col;
    use arrow::array::new_empty_array;
    use arrow::array::{ArrayRef, GenericListArray};
    use arrow::array::{
        BooleanArray, Int64Array, ListBuilder, StringBuilder, StructArray,
    };
    use arrow::datatypes::Fields;
    use arrow::{array::StringArray, datatypes::Field};
    use datafusion_common::cast::{as_boolean_array, as_list_array, as_string_array};
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

    #[test]
    fn get_indexed_field_struct() -> Result<()> {
        let schema = struct_schema();
        let boolean = BooleanArray::from(vec![false, false, true, true]);
        let int = Int64Array::from(vec![42, 28, 19, 31]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Boolean, true)),
                Arc::new(boolean.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Int64, true)),
                Arc::new(int) as ArrayRef,
            ),
        ]);
        let expr = col("str", &schema).unwrap();
        // only one row should be processed
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)])?;
        let expr = Arc::new(GetIndexedFieldExpr::new(
            expr,
            GetIndexedFieldExprKey::new(
                None,
                Some(ScalarValue::Utf8(Some(String::from("a")))),
            ),
            None,
        ));
        let result = expr.evaluate(&batch)?.into_array(1);
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");
        assert_eq!(boolean, result.clone());
        Ok(())
    }

    fn struct_schema() -> Schema {
        Schema::new(vec![Field::new_struct(
            "str",
            Fields::from(vec![
                Field::new("a", DataType::Boolean, true),
                Field::new("b", DataType::Int64, true),
            ]),
            true,
        )])
    }

    fn list_schema(cols: &[&str]) -> Schema {
        if cols.len() == 2 {
            Schema::new(vec![
                Field::new_list(cols[0], Field::new("item", DataType::Utf8, true), true),
                Field::new(cols[1], DataType::Int64, true),
            ])
        } else {
            Schema::new(vec![
                Field::new_list(cols[0], Field::new("item", DataType::Utf8, true), true),
                Field::new(cols[1], DataType::Int64, true),
                Field::new(cols[2], DataType::Int64, true),
            ])
        }
    }

    #[test]
    fn get_indexed_field_list_without_extra_key() -> Result<()> {
        let list_of_lists = vec![
            vec![Some("a"), Some("b"), None],
            vec![None, Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];
        let list_of_keys = vec![Some(1), Some(2), None];
        let list_of_extra_keys = vec![None];
        let expected_list = vec![Some("a"), Some("c"), None];

        let schema = list_schema(&["l", "k"]);
        let (list_col, key_col, _) =
            build_list_arguments(list_of_lists, list_of_keys, list_of_extra_keys);
        let expr = col("l", &schema).unwrap();
        let key = col("k", &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_col), Arc::new(key_col)],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new(
            expr,
            GetIndexedFieldExprKey::new(Some(key), None),
            None,
        ));
        let result = expr.evaluate(&batch)?.into_array(1);
        let result = as_string_array(&result).expect("failed to downcast to ListArray");
        let expected = StringArray::from(expected_list);
        assert_eq!(expected, result.clone());
        Ok(())
    }

    #[test]
    fn get_indexed_field_list_with_extra_key() -> Result<()> {
        let list_of_lists = vec![
            vec![Some("a"), Some("b"), None],
            vec![None, Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];
        let list_of_keys = vec![Some(1), Some(2), None];
        let list_of_extra_keys = vec![Some(2), None, Some(3)];
        let expected_list = vec![
            vec![Some("a"), Some("b")],
            vec![Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];

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
        let expr = Arc::new(GetIndexedFieldExpr::new(
            expr,
            GetIndexedFieldExprKey::new(Some(key), None),
            Some(extra_key),
        ));
        let result = expr.evaluate(&batch)?.into_array(1);
        let result = as_list_array(&result).expect("failed to downcast to ListArray");
        let (expected, _, _) =
            build_list_arguments(expected_list, vec![None], vec![None]);
        assert_eq!(expected, result.clone());
        Ok(())
    }

    #[test]
    fn get_indexed_field_empty_list() -> Result<()> {
        let schema = list_schema(&["l", "k"]);
        let builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(builder);
        let key_array = new_empty_array(&DataType::Int64);
        let expr = col("l", &schema).unwrap();
        let key = col("k", &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_builder.finish()), key_array],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new(
            expr,
            GetIndexedFieldExprKey::new(Some(key), None),
            None,
        ));
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn get_indexed_field_invalid_list_index() -> Result<()> {
        let schema = list_schema(&["l", "e"]);
        let expr = col("l", &schema).unwrap();
        let key_expr = col("e", &schema).unwrap();
        let builder = StringBuilder::with_capacity(3, 1024);
        let mut list_builder = ListBuilder::new(builder);
        list_builder.values().append_value("hello");
        list_builder.append(true);

        let key_array = Int64Array::from(vec![Some(3)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_builder.finish()), Arc::new(key_array)],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new(
            expr,
            GetIndexedFieldExprKey::new(Some(key_expr), None),
            None,
        ));
        let result = expr.evaluate(&batch)?.into_array(1);
        assert!(result.is_null(0));
        Ok(())
    }
}
