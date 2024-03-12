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
use datafusion_common::exec_err;

use crate::physical_expr::down_cast_any_ref;
use arrow::{
    array::{Array, Scalar, StringArray},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{
    cast::{as_map_array, as_struct_array},
    Result, ScalarValue,
};
use datafusion_expr::{field_util::GetFieldAccessSchema, ColumnarValue};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

/// Access a sub field of a nested type, such as `Field` or `List`
#[derive(Clone, Hash, Debug)]
pub enum GetFieldAccessExpr {
    /// Named field, For example `struct["name"]`
    NamedStructField { name: ScalarValue },
}

impl std::fmt::Display for GetFieldAccessExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            GetFieldAccessExpr::NamedStructField { name } => write!(f, "[{}]", name),
        }
    }
}

impl PartialEq<dyn Any> for GetFieldAccessExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| match (self, x) {
                (
                    GetFieldAccessExpr::NamedStructField { name: lhs },
                    GetFieldAccessExpr::NamedStructField { name: rhs },
                ) => lhs.eq(rhs),
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
    field: GetFieldAccessExpr,
}

impl GetIndexedFieldExpr {
    /// Create new [`GetIndexedFieldExpr`]
    pub fn new(arg: Arc<dyn PhysicalExpr>, field: GetFieldAccessExpr) -> Self {
        Self { arg, field }
    }

    /// Create a new [`GetIndexedFieldExpr`] for accessing the named field
    pub fn new_field(arg: Arc<dyn PhysicalExpr>, name: impl Into<String>) -> Self {
        Self::new(
            arg,
            GetFieldAccessExpr::NamedStructField {
                name: ScalarValue::from(name.into()),
            },
        )
    }

    /// Get the description of what field should be accessed
    pub fn field(&self) -> &GetFieldAccessExpr {
        &self.field
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }

    fn schema_access(&self, _input_schema: &Schema) -> Result<GetFieldAccessSchema> {
        Ok(match &self.field {
            GetFieldAccessExpr::NamedStructField { name } => {
                GetFieldAccessSchema::NamedStructField { name: name.clone() }
            }
        })
    }
}

impl std::fmt::Display for GetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).{}", self.arg, self.field)
    }
}

impl PhysicalExpr for GetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let arg_dt = self.arg.data_type(input_schema)?;
        self.schema_access(input_schema)?
            .get_accessed_field(&arg_dt)
            .map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let arg_dt = self.arg.data_type(input_schema)?;
        self.schema_access(input_schema)?
            .get_accessed_field(&arg_dt)
            .map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(batch.num_rows())?;
        match &self.field {
            GetFieldAccessExpr::NamedStructField{name} => match (array.data_type(), name) {
                (DataType::Map(_, _), ScalarValue::Utf8(Some(k))) => {
                    let map_array = as_map_array(array.as_ref())?;
                    let key_scalar = Scalar::new(StringArray::from(vec![k.clone()]));
                    let keys = arrow::compute::kernels::cmp::eq(&key_scalar, map_array.keys())?;
                    let entries = arrow::compute::filter(map_array.entries(), &keys)?;
                    let entries_struct_array = as_struct_array(entries.as_ref())?;
                    Ok(ColumnarValue::Array(entries_struct_array.column(1).clone()))
                }
                (DataType::Struct(_), ScalarValue::Utf8(Some(k))) => {
                    let as_struct_array = as_struct_array(&array)?;
                    match as_struct_array.column_by_name(k) {
                        None => exec_err!(
                            "get indexed field {k} not found in struct"),
                        Some(col) => Ok(ColumnarValue::Array(col.clone()))
                    }
                }
                (DataType::Struct(_), name) => exec_err!(
                    "get indexed field is only possible on struct with utf8 indexes. \
                             Tried with {name:?} index"),
                (dt, name) => exec_err!(
                                "get indexed field is only possible on lists with int64 indexes or struct \
                                         with utf8 indexes. Tried {dt:?} with {name:?} index"),
            },
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
            self.field.clone(),
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
            .map(|x| self.arg.eq(&x.arg) && self.field.eq(&x.field))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::array::ArrayRef;
    use arrow::array::{BooleanArray, Int64Array, StructArray};
    use arrow::datatypes::Field;
    use arrow::datatypes::Fields;
    use datafusion_common::cast::as_boolean_array;
    use datafusion_common::Result;

    #[test]
    fn get_indexed_field_named_struct_field() -> Result<()> {
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
        let expr = Arc::new(GetIndexedFieldExpr::new_field(expr, "a"));
        let result = expr
            .evaluate(&batch)?
            .into_array(1)
            .expect("Failed to convert to array");
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
}
