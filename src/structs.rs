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

use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use arrow_array::types::Int32Type;
use arrow_array::{Array, DictionaryArray, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::utils::down_cast_any_ref;

#[derive(Debug, Hash)]
pub struct CreateNamedStruct {
    values: Vec<Arc<dyn PhysicalExpr>>,
    data_type: DataType,
}

impl CreateNamedStruct {
    pub fn new(values: Vec<Arc<dyn PhysicalExpr>>, data_type: DataType) -> Self {
        Self { values, data_type }
    }
}

impl PhysicalExpr for CreateNamedStruct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let values = self
            .values
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let arrays = ColumnarValue::values_to_arrays(&values)?;
        // TODO it would be more efficient if we could preserve dictionaries within the
        // struct array but for now we unwrap them to avoid runtime errors
        // https://github.com/apache/datafusion-comet/issues/755
        let arrays = arrays
            .iter()
            .map(|array| {
                if let Some(dict_array) =
                    array.as_any().downcast_ref::<DictionaryArray<Int32Type>>()
                {
                    take(dict_array.values().as_ref(), dict_array.keys(), None)
                } else {
                    Ok(Arc::clone(array))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields = match &self.data_type {
            DataType::Struct(fields) => fields,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Expected struct data type, got {:?}",
                    self.data_type
                )))
            }
        };
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            fields.clone(),
            arrays,
            None,
        ))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.values.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CreateNamedStruct::new(
            children.clone(),
            self.data_type.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.values.hash(&mut s);
        self.data_type.hash(&mut s);
        self.hash(&mut s);
    }
}

impl Display for CreateNamedStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CreateNamedStruct [values: {:?}, data_type: {:?}]",
            self.values, self.data_type
        )
    }
}

impl PartialEq<dyn Any> for CreateNamedStruct {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.values
                    .iter()
                    .zip(x.values.iter())
                    .all(|(a, b)| a.eq(b))
                    && self.data_type.eq(&x.data_type)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash)]
pub struct GetStructField {
    child: Arc<dyn PhysicalExpr>,
    ordinal: usize,
}

impl GetStructField {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: usize) -> Self {
        Self { child, ordinal }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<Arc<Field>> {
        match self.child.data_type(input_schema)? {
            DataType::Struct(fields) => Ok(fields[self.ordinal].clone()),
            data_type => Err(DataFusionError::Plan(format!(
                "Expect struct field, got {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for GetStructField {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?;

        match child_value {
            ColumnarValue::Array(array) => {
                let struct_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("A struct is expected");

                Ok(ColumnarValue::Array(
                    struct_array.column(self.ordinal).clone(),
                ))
            }
            ColumnarValue::Scalar(ScalarValue::Struct(struct_array)) => Ok(ColumnarValue::Array(
                struct_array.column(self.ordinal).clone(),
            )),
            value => Err(DataFusionError::Execution(format!(
                "Expected a struct array, got {:?}",
                value
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(GetStructField::new(
            children[0].clone(),
            self.ordinal,
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.ordinal.hash(&mut s);
        self.hash(&mut s);
    }
}

impl Display for GetStructField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetStructField [child: {:?}, ordinal: {:?}]",
            self.child, self.ordinal
        )
    }
}

impl PartialEq<dyn Any> for GetStructField {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.ordinal.eq(&x.ordinal))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use super::CreateNamedStruct;
    use arrow_array::{Array, DictionaryArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion_common::Result;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr_common::expressions::column::Column;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use std::sync::Arc;

    #[test]
    fn test_create_struct_from_dict_encoded_i32() -> Result<()> {
        let keys = Int32Array::from(vec![0, 1, 2]);
        let values = Int32Array::from(vec![0, 111, 233]);
        let dict = DictionaryArray::try_new(keys, Arc::new(values))?;
        let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let schema = Schema::new(vec![Field::new("a", data_type, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)])?;
        let data_type =
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)]));
        let x = CreateNamedStruct::new(vec![Arc::new(Column::new("a", 0))], data_type);
        let ColumnarValue::Array(x) = x.evaluate(&batch)? else {
            unreachable!()
        };
        assert_eq!(3, x.len());
        Ok(())
    }

    #[test]
    fn test_create_struct_from_dict_encoded_string() -> Result<()> {
        let keys = Int32Array::from(vec![0, 1, 2]);
        let values = StringArray::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let dict = DictionaryArray::try_new(keys, Arc::new(values))?;
        let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Schema::new(vec![Field::new("a", data_type, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)])?;
        let data_type =
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Utf8, false)]));
        let x = CreateNamedStruct::new(vec![Arc::new(Column::new("a", 0))], data_type);
        let ColumnarValue::Array(x) = x.evaluate(&batch)? else {
            unreachable!()
        };
        assert_eq!(3, x.len());
        Ok(())
    }
}
