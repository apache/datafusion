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

use arrow::record_batch::RecordBatch;
use arrow_array::StructArray;
use arrow_schema::{DataType, Field, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::Result as DataFusionResult;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CreateNamedStruct {
    values: Vec<Arc<dyn PhysicalExpr>>,
    names: Vec<String>,
}

impl CreateNamedStruct {
    pub fn new(values: Vec<Arc<dyn PhysicalExpr>>, names: Vec<String>) -> Self {
        Self { values, names }
    }

    fn fields(&self, schema: &Schema) -> DataFusionResult<Vec<Field>> {
        self.values
            .iter()
            .zip(&self.names)
            .map(|(expr, name)| {
                let data_type = expr.data_type(schema)?;
                let nullable = expr.nullable(schema)?;
                Ok(Field::new(name, data_type, nullable))
            })
            .collect()
    }
}

impl PhysicalExpr for CreateNamedStruct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        let fields = self.fields(input_schema)?;
        Ok(DataType::Struct(fields.into()))
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
        let fields = self.fields(&batch.schema())?;
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            fields.into(),
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
            self.names.clone(),
        )))
    }
}

impl Display for CreateNamedStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CreateNamedStruct [values: {:?}, names: {:?}]",
            self.values, self.names
        )
    }
}

#[cfg(test)]
mod test {
    use super::CreateNamedStruct;
    use arrow_array::{Array, DictionaryArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::PhysicalExpr;
    use std::sync::Arc;

    #[test]
    fn test_create_struct_from_dict_encoded_i32() -> Result<()> {
        let keys = Int32Array::from(vec![0, 1, 2]);
        let values = Int32Array::from(vec![0, 111, 233]);
        let dict = DictionaryArray::try_new(keys, Arc::new(values))?;
        let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let schema = Schema::new(vec![Field::new("a", data_type, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)])?;
        let field_names = vec!["a".to_string()];
        let x = CreateNamedStruct::new(vec![Arc::new(Column::new("a", 0))], field_names);
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
        let field_names = vec!["a".to_string()];
        let x = CreateNamedStruct::new(vec![Arc::new(Column::new("a", 0))], field_names);
        let ColumnarValue::Array(x) = x.evaluate(&batch)? else {
            unreachable!()
        };
        assert_eq!(3, x.len());
        Ok(())
    }
}
