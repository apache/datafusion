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

//! IS NOT NULL expression

use std::hash::Hash;
use std::{any::Any, sync::Arc};

use crate::PhysicalExpr;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

/// IS NOT NULL expression
#[derive(Debug, Eq)]
pub struct IsNotNullExpr {
    /// The input expression
    arg: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for IsNotNullExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl Hash for IsNotNullExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl IsNotNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for IsNotNullExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} IS NOT NULL", self.arg)
    }
}

impl PhysicalExpr for IsNotNullExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let is_not_null = arrow::compute::is_not_null(&array)?;
                Ok(ColumnarValue::Array(Arc::new(is_not_null)))
            }
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(!scalar.is_null())),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IsNotNullExpr::new(Arc::clone(&children[0]))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.arg.fmt_sql(f)?;
        write!(f, " IS NOT NULL")
    }
}

/// Create an IS NOT NULL expression
pub fn is_not_null(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(IsNotNullExpr::new(arg)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::array::{
        Array, BooleanArray, Float64Array, Int32Array, StringArray, UnionArray,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::*;
    use datafusion_common::cast::as_boolean_array;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;

    #[test]
    fn is_not_null_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), None]);
        let expr = is_not_null(col("a", &schema)?).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a is not null"
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");

        let expected = &BooleanArray::from(vec![true, false]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn union_is_not_null_op() {
        // union of [{A=1}, {A=}, {B=1.1}, {B=1.2}, {B=}]
        let int_array = Int32Array::from(vec![Some(1), None, None, None, None]);
        let float_array =
            Float64Array::from(vec![None, None, Some(1.1), Some(1.2), None]);
        let type_ids = [0, 0, 1, 1, 1].into_iter().collect::<ScalarBuffer<i8>>();

        let children = vec![Arc::new(int_array) as Arc<dyn Array>, Arc::new(float_array)];

        let union_fields: UnionFields = [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
        ]
        .into_iter()
        .collect();

        let array =
            UnionArray::try_new(union_fields.clone(), type_ids, None, children).unwrap();

        let field = Field::new(
            "my_union",
            DataType::Union(union_fields, UnionMode::Sparse),
            true,
        );

        let schema = Schema::new(vec![field]);
        let expr = is_not_null(col("my_union", &schema).unwrap()).unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // expression: "a is not null"
        let actual = expr
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let actual = as_boolean_array(&actual).unwrap();

        let expected = &BooleanArray::from(vec![true, false, true, true, false]);

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let union_fields: UnionFields = [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
        ]
        .into_iter()
        .collect();

        let field = Field::new(
            "my_union",
            DataType::Union(union_fields, UnionMode::Sparse),
            true,
        );

        let schema = Schema::new(vec![field]);
        let expr = is_not_null(col("my_union", &schema).unwrap()).unwrap();
        let display_string = expr.to_string();
        assert_eq!(display_string, "my_union@0 IS NOT NULL");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "my_union IS NOT NULL");

        Ok(())
    }
}
