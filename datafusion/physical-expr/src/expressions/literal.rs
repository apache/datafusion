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

//! Literal expressions for physical operations

use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use crate::physical_expr::PhysicalExpr;

use arrow::datatypes::{Field, FieldRef};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::Expr;
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::{ExprProperties, SortProperties};

/// Represents a literal value
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Literal {
    value: ScalarValue,
    field: FieldRef,
}

impl Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        let metadata = self.field.metadata();
        let mut keys = metadata.keys().collect::<Vec<_>>();
        keys.sort();
        for key in keys {
            key.hash(state);
            metadata.get(key).unwrap().hash(state);
        }
    }
}

impl Literal {
    /// Create a literal value expression
    pub fn new(value: ScalarValue) -> Self {
        Self::new_with_metadata(value, None)
    }

    /// Create a literal value expression
    pub fn new_with_metadata(
        value: ScalarValue,
        metadata: Option<FieldMetadata>,
    ) -> Self {
        let mut field = Field::new("lit".to_string(), value.data_type(), value.is_null());

        if let Some(metadata) = metadata {
            field = metadata.add_to_field(field);
        }

        Self {
            value,
            field: field.into(),
        }
    }

    /// Get the scalar value
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PhysicalExpr for Literal {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.field))
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn get_properties(&self, _children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties {
            sort_properties: SortProperties::Singleton,
            range: Interval::try_new(self.value().clone(), self.value().clone())?,
            preserves_lex_ordering: true,
        })
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// Create a literal expression
pub fn lit<T: datafusion_expr::Literal>(value: T) -> Arc<dyn PhysicalExpr> {
    match value.lit() {
        Expr::Literal(v, _) => Arc::new(Literal::new(v)),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use datafusion_common::cast::as_int32_array;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;

    #[test]
    fn literal_i32() -> Result<()> {
        // create an arbitrary record batch
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // create and evaluate a literal expression
        let literal_expr = lit(42i32);
        assert_eq!("42", format!("{literal_expr}"));

        let literal_array = literal_expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let literal_array = as_int32_array(&literal_array)?;

        // note that the contents of the literal array are unrelated to the batch contents except for the length of the array
        assert_eq!(literal_array.len(), 5); // 5 rows in the batch
        for i in 0..literal_array.len() {
            assert_eq!(literal_array.value(i), 42);
        }

        Ok(())
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        // create and evaluate a literal expression
        let expr = lit(42i32);
        let display_string = expr.to_string();
        assert_eq!(display_string, "42");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "42");

        Ok(())
    }
}
