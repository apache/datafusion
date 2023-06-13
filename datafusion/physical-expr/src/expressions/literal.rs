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
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::physical_expr::down_cast_any_ref;
use crate::{AnalysisContext, ExprBoundaries, PhysicalExpr};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, Expr};

/// Represents a literal value
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Literal {
    value: ScalarValue,
}

impl Literal {
    /// Create a literal value expression
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
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
        Ok(self.value.get_datatype())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    /// Return the boundaries of this literal expression (which is the same as
    /// the value it represents).
    fn analyze(&self, context: AnalysisContext) -> AnalysisContext {
        context.with_boundaries(Some(ExprBoundaries::new(
            self.value.clone(),
            self.value.clone(),
            Some(1),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for Literal {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

/// Create a literal expression
pub fn lit<T: datafusion_expr::Literal>(value: T) -> Arc<dyn PhysicalExpr> {
    match value.lit() {
        Expr::Literal(v) => Arc::new(Literal::new(v)),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::*;
    use datafusion_common::cast::as_int32_array;
    use datafusion_common::Result;

    #[test]
    fn literal_i32() -> Result<()> {
        // create an arbitrary record bacth
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // create and evaluate a literal expression
        let literal_expr = lit(42i32);
        assert_eq!("42", format!("{literal_expr}"));

        let literal_array = literal_expr.evaluate(&batch)?.into_array(batch.num_rows());
        let literal_array = as_int32_array(&literal_array)?;

        // note that the contents of the literal array are unrelated to the batch contents except for the length of the array
        assert_eq!(literal_array.len(), 5); // 5 rows in the batch
        for i in 0..literal_array.len() {
            assert_eq!(literal_array.value(i), 42);
        }

        Ok(())
    }

    #[test]
    fn literal_bounds_analysis() -> Result<()> {
        let schema = Schema::empty();
        let context = AnalysisContext::new(&schema, vec![]);

        let literal_expr = lit(42i32);
        let result_ctx = literal_expr.analyze(context);
        let boundaries = result_ctx.boundaries.unwrap();
        assert_eq!(boundaries.min_value, ScalarValue::Int32(Some(42)));
        assert_eq!(boundaries.max_value, ScalarValue::Int32(Some(42)));
        assert_eq!(boundaries.distinct_count, Some(1));
        assert_eq!(boundaries.selectivity, None);

        Ok(())
    }
}
