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

//! Not expression

use std::any::Any;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::PhysicalExpr;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{cast::as_boolean_array, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::ColumnarValue;

/// Not expression
#[derive(Debug, Eq)]
pub struct NotExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for NotExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl Hash for NotExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl NotExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl fmt::Display for NotExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NOT {}", self.arg)
    }
}

impl PhysicalExpr for NotExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let evaluate_arg = self.arg.evaluate(batch)?;
        match evaluate_arg {
            ColumnarValue::Array(array) => {
                let array = as_boolean_array(&array)?;
                Ok(ColumnarValue::Array(Arc::new(
                    arrow::compute::kernels::boolean::not(array)?,
                )))
            }
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }
                let bool_value: bool = scalar.try_into()?;
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                    !bool_value,
                ))))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NotExpr::new(Arc::clone(&children[0]))))
    }
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        children[0].not()
    }
}

/// Creates a unary expression NOT
pub fn not(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(NotExpr::new(arg)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::{array::BooleanArray, datatypes::*};
    use std::sync::LazyLock;

    #[test]
    fn neg_op() -> Result<()> {
        let schema = schema();

        let expr = not(col("a", &schema)?)?;
        assert_eq!(expr.data_type(&schema)?, DataType::Boolean);
        assert!(expr.nullable(&schema)?);

        let input = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let expected = &BooleanArray::from(vec![Some(false), None, Some(true)]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(input)])?;

        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");
        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_evaluate_bounds() -> Result<()> {
        // Note that `None` for boolean intervals is converted to `Some(false)`
        // / `Some(true)` by `Interval::make`, so it is not explicitly tested
        // here

        // if the bounds are all booleans (false, true) so is the negation
        assert_evaluate_bounds(
            Interval::make(Some(false), Some(true))?,
            Interval::make(Some(false), Some(true))?,
        )?;
        // (true, false) is not tested because it is not a valid interval (lower
        // bound is greater than upper bound)
        assert_evaluate_bounds(
            Interval::make(Some(true), Some(true))?,
            Interval::make(Some(false), Some(false))?,
        )?;
        assert_evaluate_bounds(
            Interval::make(Some(false), Some(false))?,
            Interval::make(Some(true), Some(true))?,
        )?;
        Ok(())
    }

    fn assert_evaluate_bounds(
        interval: Interval,
        expected_interval: Interval,
    ) -> Result<()> {
        let not_expr = not(col("a", &schema())?)?;
        assert_eq!(
            not_expr.evaluate_bounds(&[&interval]).unwrap(),
            expected_interval
        );
        Ok(())
    }

    fn schema() -> SchemaRef {
        static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]))
        });
        Arc::clone(&SCHEMA)
    }
}
