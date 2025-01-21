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
use datafusion_common::{cast::as_boolean_array, internal_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::stats::StatisticsV2;
use datafusion_physical_expr_common::stats::StatisticsV2::{
    Bernoulli, Exponential, Gaussian, Uniform, Unknown,
};

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

    fn evaluate_statistics(&self, stats: &[&StatisticsV2]) -> Result<StatisticsV2> {
        assert_eq!(stats.len(), 1);

        if !stats[0].is_valid() {
            return internal_err!(
                "Cannot evaluate statistics for NOT expression with invalid statistics: {:?}",
                stats[0]);
        }
        match stats[0] {
            Uniform { interval }  => {
                if interval.lower().data_type().eq(&DataType::Boolean)
                    && interval.lower().data_type().eq(&DataType::Boolean) {
                    Ok(Uniform {
                        interval: interval.not()?,
                    })
                } else {
                    Ok(Unknown {
                        mean: None,
                        median: None,
                        variance: None,
                        range: Interval::UNCERTAIN
                    })
                }
            },
            Unknown { range, .. } => {
                if range.lower().data_type().eq(&DataType::Boolean)
                    && range.lower().data_type().eq(&DataType::Boolean) {
                    Ok(Unknown {
                        mean: None,
                        median: None,
                        variance: None,
                        range: range.not()?
                    })
                } else {
                    Ok(Unknown {
                        mean: None,
                        median: None,
                        variance: None,
                        range: Interval::UNCERTAIN
                    })
                }
            }
            // Note: NOT Exponential distribution is mirrored on X axis and in fact,
            //  it is a plot of logarithmic function, which is Unknown.
            // Note: NOT Gaussian distribution is mirrored on X axis and is Unknown
            Exponential { .. } | Gaussian { .. } |  Bernoulli { .. } => Ok(Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::UNCERTAIN
            }),
        }
    }
}

/// Creates a unary expression NOT
pub fn not(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(NotExpr::new(arg)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, Column};
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

    #[test]
    fn test_evaluate_statistics() -> Result<()> {
        let _schema = &Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
        ]);
        let a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
        let expr = not(a)?;

        // Uniform with boolean bounds
        assert_eq!(
            expr.evaluate_statistics(&[&Uniform {interval: Interval::CERTAINLY_FALSE}])?,
            Uniform {interval: Interval::CERTAINLY_TRUE}
        );
        assert_eq!(
            expr.evaluate_statistics(&[&Uniform {interval: Interval::CERTAINLY_FALSE}])?,
            Uniform {interval: Interval::CERTAINLY_TRUE}
        );

        // Uniform with non-boolean bounds
        assert_eq!(
            expr.evaluate_statistics(&[&Uniform {
                interval: Interval::make_unbounded(&DataType::Float64)?}])?,
            uncertain_unknown()
        );

        // Exponential
        assert_eq!(
            expr.evaluate_statistics(&[&Exponential {
                rate: ScalarValue::new_one(&DataType::Float64)?,
                offset: ScalarValue::new_one(&DataType::Float64)?
            }])?,
            uncertain_unknown()
        );

        // Gaussian
        assert_eq!(
            expr.evaluate_statistics(&[&Gaussian {
                mean: ScalarValue::new_one(&DataType::Float64)?,
                variance: ScalarValue::new_one(&DataType::Float64)?
            }])?,
            uncertain_unknown()
        );

        // Bernoulli
        assert_eq!(
            expr.evaluate_statistics(&[&Bernoulli { p: ScalarValue::Float64(Some(0.25)) }])?,
            uncertain_unknown()
        );

        // Unknown with boolean interval as range
        assert_eq!(
            expr.evaluate_statistics(&[&Unknown {
                mean: Some(ScalarValue::Boolean(Some(true))),
                median: Some(ScalarValue::Boolean(Some(true))),
                variance: Some(ScalarValue::Boolean(Some(true))),
                range: Interval::CERTAINLY_TRUE
            }])?,
            Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::CERTAINLY_FALSE
            }
        );

        // Unknown with non-boolean interval as range
        assert_eq!(
            expr.evaluate_statistics(&[&Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::make_unbounded(&DataType::Float64)?
            }])?,
           uncertain_unknown()
        );

        Ok(())
    }

    fn uncertain_unknown() -> StatisticsV2 {
        Unknown {
            mean: None,
            median: None,
            variance: None,
            range: Interval::UNCERTAIN
        }
    }

    fn schema() -> SchemaRef {
        static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]))
        });
        Arc::clone(&SCHEMA)
    }
}
