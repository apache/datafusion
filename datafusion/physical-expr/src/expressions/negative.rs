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

//! Negation (-) expression

use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::{
    compute::kernels::numeric::neg_wrapping,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::{
    type_coercion::{is_interval, is_null, is_signed_numeric, is_timestamp},
    ColumnarValue,
};
use datafusion_physical_expr_common::stats::StatisticsV2;
use datafusion_physical_expr_common::stats::StatisticsV2::{
    Bernoulli, Exponential, Gaussian, Uniform, Unknown,
};

/// Negative expression
#[derive(Debug, Eq)]
pub struct NegativeExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for NegativeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl Hash for NegativeExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for NegativeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(- {})", self.arg)
    }
}

impl PhysicalExpr for NegativeExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result = neg_wrapping(array.as_ref())?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                Ok(ColumnarValue::Scalar((scalar.arithmetic_negate())?))
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
        Ok(Arc::new(NegativeExpr::new(Arc::clone(&children[0]))))
    }

    /// Given the child interval of a NegativeExpr, it calculates the NegativeExpr's interval.
    /// It replaces the upper and lower bounds after multiplying them with -1.
    /// Ex: `(a, b]` => `[-b, -a)`
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        Interval::try_new(
            children[0].upper().arithmetic_negate()?,
            children[0].lower().arithmetic_negate()?,
        )
    }

    /// Returns a new [`Interval`] of a NegativeExpr  that has the existing `interval` given that
    /// given the input interval is known to be `children`.
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let child_interval = children[0];
        let negated_interval = Interval::try_new(
            interval.upper().arithmetic_negate()?,
            interval.lower().arithmetic_negate()?,
        )?;

        Ok(child_interval
            .intersect(negated_interval)?
            .map(|result| vec![result]))
    }

    fn evaluate_statistics(&self, stats: &[&StatisticsV2]) -> Result<StatisticsV2> {
        assert_eq!(stats.len(), 1);

        if !stats[0].is_valid() {
            return internal_err!(
                "Cannot evaluate statistics for negative expression with invalid statistics: {:?}",
                stats[0]);
        }
        match stats[0] {
            Uniform { interval } => Ok(Uniform {
                interval: self.evaluate_bounds(&[interval])?,
            }),
            Unknown { range, .. } => {
                if let (Some(mean), Some(median), Some(variance)) =
                    (stats[0].mean()?, stats[0].median()?, stats[0].variance()?)
                {
                    Ok(Unknown {
                        mean: Some(mean.arithmetic_negate()?),
                        median: Some(median.arithmetic_negate()?),
                        variance: Some(variance.arithmetic_negate()?),
                        range: self.evaluate_bounds(&[range])?,
                    })
                } else {
                    Ok(Unknown {
                        mean: None,
                        median: None,
                        variance: None,
                        range: self.evaluate_bounds(&[range])?,
                    })
                }
            }
            Bernoulli { p } => Ok(Bernoulli {
                p: ScalarValue::new_one(&DataType::Float64)?.sub_checked(p)?,
            }),
            Exponential { .. } | Gaussian { .. } => Ok(Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::UNCERTAIN,
            }),
        }
    }

    /// The ordering of a [`NegativeExpr`] is simply the reverse of its child.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties {
            sort_properties: -children[0].sort_properties,
            range: children[0].range.clone().arithmetic_negate()?,
            preserves_lex_ordering: false,
        })
    }
}

/// Creates a unary expression NEGATIVE
///
/// # Errors
///
/// This function errors when the argument's type is not signed numeric
pub fn negative(
    arg: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let data_type = arg.data_type(input_schema)?;
    if is_null(&data_type) {
        Ok(arg)
    } else if !is_signed_numeric(&data_type)
        && !is_interval(&data_type)
        && !is_timestamp(&data_type)
    {
        plan_err!("Negation only supports numeric, interval and timestamp types")
    } else {
        Ok(Arc::new(NegativeExpr::new(arg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, Column};

    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow_schema::DataType::{Float32, Float64, Int16, Int32, Int64, Int8};
    use datafusion_common::cast::as_primitive_array;
    use datafusion_common::DataFusionError;

    use paste::paste;

    macro_rules! test_array_negative_op {
        ($DATA_TY:tt, $($VALUE:expr),*   ) => {
            let schema = Schema::new(vec![Field::new("a", DataType::$DATA_TY, true)]);
            let expr = negative(col("a", &schema)?, &schema)?;
            assert_eq!(expr.data_type(&schema)?, DataType::$DATA_TY);
            assert!(expr.nullable(&schema)?);
            let mut arr = Vec::new();
            let mut arr_expected = Vec::new();
            $(
                arr.push(Some($VALUE));
                arr_expected.push(Some(-$VALUE));
            )+
            arr.push(None);
            arr_expected.push(None);
            let input = paste!{[<$DATA_TY Array>]::from(arr)};
            let expected = &paste!{[<$DATA_TY Array>]::from(arr_expected)};
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)])?;
            let result = expr.evaluate(&batch)?.into_array(batch.num_rows()).expect("Failed to convert to array");
            let result =
                as_primitive_array(&result).expect(format!("failed to downcast to {:?}Array", $DATA_TY).as_str());
            assert_eq!(result, expected);
        };
    }

    #[test]
    fn array_negative_op() -> Result<()> {
        test_array_negative_op!(Int8, 2i8, 1i8);
        test_array_negative_op!(Int16, 234i16, 123i16);
        test_array_negative_op!(Int32, 2345i32, 1234i32);
        test_array_negative_op!(Int64, 23456i64, 12345i64);
        test_array_negative_op!(Float32, 2345.0f32, 1234.0f32);
        test_array_negative_op!(Float64, 23456.0f64, 12345.0f64);
        Ok(())
    }

    #[test]
    fn test_evaluate_bounds() -> Result<()> {
        let negative_expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));
        let child_interval = Interval::make(Some(-2), Some(1))?;
        let negative_expr_interval = Interval::make(Some(-1), Some(2))?;
        assert_eq!(
            negative_expr.evaluate_bounds(&[&child_interval])?,
            negative_expr_interval
        );
        Ok(())
    }

    #[test]
    fn test_evaluate_statistics() -> Result<()> {
        let negative_expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));

        // Uniform
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Uniform {
                interval: Interval::make(Some(-2.), Some(3.))?,
            }])?,
            Uniform {
                interval: Interval::make(Some(-3.), Some(2.))?,
            }
        );

        // Bernoulli
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Bernoulli {
                p: ScalarValue::Float64(Some(0.75))
            }])?,
            Bernoulli {
                p: ScalarValue::Float64(Some(0.25))
            }
        );

        // Exponential
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Exponential {
                rate: ScalarValue::Float64(Some(1.)),
                offset: ScalarValue::Float64(Some(1.)),
            }])?,
            Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::UNCERTAIN
            }
        );

        // Gaussian
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Gaussian {
                mean: ScalarValue::Int32(Some(15)),
                variance: ScalarValue::Int32(Some(225)),
            }])?,
            Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::UNCERTAIN
            }
        );

        // Unknown
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Unknown {
                mean: Some(ScalarValue::Int32(Some(15))),
                median: Some(ScalarValue::Int32(Some(15))),
                variance: Some(ScalarValue::Int32(Some(15))),
                range: Interval::make(Some(10), Some(20))?,
            }])?,
            Unknown {
                mean: Some(ScalarValue::Int32(Some(-15))),
                median: Some(ScalarValue::Int32(Some(-15))),
                variance: Some(ScalarValue::Int32(Some(-15))),
                range: Interval::make(Some(-20), Some(-10))?,
            }
        );

        Ok(())
    }

    #[test]
    fn test_propagate_constraints() -> Result<()> {
        let negative_expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));
        let original_child_interval = Interval::make(Some(-2), Some(3))?;
        let negative_expr_interval = Interval::make(Some(0), Some(4))?;
        let after_propagation = Some(vec![Interval::make(Some(-2), Some(0))?]);
        assert_eq!(
            negative_expr.propagate_constraints(
                &negative_expr_interval,
                &[&original_child_interval]
            )?,
            after_propagation
        );
        Ok(())
    }

    #[test]
    fn test_negation_valid_types() -> Result<()> {
        let negatable_types = [
            Int8,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Interval(IntervalUnit::YearMonth),
        ];
        for negatable_type in negatable_types {
            let schema = Schema::new(vec![Field::new("a", negatable_type, true)]);
            let _expr = negative(col("a", &schema)?, &schema)?;
        }
        Ok(())
    }

    #[test]
    fn test_negation_invalid_types() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let expr = negative(col("a", &schema)?, &schema).unwrap_err();
        matches!(expr, DataFusionError::Plan(_));
        Ok(())
    }
}
