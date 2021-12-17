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

//! Declaration of built-in (aggregate) functions.
//! This module contains built-in aggregates' enumeration and metadata.
//!
//! Generally, an aggregate has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for min, ([f32]) -> f32, ([f64]) -> f64.

use super::{
    functions::{Signature, Volatility},
    Accumulator, AggregateExpr, PhysicalExpr,
};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::coercion_rule::aggregate_rule::{coerce_exprs, coerce_types};
use crate::physical_plan::distinct_expressions;
use crate::physical_plan::expressions;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use expressions::{avg_return_type, sum_return_type};
use std::{fmt, str::FromStr, sync::Arc};

/// the implementation of an aggregate function
pub type AccumulatorFunctionImplementation =
    Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// This signature corresponds to which types an aggregator serializes
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;

/// Enum of all built-in aggregate functions
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum AggregateFunction {
    /// count
    Count,
    /// sum
    Sum,
    /// min
    Min,
    /// max
    Max,
    /// avg
    Avg,
    /// Approximate aggregate function
    ApproxDistinct,
    /// array_agg
    ArrayAgg,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // uppercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}

impl FromStr for AggregateFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<AggregateFunction> {
        Ok(match name {
            "min" => AggregateFunction::Min,
            "max" => AggregateFunction::Max,
            "count" => AggregateFunction::Count,
            "avg" => AggregateFunction::Avg,
            "sum" => AggregateFunction::Sum,
            "approx_distinct" => AggregateFunction::ApproxDistinct,
            "array_agg" => AggregateFunction::ArrayAgg,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in function named {}",
                    name
                )));
            }
        })
    }
}

/// Returns the datatype of the aggregate function.
/// This is used to get the returned data type for aggregate expr.
pub fn return_type(
    fun: &AggregateFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    let coerced_data_types = coerce_types(fun, input_expr_types, &signature(fun))?;

    match fun {
        // TODO If the datafusion is compatible with PostgreSQL, the returned data type should be INT64.
        AggregateFunction::Count | AggregateFunction::ApproxDistinct => {
            Ok(DataType::UInt64)
        }
        AggregateFunction::Max | AggregateFunction::Min => {
            // For min and max agg function, the returned type is same as input type.
            // The coerced_data_types is same with input_types.
            Ok(coerced_data_types[0].clone())
        }
        AggregateFunction::Sum => sum_return_type(&coerced_data_types[0]),
        AggregateFunction::Avg => avg_return_type(&coerced_data_types[0]),
        AggregateFunction::ArrayAgg => Ok(DataType::List(Box::new(Field::new(
            "item",
            coerced_data_types[0].clone(),
            true,
        )))),
    }
}

/// Create a physical aggregation expression.
/// This function errors when `input_phy_exprs`' can't be coerced to a valid argument type of the aggregation function.
pub fn create_aggregate_expr(
    fun: &AggregateFunction,
    distinct: bool,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    let name = name.into();
    // get the coerced phy exprs if some expr need to be wrapped with the try cast.
    let coerced_phy_exprs =
        coerce_exprs(fun, input_phy_exprs, input_schema, &signature(fun))?;
    if coerced_phy_exprs.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid or wrong number of arguments passed to aggregate: '{}'",
            name,
        )));
    }
    let coerced_exprs_types = coerced_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // get the result data type for this aggregate function
    let input_phy_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let return_type = return_type(fun, &input_phy_types)?;

    Ok(match (fun, distinct) {
        (AggregateFunction::Count, false) => Arc::new(expressions::Count::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Count, true) => {
            Arc::new(distinct_expressions::DistinctCount::new(
                coerced_exprs_types,
                coerced_phy_exprs,
                name,
                return_type,
            ))
        }
        (AggregateFunction::Sum, false) => Arc::new(expressions::Sum::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Sum, true) => {
            return Err(DataFusionError::NotImplemented(
                "SUM(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::ApproxDistinct, _) => {
            Arc::new(expressions::ApproxDistinct::new(
                coerced_phy_exprs[0].clone(),
                name,
                coerced_exprs_types[0].clone(),
            ))
        }
        (AggregateFunction::ArrayAgg, _) => Arc::new(expressions::ArrayAgg::new(
            coerced_phy_exprs[0].clone(),
            name,
            coerced_exprs_types[0].clone(),
        )),
        (AggregateFunction::Min, _) => Arc::new(expressions::Min::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Max, _) => Arc::new(expressions::Max::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Avg, false) => Arc::new(expressions::Avg::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Avg, true) => {
            return Err(DataFusionError::NotImplemented(
                "AVG(DISTINCT) aggregations are not available".to_string(),
            ));
        }
    })
}

static STRINGS: &[DataType] = &[DataType::Utf8, DataType::LargeUtf8];

static NUMERICS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Float32,
    DataType::Float64,
];

static TIMESTAMPS: &[DataType] = &[
    DataType::Timestamp(TimeUnit::Second, None),
    DataType::Timestamp(TimeUnit::Millisecond, None),
    DataType::Timestamp(TimeUnit::Microsecond, None),
    DataType::Timestamp(TimeUnit::Nanosecond, None),
];

static DATES: &[DataType] = &[DataType::Date32, DataType::Date64];

/// the signatures supported by the function `fun`.
pub fn signature(fun: &AggregateFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.
    match fun {
        AggregateFunction::Count
        | AggregateFunction::ApproxDistinct
        | AggregateFunction::ArrayAgg => Signature::any(1, Volatility::Immutable),
        AggregateFunction::Min | AggregateFunction::Max => {
            let valid = STRINGS
                .iter()
                .chain(NUMERICS.iter())
                .chain(TIMESTAMPS.iter())
                .chain(DATES.iter())
                .cloned()
                .collect::<Vec<_>>();
            Signature::uniform(1, valid, Volatility::Immutable)
        }
        AggregateFunction::Avg | AggregateFunction::Sum => {
            Signature::uniform(1, NUMERICS.to_vec(), Volatility::Immutable)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::{
        ApproxDistinct, ArrayAgg, Avg, Count, Max, Min, Sum,
    };

    #[test]
    fn test_count_arragg_approx_expr() -> Result<()> {
        let funcs = vec![
            AggregateFunction::Count,
            AggregateFunction::ArrayAgg,
            AggregateFunction::ApproxDistinct,
        ];
        let data_types = vec![
            DataType::UInt32,
            DataType::Int32,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal(10, 2),
            DataType::Utf8,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Count => {
                        assert!(result_agg_phy_exprs.as_any().is::<Count>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::UInt64, true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::ApproxDistinct => {
                        assert!(result_agg_phy_exprs.as_any().is::<ApproxDistinct>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::UInt64, false),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::ArrayAgg => {
                        assert!(result_agg_phy_exprs.as_any().is::<ArrayAgg>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new(
                                "c1",
                                DataType::List(Box::new(Field::new(
                                    "item",
                                    data_type.clone(),
                                    true
                                ))),
                                false
                            ),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    #[test]
    fn test_min_max_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Min, AggregateFunction::Max];
        let data_types = vec![
            DataType::UInt32,
            DataType::Int32,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal(10, 2),
            DataType::Utf8,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Min => {
                        assert!(result_agg_phy_exprs.as_any().is::<Min>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::Max => {
                        assert!(result_agg_phy_exprs.as_any().is::<Max>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    #[test]
    fn test_sum_avg_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Sum, AggregateFunction::Avg];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Sum => {
                        assert!(result_agg_phy_exprs.as_any().is::<Sum>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        let expect_type = match data_type {
                            DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::UInt64 => DataType::UInt64,
                            DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64 => DataType::Int64,
                            DataType::Float32 | DataType::Float64 => DataType::Float64,
                            _ => data_type.clone(),
                        };

                        assert_eq!(
                            Field::new("c1", expect_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::Avg => {
                        assert!(result_agg_phy_exprs.as_any().is::<Avg>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::Float64, true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    #[test]
    fn test_min_max() -> Result<()> {
        let observed = return_type(&AggregateFunction::Min, &[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&AggregateFunction::Max, &[DataType::Int32])?;
        assert_eq!(DataType::Int32, observed);

        // test decimal for min
        let observed = return_type(&AggregateFunction::Min, &[DataType::Decimal(10, 6)])?;
        assert_eq!(DataType::Decimal(10, 6), observed);

        // test decimal for max
        let observed =
            return_type(&AggregateFunction::Max, &[DataType::Decimal(28, 13)])?;
        assert_eq!(DataType::Decimal(28, 13), observed);

        Ok(())
    }

    #[test]
    fn test_sum_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Sum, &[DataType::Int32])?;
        assert_eq!(DataType::Int64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::UInt8])?;
        assert_eq!(DataType::UInt64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Decimal(10, 5)])?;
        assert_eq!(DataType::Decimal(20, 5), observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Decimal(35, 5)])?;
        assert_eq!(DataType::Decimal(38, 5), observed);

        Ok(())
    }

    #[test]
    fn test_sum_no_utf8() {
        let observed = return_type(&AggregateFunction::Sum, &[DataType::Utf8]);
        assert!(observed.is_err());
    }

    #[test]
    fn test_sum_upcasts() -> Result<()> {
        let observed = return_type(&AggregateFunction::Sum, &[DataType::UInt32])?;
        assert_eq!(DataType::UInt64, observed);
        Ok(())
    }

    #[test]
    fn test_count_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Count, &[DataType::Utf8])?;
        assert_eq!(DataType::UInt64, observed);

        let observed = return_type(&AggregateFunction::Count, &[DataType::Int8])?;
        assert_eq!(DataType::UInt64, observed);

        let observed =
            return_type(&AggregateFunction::Count, &[DataType::Decimal(28, 13)])?;
        assert_eq!(DataType::UInt64, observed);
        Ok(())
    }

    #[test]
    fn test_avg_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Avg, &[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Int32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Decimal(10, 6)])?;
        assert_eq!(DataType::Decimal(14, 10), observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Decimal(36, 6)])?;
        assert_eq!(DataType::Decimal(38, 10), observed);
        Ok(())
    }

    #[test]
    fn test_avg_no_utf8() {
        let observed = return_type(&AggregateFunction::Avg, &[DataType::Utf8]);
        assert!(observed.is_err());
    }
}
