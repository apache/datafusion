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
    type_coercion::{coerce, data_types},
    Accumulator, AggregateExpr, PhysicalExpr,
};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::distinct_expressions;
use crate::physical_plan::expressions;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use expressions::{avg_return_type, sum_return_type};
use std::{fmt, str::FromStr, sync::Arc};
/// the implementation of an aggregate function
pub type AccumulatorFunctionImplementation =
    Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// This signature corresponds to which types an aggregator serializes
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;

/// Enum of all built-in scalar functions
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
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in function named {}",
                    name
                )))
            }
        })
    }
}

/// Returns the datatype of the scalar function
pub fn return_type(fun: &AggregateFunction, arg_types: &[DataType]) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    // verify that this is a valid set of data types for this function
    data_types(arg_types, &signature(fun))?;

    match fun {
        AggregateFunction::Count | AggregateFunction::ApproxDistinct => {
            Ok(DataType::UInt64)
        }
        AggregateFunction::Max | AggregateFunction::Min => Ok(arg_types[0].clone()),
        AggregateFunction::Sum => sum_return_type(&arg_types[0]),
        AggregateFunction::Avg => avg_return_type(&arg_types[0]),
    }
}

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_aggregate_expr(
    fun: &AggregateFunction,
    distinct: bool,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    let name = name.into();
    let arg = coerce(args, input_schema, &signature(fun))?;
    if arg.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid or wrong number of arguments passed to aggregate: '{}'",
            name,
        )));
    }
    let arg = arg[0].clone();

    let arg_types = args
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    let return_type = return_type(fun, &arg_types)?;

    Ok(match (fun, distinct) {
        (AggregateFunction::Count, false) => {
            Arc::new(expressions::Count::new(arg, name, return_type))
        }
        (AggregateFunction::Count, true) => {
            Arc::new(distinct_expressions::DistinctCount::new(
                arg_types,
                args.to_vec(),
                name,
                return_type,
            ))
        }
        (AggregateFunction::Sum, false) => {
            Arc::new(expressions::Sum::new(arg, name, return_type))
        }
        (AggregateFunction::Sum, true) => {
            return Err(DataFusionError::NotImplemented(
                "SUM(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::ApproxDistinct, _) => Arc::new(
            expressions::ApproxDistinct::new(arg, name, arg_types[0].clone()),
        ),
        (AggregateFunction::Min, _) => {
            Arc::new(expressions::Min::new(arg, name, return_type))
        }
        (AggregateFunction::Max, _) => {
            Arc::new(expressions::Max::new(arg, name, return_type))
        }
        (AggregateFunction::Avg, false) => {
            Arc::new(expressions::Avg::new(arg, name, return_type))
        }
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
        AggregateFunction::Count | AggregateFunction::ApproxDistinct => {
            Signature::any(1, Volatility::Immutable)
        }
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

    #[test]
    fn test_min_max() -> Result<()> {
        let observed = return_type(&AggregateFunction::Min, &[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&AggregateFunction::Max, &[DataType::Int32])?;
        assert_eq!(DataType::Int32, observed);
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
        Ok(())
    }

    #[test]
    fn test_avg_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Avg, &[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);
        Ok(())
    }

    #[test]
    fn test_avg_no_utf8() {
        let observed = return_type(&AggregateFunction::Avg, &[DataType::Utf8]);
        assert!(observed.is_err());
    }
}
