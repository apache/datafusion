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

//! Window functions provide the ability to perform calculations across
//! sets of rows that are related to the current query row.
//!
//! see also https://www.postgresql.org/docs/current/functions-window.html

use crate::error::{DataFusionError, Result};
use crate::physical_plan::functions::{TypeSignature, Volatility};
use crate::physical_plan::{
    aggregates, aggregates::AggregateFunction, functions::Signature,
    type_coercion::data_types, windows::find_ranges_in_range, PhysicalExpr,
};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;
use std::{fmt, str::FromStr};

/// WindowFunction
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum WindowFunction {
    /// window function that leverages an aggregate function
    AggregateFunction(AggregateFunction),
    /// window function that leverages a built-in window function
    BuiltInWindowFunction(BuiltInWindowFunction),
}

impl FromStr for WindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<WindowFunction> {
        let name = name.to_lowercase();
        if let Ok(aggregate) = AggregateFunction::from_str(name.as_str()) {
            Ok(WindowFunction::AggregateFunction(aggregate))
        } else if let Ok(built_in_function) =
            BuiltInWindowFunction::from_str(name.as_str())
        {
            Ok(WindowFunction::BuiltInWindowFunction(built_in_function))
        } else {
            Err(DataFusionError::Plan(format!(
                "There is no window function named {}",
                name
            )))
        }
    }
}

impl fmt::Display for BuiltInWindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BuiltInWindowFunction::RowNumber => write!(f, "ROW_NUMBER"),
            BuiltInWindowFunction::Rank => write!(f, "RANK"),
            BuiltInWindowFunction::DenseRank => write!(f, "DENSE_RANK"),
            BuiltInWindowFunction::PercentRank => write!(f, "PERCENT_RANK"),
            BuiltInWindowFunction::CumeDist => write!(f, "CUME_DIST"),
            BuiltInWindowFunction::Ntile => write!(f, "NTILE"),
            BuiltInWindowFunction::Lag => write!(f, "LAG"),
            BuiltInWindowFunction::Lead => write!(f, "LEAD"),
            BuiltInWindowFunction::FirstValue => write!(f, "FIRST_VALUE"),
            BuiltInWindowFunction::LastValue => write!(f, "LAST_VALUE"),
            BuiltInWindowFunction::NthValue => write!(f, "NTH_VALUE"),
        }
    }
}

impl fmt::Display for WindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFunction::AggregateFunction(fun) => fun.fmt(f),
            WindowFunction::BuiltInWindowFunction(fun) => fun.fmt(f),
        }
    }
}

/// An aggregate function that is part of a built-in window function
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum BuiltInWindowFunction {
    /// number of the current row within its partition, counting from 1
    RowNumber,
    /// rank of the current row with gaps; same as row_number of its first peer
    Rank,
    /// ank of the current row without gaps; this function counts peer groups
    DenseRank,
    /// relative rank of the current row: (rank - 1) / (total rows - 1)
    PercentRank,
    /// relative rank of the current row: (number of rows preceding or peer with current row) / (total rows)
    CumeDist,
    /// integer ranging from 1 to the argument value, dividing the partition as equally as possible
    Ntile,
    /// returns value evaluated at the row that is offset rows before the current row within the partition;
    /// if there is no such row, instead return default (which must be of the same type as value).
    /// Both offset and default are evaluated with respect to the current row.
    /// If omitted, offset defaults to 1 and default to null
    Lag,
    /// returns value evaluated at the row that is offset rows after the current row within the partition;
    /// if there is no such row, instead return default (which must be of the same type as value).
    /// Both offset and default are evaluated with respect to the current row.
    /// If omitted, offset defaults to 1 and default to null
    Lead,
    /// returns value evaluated at the row that is the first row of the window frame
    FirstValue,
    /// returns value evaluated at the row that is the last row of the window frame
    LastValue,
    /// returns value evaluated at the row that is the nth row of the window frame (counting from 1); null if no such row
    NthValue,
}

impl FromStr for BuiltInWindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltInWindowFunction> {
        Ok(match name.to_uppercase().as_str() {
            "ROW_NUMBER" => BuiltInWindowFunction::RowNumber,
            "RANK" => BuiltInWindowFunction::Rank,
            "DENSE_RANK" => BuiltInWindowFunction::DenseRank,
            "PERCENT_RANK" => BuiltInWindowFunction::PercentRank,
            "CUME_DIST" => BuiltInWindowFunction::CumeDist,
            "NTILE" => BuiltInWindowFunction::Ntile,
            "LAG" => BuiltInWindowFunction::Lag,
            "LEAD" => BuiltInWindowFunction::Lead,
            "FIRST_VALUE" => BuiltInWindowFunction::FirstValue,
            "LAST_VALUE" => BuiltInWindowFunction::LastValue,
            "NTH_VALUE" => BuiltInWindowFunction::NthValue,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in window function named {}",
                    name
                )))
            }
        })
    }
}

/// Returns the datatype of the window function
pub fn return_type(
    fun: &WindowFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    match fun {
        WindowFunction::AggregateFunction(fun) => {
            aggregates::return_type(fun, input_expr_types)
        }
        WindowFunction::BuiltInWindowFunction(fun) => {
            return_type_for_built_in(fun, input_expr_types)
        }
    }
}

/// Returns the datatype of the built-in window function
pub(super) fn return_type_for_built_in(
    fun: &BuiltInWindowFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    // verify that this is a valid set of data types for this function
    data_types(input_expr_types, &signature_for_built_in(fun))?;

    match fun {
        BuiltInWindowFunction::RowNumber
        | BuiltInWindowFunction::Rank
        | BuiltInWindowFunction::DenseRank => Ok(DataType::UInt64),
        BuiltInWindowFunction::PercentRank | BuiltInWindowFunction::CumeDist => {
            Ok(DataType::Float64)
        }
        BuiltInWindowFunction::Ntile => Ok(DataType::UInt32),
        BuiltInWindowFunction::Lag
        | BuiltInWindowFunction::Lead
        | BuiltInWindowFunction::FirstValue
        | BuiltInWindowFunction::LastValue
        | BuiltInWindowFunction::NthValue => Ok(input_expr_types[0].clone()),
    }
}

/// the signatures supported by the function `fun`.
pub fn signature(fun: &WindowFunction) -> Signature {
    match fun {
        WindowFunction::AggregateFunction(fun) => aggregates::signature(fun),
        WindowFunction::BuiltInWindowFunction(fun) => signature_for_built_in(fun),
    }
}

/// the signatures supported by the built-in window function `fun`.
pub(super) fn signature_for_built_in(fun: &BuiltInWindowFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.
    match fun {
        BuiltInWindowFunction::RowNumber
        | BuiltInWindowFunction::Rank
        | BuiltInWindowFunction::DenseRank
        | BuiltInWindowFunction::PercentRank
        | BuiltInWindowFunction::CumeDist => Signature::any(0, Volatility::Immutable),
        BuiltInWindowFunction::Lag | BuiltInWindowFunction::Lead => Signature::one_of(
            vec![
                TypeSignature::Any(1),
                TypeSignature::Any(2),
                TypeSignature::Any(3),
            ],
            Volatility::Immutable,
        ),
        BuiltInWindowFunction::FirstValue | BuiltInWindowFunction::LastValue => {
            Signature::any(1, Volatility::Immutable)
        }
        BuiltInWindowFunction::Ntile => {
            Signature::exact(vec![DataType::UInt64], Volatility::Immutable)
        }
        BuiltInWindowFunction::NthValue => Signature::any(2, Volatility::Immutable),
    }
}

/// Partition evaluator
pub(crate) trait PartitionEvaluator {
    /// Whether the evaluator should be evaluated with rank
    fn include_rank(&self) -> bool {
        false
    }

    /// evaluate the partition evaluator against the partitions
    fn evaluate(&self, partition_points: Vec<Range<usize>>) -> Result<Vec<ArrayRef>> {
        partition_points
            .into_iter()
            .map(|partition| self.evaluate_partition(partition))
            .collect()
    }

    /// evaluate the partition evaluator against the partitions with rank information
    fn evaluate_with_rank(
        &self,
        partition_points: Vec<Range<usize>>,
        sort_partition_points: Vec<Range<usize>>,
    ) -> Result<Vec<ArrayRef>> {
        partition_points
            .into_iter()
            .map(|partition| {
                let ranks_in_partition =
                    find_ranges_in_range(&partition, &sort_partition_points);
                self.evaluate_partition_with_rank(partition, ranks_in_partition)
            })
            .collect()
    }

    /// evaluate the partition evaluator against the partition
    fn evaluate_partition(&self, _partition: Range<usize>) -> Result<ArrayRef>;

    /// evaluate the partition evaluator against the partition but with rank
    fn evaluate_partition_with_rank(
        &self,
        _partition: Range<usize>,
        _ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate_partition_with_rank is not implemented by default".into(),
        ))
    }
}

/// A window expression that is a built-in window function.
///
/// Note that unlike aggregation based window functions, built-in window functions normally ignore
/// window frame spec, with the exception of first_value, last_value, and nth_value.
pub(crate) trait BuiltInWindowFunctionExpr: Send + Sync + std::fmt::Debug {
    /// Returns the aggregate expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "BuiltInWindowFunctionExpr: default name"
    }

    /// Create built-in window evaluator with a batch
    fn create_evaluator(
        &self,
        batch: &RecordBatch,
    ) -> Result<Box<dyn PartitionEvaluator>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_function_case_insensitive() -> Result<()> {
        let names = vec![
            "row_number",
            "rank",
            "dense_rank",
            "percent_rank",
            "cume_dist",
            "ntile",
            "lag",
            "lead",
            "first_value",
            "last_value",
            "nth_value",
            "min",
            "max",
            "count",
            "avg",
            "sum",
        ];
        for name in names {
            let fun = WindowFunction::from_str(name)?;
            let fun2 = WindowFunction::from_str(name.to_uppercase().as_str())?;
            assert_eq!(fun, fun2);
            assert_eq!(fun.to_string(), name.to_uppercase());
        }
        Ok(())
    }

    #[test]
    fn test_window_function_from_str() -> Result<()> {
        assert_eq!(
            WindowFunction::from_str("max")?,
            WindowFunction::AggregateFunction(AggregateFunction::Max)
        );
        assert_eq!(
            WindowFunction::from_str("min")?,
            WindowFunction::AggregateFunction(AggregateFunction::Min)
        );
        assert_eq!(
            WindowFunction::from_str("avg")?,
            WindowFunction::AggregateFunction(AggregateFunction::Avg)
        );
        assert_eq!(
            WindowFunction::from_str("cume_dist")?,
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::CumeDist)
        );
        assert_eq!(
            WindowFunction::from_str("first_value")?,
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::FirstValue)
        );
        assert_eq!(
            WindowFunction::from_str("LAST_value")?,
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::LastValue)
        );
        assert_eq!(
            WindowFunction::from_str("LAG")?,
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::Lag)
        );
        assert_eq!(
            WindowFunction::from_str("LEAD")?,
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::Lead)
        );
        Ok(())
    }

    #[test]
    fn test_count_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("count")?;
        let observed = return_type(&fun, &[DataType::Utf8])?;
        assert_eq!(DataType::UInt64, observed);

        let observed = return_type(&fun, &[DataType::UInt64])?;
        assert_eq!(DataType::UInt64, observed);

        Ok(())
    }

    #[test]
    fn test_first_value_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("first_value")?;
        let observed = return_type(&fun, &[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&fun, &[DataType::UInt64])?;
        assert_eq!(DataType::UInt64, observed);

        Ok(())
    }

    #[test]
    fn test_last_value_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("last_value")?;
        let observed = return_type(&fun, &[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&fun, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_lead_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("lead")?;
        let observed = return_type(&fun, &[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&fun, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_lag_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("lag")?;
        let observed = return_type(&fun, &[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&fun, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_nth_value_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("nth_value")?;
        let observed = return_type(&fun, &[DataType::Utf8, DataType::UInt64])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&fun, &[DataType::Float64, DataType::UInt64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_percent_rank_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("percent_rank")?;
        let observed = return_type(&fun, &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_cume_dist_return_type() -> Result<()> {
        let fun = WindowFunction::from_str("cume_dist")?;
        let observed = return_type(&fun, &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }
}
