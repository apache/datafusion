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
//! see also <https://www.postgresql.org/docs/current/functions-window.html>

use crate::error::Result;
use crate::physical_plan::functions::{TypeSignature, Volatility};
use crate::physical_plan::{aggregates, functions::Signature, type_coercion::data_types};
use arrow::datatypes::DataType;
pub use datafusion_expr::{BuiltInWindowFunction, WindowFunction};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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
