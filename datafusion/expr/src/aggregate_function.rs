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

//! Aggregate function module contains all built-in aggregate functions definitions

use std::sync::Arc;
use std::{fmt, str::FromStr};

use crate::utils;
use crate::{type_coercion::aggregates::*, Signature, Volatility};

use arrow::datatypes::{DataType, Field};
use datafusion_common::{plan_datafusion_err, plan_err, DataFusionError, Result};

use strum_macros::EnumIter;

/// Enum of all built-in aggregate functions
// Contributor's guide for adding new aggregate functions
// https://datafusion.apache.org/contributor-guide/index.html#how-to-add-a-new-aggregate-function
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, EnumIter)]
pub enum AggregateFunction {
    /// Minimum
    Min,
    /// Maximum
    Max,
    /// Aggregation into an array
    ArrayAgg,
    /// N'th value in a group according to some ordering
    NthValue,
    /// Grouping
    Grouping,
}

impl AggregateFunction {
    pub fn name(&self) -> &str {
        use AggregateFunction::*;
        match self {
            Min => "MIN",
            Max => "MAX",
            ArrayAgg => "ARRAY_AGG",
            NthValue => "NTH_VALUE",
            Grouping => "GROUPING",
        }
    }
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for AggregateFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<AggregateFunction> {
        Ok(match name {
            // general
            "max" => AggregateFunction::Max,
            "min" => AggregateFunction::Min,
            "array_agg" => AggregateFunction::ArrayAgg,
            "nth_value" => AggregateFunction::NthValue,
            // other
            "grouping" => AggregateFunction::Grouping,
            _ => {
                return plan_err!("There is no built-in function named {name}");
            }
        })
    }
}

impl AggregateFunction {
    /// Returns the datatype of the aggregate function given its argument types
    ///
    /// This is used to get the returned data type for aggregate expr.
    pub fn return_type(
        &self,
        input_expr_types: &[DataType],
        input_expr_nullable: &[bool],
    ) -> Result<DataType> {
        // Note that this function *must* return the same type that the respective physical expression returns
        // or the execution panics.

        let coerced_data_types = coerce_types(self, input_expr_types, &self.signature())
            // original errors are all related to wrong function signature
            // aggregate them for better error message
            .map_err(|_| {
                plan_datafusion_err!(
                    "{}",
                    utils::generate_signature_error_msg(
                        &format!("{self}"),
                        self.signature(),
                        input_expr_types,
                    )
                )
            })?;

        match self {
            AggregateFunction::Max | AggregateFunction::Min => {
                // For min and max agg function, the returned type is same as input type.
                // The coerced_data_types is same with input_types.
                Ok(coerced_data_types[0].clone())
            }
            AggregateFunction::ArrayAgg => Ok(DataType::List(Arc::new(Field::new(
                "item",
                coerced_data_types[0].clone(),
                input_expr_nullable[0],
            )))),
            AggregateFunction::Grouping => Ok(DataType::Int32),
            AggregateFunction::NthValue => Ok(coerced_data_types[0].clone()),
        }
    }

    /// Returns if the return type of the aggregate function is nullable given its argument
    /// nullability
    pub fn nullable(&self) -> Result<bool> {
        match self {
            AggregateFunction::Max | AggregateFunction::Min => Ok(true),
            AggregateFunction::ArrayAgg => Ok(false),
            AggregateFunction::Grouping => Ok(true),
            AggregateFunction::NthValue => Ok(true),
        }
    }
}

impl AggregateFunction {
    /// the signatures supported by the function `fun`.
    pub fn signature(&self) -> Signature {
        // note: the physical expression must accept the type returned by this function or the execution panics.
        match self {
            AggregateFunction::Grouping | AggregateFunction::ArrayAgg => {
                Signature::any(1, Volatility::Immutable)
            }
            AggregateFunction::Min | AggregateFunction::Max => {
                let valid = STRINGS
                    .iter()
                    .chain(NUMERICS.iter())
                    .chain(TIMESTAMPS.iter())
                    .chain(DATES.iter())
                    .chain(TIMES.iter())
                    .chain(BINARYS.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                Signature::uniform(1, valid, Volatility::Immutable)
            }
            AggregateFunction::NthValue => Signature::any(2, Volatility::Immutable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use strum::IntoEnumIterator;

    #[test]
    // Test for AggregateFuncion's Display and from_str() implementations.
    // For each variant in AggregateFuncion, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same. This assertion is also necessary for
    // function suggestion. See https://github.com/apache/datafusion/issues/8082
    fn test_display_and_from_str() {
        for func_original in AggregateFunction::iter() {
            let func_name = func_original.to_string();
            let func_from_str =
                AggregateFunction::from_str(func_name.to_lowercase().as_str()).unwrap();
            assert_eq!(func_from_str, func_original);
        }
    }
}
