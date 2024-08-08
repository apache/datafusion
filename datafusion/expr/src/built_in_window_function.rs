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

//! Built-in functions module contains all the built-in functions definitions.

use std::fmt;
use std::str::FromStr;

use crate::type_coercion::functions::data_types;
use crate::utils;
use crate::{Signature, TypeSignature, Volatility};
use datafusion_common::{plan_datafusion_err, plan_err, DataFusionError, Result};

use arrow::datatypes::DataType;

use strum_macros::EnumIter;

impl fmt::Display for BuiltInWindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A [window function] built in to DataFusion
///
/// [window function]: https://en.wikipedia.org/wiki/Window_function_(SQL)
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumIter)]
pub enum BuiltInWindowFunction {
    /// rank of the current row with gaps; same as row_number of its first peer
    Rank,
    /// rank of the current row without gaps; this function counts peer groups
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

impl BuiltInWindowFunction {
    pub fn name(&self) -> &str {
        use BuiltInWindowFunction::*;
        match self {
            Rank => "RANK",
            DenseRank => "DENSE_RANK",
            PercentRank => "PERCENT_RANK",
            CumeDist => "CUME_DIST",
            Ntile => "NTILE",
            Lag => "LAG",
            Lead => "LEAD",
            FirstValue => "first_value",
            LastValue => "last_value",
            NthValue => "NTH_VALUE",
        }
    }
}

impl FromStr for BuiltInWindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltInWindowFunction> {
        Ok(match name.to_uppercase().as_str() {
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
            _ => return plan_err!("There is no built-in window function named {name}"),
        })
    }
}

/// Returns the datatype of the built-in window function
impl BuiltInWindowFunction {
    pub fn return_type(&self, input_expr_types: &[DataType]) -> Result<DataType> {
        // Note that this function *must* return the same type that the respective physical expression returns
        // or the execution panics.

        // verify that this is a valid set of data types for this function
        data_types(input_expr_types, &self.signature())
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
            BuiltInWindowFunction::Rank
            | BuiltInWindowFunction::DenseRank
            | BuiltInWindowFunction::Ntile => Ok(DataType::UInt64),
            BuiltInWindowFunction::PercentRank | BuiltInWindowFunction::CumeDist => {
                Ok(DataType::Float64)
            }
            BuiltInWindowFunction::Lag
            | BuiltInWindowFunction::Lead
            | BuiltInWindowFunction::FirstValue
            | BuiltInWindowFunction::LastValue
            | BuiltInWindowFunction::NthValue => Ok(input_expr_types[0].clone()),
        }
    }

    /// the signatures supported by the built-in window function `fun`.
    pub fn signature(&self) -> Signature {
        // note: the physical expression must accept the type returned by this function or the execution panics.
        match self {
            BuiltInWindowFunction::Rank
            | BuiltInWindowFunction::DenseRank
            | BuiltInWindowFunction::PercentRank
            | BuiltInWindowFunction::CumeDist => Signature::any(0, Volatility::Immutable),
            BuiltInWindowFunction::Lag | BuiltInWindowFunction::Lead => {
                Signature::one_of(
                    vec![
                        TypeSignature::Any(1),
                        TypeSignature::Any(2),
                        TypeSignature::Any(3),
                    ],
                    Volatility::Immutable,
                )
            }
            BuiltInWindowFunction::FirstValue | BuiltInWindowFunction::LastValue => {
                Signature::any(1, Volatility::Immutable)
            }
            BuiltInWindowFunction::Ntile => Signature::uniform(
                1,
                vec![
                    DataType::UInt64,
                    DataType::UInt32,
                    DataType::UInt16,
                    DataType::UInt8,
                    DataType::Int64,
                    DataType::Int32,
                    DataType::Int16,
                    DataType::Int8,
                ],
                Volatility::Immutable,
            ),
            BuiltInWindowFunction::NthValue => Signature::any(2, Volatility::Immutable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;
    #[test]
    // Test for BuiltInWindowFunction's Display and from_str() implementations.
    // For each variant in BuiltInWindowFunction, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same. This assertion is also necessary for
    // function suggestion. See https://github.com/apache/datafusion/issues/8082
    fn test_display_and_from_str() {
        for func_original in BuiltInWindowFunction::iter() {
            let func_name = func_original.to_string();
            let func_from_str = BuiltInWindowFunction::from_str(&func_name).unwrap();
            assert_eq!(func_from_str, func_original);
        }
    }
}
