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

//! Window function module contains foundational types that are used to represent window functions
//! in DataFusion.

use crate::aggregate_function::AggregateFunction;
use datafusion_common::{DataFusionError, Result};
use std::{fmt, str::FromStr};

/// WindowFunction
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
}
