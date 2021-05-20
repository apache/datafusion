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
use crate::physical_plan::{
    aggregates, aggregates::AggregateFunction, functions::Signature,
    type_coercion::data_types,
};
use arrow::datatypes::DataType;
use std::{fmt, str::FromStr};

/// WindowFunction
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFunction {
    /// window function that leverages an aggregate function
    AggregateFunction(AggregateFunction),
    /// window function that leverages a built-in window function
    BuiltInWindowFunction(BuiltInWindowFunction),
}

impl FromStr for WindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<WindowFunction> {
        if let Ok(aggregate) = AggregateFunction::from_str(name) {
            Ok(WindowFunction::AggregateFunction(aggregate))
        } else if let Ok(built_in_function) = BuiltInWindowFunction::from_str(name) {
            Ok(WindowFunction::BuiltInWindowFunction(built_in_function))
        } else {
            Err(DataFusionError::Plan(format!(
                "There is no built-in function named {}",
                name
            )))
        }
    }
}

impl fmt::Display for BuiltInWindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // uppercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_uppercase())
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltInWindowFunction {
    /// row number
    RowNumber,
    /// rank
    Rank,
    /// dense rank
    DenseRank,
    /// lag
    Lag,
    /// lead
    Lead,
    /// first value
    FirstValue,
    /// last value
    LastValue,
}

impl FromStr for BuiltInWindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltInWindowFunction> {
        Ok(match name {
            "row_number" => BuiltInWindowFunction::RowNumber,
            "rank" => BuiltInWindowFunction::Rank,
            "dense_rank" => BuiltInWindowFunction::DenseRank,
            "first_value" => BuiltInWindowFunction::FirstValue,
            "last_value" => BuiltInWindowFunction::LastValue,
            "lag" => BuiltInWindowFunction::Lag,
            "lead" => BuiltInWindowFunction::Lead,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in window function named {}",
                    name
                )))
            }
        })
    }
}

/// Returns the datatype of the scalar function
pub fn return_type(fun: &WindowFunction, arg_types: &[DataType]) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    // verify that this is a valid set of data types for this function
    data_types(arg_types, &signature(fun))?;

    match fun {
        WindowFunction::AggregateFunction(fun) => aggregates::return_type(fun, arg_types),
        WindowFunction::BuiltInWindowFunction(fun) => match fun {
            BuiltInWindowFunction::RowNumber
            | BuiltInWindowFunction::Rank
            | BuiltInWindowFunction::DenseRank => Ok(DataType::UInt64),
            BuiltInWindowFunction::Lag
            | BuiltInWindowFunction::Lead
            | BuiltInWindowFunction::FirstValue
            | BuiltInWindowFunction::LastValue => Ok(arg_types[0].clone()),
        },
    }
}

/// the signatures supported by the function `fun`.
fn signature(fun: &WindowFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.
    match fun {
        WindowFunction::AggregateFunction(fun) => aggregates::signature(fun),
        WindowFunction::BuiltInWindowFunction(fun) => match fun {
            BuiltInWindowFunction::RowNumber
            | BuiltInWindowFunction::Rank
            | BuiltInWindowFunction::DenseRank => Signature::Any(0),
            BuiltInWindowFunction::Lag
            | BuiltInWindowFunction::Lead
            | BuiltInWindowFunction::FirstValue
            | BuiltInWindowFunction::LastValue => Signature::Any(1),
        },
    }
}
