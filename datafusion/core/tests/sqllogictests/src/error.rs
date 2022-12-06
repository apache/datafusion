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

use arrow::error::ArrowError;
use datafusion_common::DataFusionError;
use sqllogictest::TestError;
use sqlparser::parser::ParserError;
use std::error;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, DFSqlLogicTestError>;

/// DataFusion sql-logicaltest error
#[derive(Debug)]
pub enum DFSqlLogicTestError {
    /// Error from sqllogictest-rs
    SqlLogicTest(TestError),
    /// Error from datafusion
    DataFusion(DataFusionError),
    /// Error returned when SQL is syntactically incorrect.
    Sql(ParserError),
    /// Error from arrow-rs
    Arrow(ArrowError),
}

impl From<TestError> for DFSqlLogicTestError {
    fn from(value: TestError) -> Self {
        DFSqlLogicTestError::SqlLogicTest(value)
    }
}

impl From<DataFusionError> for DFSqlLogicTestError {
    fn from(value: DataFusionError) -> Self {
        DFSqlLogicTestError::DataFusion(value)
    }
}

impl From<ParserError> for DFSqlLogicTestError {
    fn from(value: ParserError) -> Self {
        DFSqlLogicTestError::Sql(value)
    }
}

impl From<ArrowError> for DFSqlLogicTestError {
    fn from(value: ArrowError) -> Self {
        DFSqlLogicTestError::Arrow(value)
    }
}

impl Display for DFSqlLogicTestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DFSqlLogicTestError::SqlLogicTest(error) => write!(
                f,
                "SqlLogicTest error(from sqllogictest-rs crate): {}",
                error
            ),
            DFSqlLogicTestError::DataFusion(error) => {
                write!(f, "DataFusion error: {}", error)
            }
            DFSqlLogicTestError::Sql(error) => write!(f, "SQL Parser error: {}", error),
            DFSqlLogicTestError::Arrow(error) => write!(f, "Arrow error: {}", error),
        }
    }
}

impl error::Error for DFSqlLogicTestError {}
