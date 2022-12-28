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
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DFSqlLogicTestError>;

/// DataFusion sql-logicaltest error
#[derive(Debug, Error)]
pub enum DFSqlLogicTestError {
    /// Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(TestError),
    /// Error from datafusion
    #[error("DataFusion error: {0}")]
    DataFusion(DataFusionError),
    /// Error returned when SQL is syntactically incorrect.
    #[error("SQL Parser error: {0}")]
    Sql(ParserError),
    /// Error from arrow-rs
    #[error("Arrow error: {0}")]
    Arrow(ArrowError),
    /// Generic error
    #[error("Other Error: {0}")]
    Other(String),
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

impl From<String> for DFSqlLogicTestError {
    fn from(value: String) -> Self {
        DFSqlLogicTestError::Other(value)
    }
}
