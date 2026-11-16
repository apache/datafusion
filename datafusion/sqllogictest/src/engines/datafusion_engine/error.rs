// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use sqllogictest::TestError;
use sqlparser::parser::ParserError;
use thiserror::Error;

pub type Result<T, E = DFSqlLogicTestError> = std::result::Result<T, E>;

/// DataFusion sql-logicaltest error
#[derive(Debug, Error)]
pub enum DFSqlLogicTestError {
    /// Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(#[from] Box<TestError>),
    /// Error from datafusion
    #[error("DataFusion error: {}", .0.strip_backtrace())]
    DataFusion(#[from] DataFusionError),
    /// Error returned when SQL is syntactically incorrect.
    #[error("SQL Parser error: {0}")]
    Sql(#[from] ParserError),
    /// Error from arrow-rs
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// Generic error
    #[error("Other Error: {0}")]
    Other(String),
}

impl From<String> for DFSqlLogicTestError {
    fn from(value: String) -> Self {
        DFSqlLogicTestError::Other(value)
    }
}
