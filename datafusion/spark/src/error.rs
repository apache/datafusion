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

use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;

#[derive(thiserror::Error, Debug)]
pub enum SparkError {
    // Note that this message format is based on Spark 3.4 and is more detailed than the message
    // returned by Spark 3.3
    #[error("[CAST_INVALID_INPUT] The value '{value}' of the type \"{from_type}\" cannot be cast to \"{to_type}\" \
        because it is malformed. Correct the value as per the syntax, or change its target type. \
        Use `try_cast` to tolerate malformed input and return NULL instead. If necessary \
        set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    CastInvalidValue {
        value: String,
        from_type: String,
        to_type: String,
    },

    #[error("[NUMERIC_VALUE_OUT_OF_RANGE] {value} cannot be represented as Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error, and return NULL instead.")]
    NumericValueOutOfRange {
        value: String,
        precision: u8,
        scale: i8,
    },

    #[error("[CAST_OVERFLOW] The value {value} of the type \"{from_type}\" cannot be cast to \"{to_type}\" \
        due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. If necessary \
        set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    CastOverFlow {
        value: String,
        from_type: String,
        to_type: String,
    },

    #[error("[ARITHMETIC_OVERFLOW] {from_type} overflow. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    ArithmeticOverflow { from_type: String },

    #[error("ArrowError: {0}.")]
    Arrow(ArrowError),

    #[error("InternalError: {0}.")]
    Internal(String),
}

pub type SparkResult<T> = Result<T, SparkError>;

impl From<ArrowError> for SparkError {
    fn from(value: ArrowError) -> Self {
        SparkError::Arrow(value)
    }
}

impl From<SparkError> for DataFusionError {
    fn from(value: SparkError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}
