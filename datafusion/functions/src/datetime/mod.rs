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

//! date & time DataFusion functions

use std::sync::Arc;

use datafusion_expr::ScalarUDF;

mod common;
mod to_date;
mod to_timestamp;

// create UDFs
make_udf_function!(to_date::ToDateFunc, TO_DATE, to_date);
make_udf_function!(to_timestamp::ToTimestampFunc, TO_TIMESTAMP, to_timestamp);
make_udf_function!(
    to_timestamp::ToTimestampSecondsFunc,
    TO_TIMESTAMP_SECONDS,
    to_timestamp_seconds
);
make_udf_function!(
    to_timestamp::ToTimestampMillisFunc,
    TO_TIMESTAMP_MILLIS,
    to_timestamp_millis
);
make_udf_function!(
    to_timestamp::ToTimestampMicrosFunc,
    TO_TIMESTAMP_MICROS,
    to_timestamp_micros
);
make_udf_function!(
    to_timestamp::ToTimestampNanosFunc,
    TO_TIMESTAMP_NANOS,
    to_timestamp_nanos
);

// we cannot currently use the export_functions macro since it doesn't handle
// functions with varargs currently

pub mod expr_fn {
    use datafusion_expr::Expr;

    /// ```ignore
    /// # use std::sync::Arc;
    ///
    /// # use datafusion_common::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// #  use arrow::array::StringArray;
    /// #  use arrow::datatypes::{DataType, Field, Schema};
    /// #  use arrow::record_batch::RecordBatch;
    /// #  use datafusion_expr::col;
    /// #  use datafusion::prelude::*;
    /// #  use datafusion_functions::expr_fn::to_date;
    ///     
    ///     // define a schema.
    ///     let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
    ///
    ///     // define data.
    ///     let batch = RecordBatch::try_new(
    ///         schema,
    ///         vec![Arc::new(StringArray::from(vec![
    ///             "2020-09-08T13:42:29Z",
    ///             "2020-09-08T13:42:29.190855-05:00",
    ///             "2020-08-09 12:13:29",
    ///             "2020-01-02",
    ///         ]))],
    ///     )?;
    ///
    ///     // declare a new context. In spark API, this corresponds to a new spark SQLsession
    ///     let ctx = SessionContext::new();
    ///
    ///     // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ///     ctx.register_batch("t", batch)?;
    ///     let df = ctx.table("t").await?;
    ///
    ///     // use to_date function to convert col 'a' to timestamp type using the default parsing
    ///     let df = df.with_column("a", to_date(vec![col("a")]))?;
    ///
    ///     let df = df.select_columns(&["a"])?;
    ///
    ///     // print the results
    ///     df.show().await?;
    ///
    ///     # Ok(())
    /// # }
    /// ```
    pub fn to_date(args: Vec<Expr>) -> Expr {
        super::to_date().call(args)
    }

    #[doc = "converts a string and optional formats to a `Timestamp(Nanoseconds, None)`"]
    pub fn to_timestamp(args: Vec<Expr>) -> Expr {
        super::to_timestamp().call(args)
    }

    #[doc = "converts a string and optional formats to a `Timestamp(Seconds, None)`"]
    pub fn to_timestamp_seconds(args: Vec<Expr>) -> Expr {
        super::to_timestamp_seconds().call(args)
    }

    #[doc = "converts a string and optional formats to a `Timestamp(Milliseconds, None)`"]
    pub fn to_timestamp_millis(args: Vec<Expr>) -> Expr {
        super::to_timestamp_millis().call(args)
    }

    #[doc = "converts a string and optional formats to a `Timestamp(Microseconds, None)`"]
    pub fn to_timestamp_micros(args: Vec<Expr>) -> Expr {
        super::to_timestamp_micros().call(args)
    }

    #[doc = "converts a string and optional formats to a `Timestamp(Nanoseconds, None)`"]
    pub fn to_timestamp_nanos(args: Vec<Expr>) -> Expr {
        super::to_timestamp_nanos().call(args)
    }
}

///   Return a list of all functions in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        to_date(),
        to_timestamp(),
        to_timestamp_seconds(),
        to_timestamp_millis(),
        to_timestamp_micros(),
        to_timestamp_nanos(),
    ]
}
