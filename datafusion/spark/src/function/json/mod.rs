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

pub mod json_tuple;
pub mod to_json;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(json_tuple::JsonTuple, json_tuple);
make_udf_function!(to_json::SparkToJson, to_json);

pub mod expr_fn {
    use datafusion_expr::Expr;
    use datafusion_functions::export_functions;

    use crate::function::json::to_json::IntoJsonStruct;

    export_functions!((
        json_tuple,
        "Extracts top-level fields from a JSON string and returns them as a struct.",
        args,
    ));

    /// Serializes a struct value to a JSON string (Spark-compatible `to_json`).
    ///
    /// The input is either an expression that evaluates to a struct,
    /// or a slice of column names that are packed into a struct first.
    /// The result is a regular expression and can be used anywhere an expression is accepted,
    /// for example in `DataFrame::with_column`.
    ///
    /// NULL fields are omitted from the object (Spark's `ignoreNullFields`
    /// default) and a NULL struct produces NULL.
    ///
    /// # Example
    ///
    /// ```
    /// use datafusion::prelude::*;
    /// use datafusion_spark::expr_fn::to_json;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .sql("SELECT * FROM (VALUES (1, 'x'), (2, NULL)) AS t(foo, bar)")
    ///     .await?
    ///     // pack columns into a struct and serialize it
    ///     .with_column("json", to_json(&["foo", "bar"]))?
    ///     // or serialize an existing struct column
    ///     .with_column("event", named_struct(vec![lit("foo"), col("foo")]))?
    ///     .with_column("event_json", to_json(col("event")))?
    ///     .select(vec![col("foo"), col("bar"), col("json"), col("event_json")])?;
    ///
    /// datafusion::assert_batches_sorted_eq!(
    ///     [
    ///         "+-----+-----+---------------------+------------+",
    ///         "| foo | bar | json                | event_json |",
    ///         "+-----+-----+---------------------+------------+",
    ///         "| 1   | x   | {\"foo\":1,\"bar\":\"x\"} | {\"foo\":1}  |",
    ///         "| 2   |     | {\"foo\":2}           | {\"foo\":2}  |",
    ///         "+-----+-----+---------------------+------------+",
    ///     ],
    ///     &df.collect().await?
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_json(input: impl IntoJsonStruct) -> Expr {
        super::to_json().call(vec![input.into_struct_expr()])
    }
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![json_tuple(), to_json()]
}
