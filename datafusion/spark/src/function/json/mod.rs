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
make_udf_function!(to_json::ToJson, to_json);

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
    /// input is either a struct-valued [`Expr`] or a list of column names,
    /// which are packed into a struct keyed by their unqualified names first.
    /// See [`IntoJsonStruct`].
    ///
    ///
    /// // existing struct column
    /// df.with_column("json", to_json(col("event")))?;
    /// // pack columns into a struct, then serialize
    /// df.with_column("json", to_json(&["foo", "bar"]))?;
    ///
    pub fn to_json(input: impl IntoJsonStruct) -> Expr {
        super::to_json().call(vec![input.into_struct_expr()])
    }
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![json_tuple(), to_json()]
}
