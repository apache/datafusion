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

pub mod cast;
pub mod cast_boolean;
pub mod cast_complex;
pub mod cast_datetime;
pub mod cast_numeric;
pub mod cast_string;
pub mod cast_utils;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(cast::SparkCast, spark_cast);
make_udf_function!(cast::SparkTryCast, spark_try_cast);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        spark_cast,
        "Casts expr to the target type using Spark-compatible semantics.",
        arg1 arg2
    ));
    export_functions!((
        spark_try_cast,
        "Casts expr to the target type using Spark TRY_CAST semantics (returns NULL on error).",
        arg1 arg2
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![spark_cast(), spark_try_cast()]
}
