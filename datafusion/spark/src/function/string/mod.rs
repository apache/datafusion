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

pub mod ascii;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(ascii::SparkAscii, spark_ascii);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        spark_ascii,
        "Returns the ASCII code point of the first character of string.",
        arg1
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![spark_ascii()]
}
