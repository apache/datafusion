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

pub mod expm1;
pub mod hex;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(expm1::SparkExpm1, expm1);
make_udf_function!(hex::SparkHex, hex);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((expm1, "Returns exp(expr) - 1 as a Float64.", arg1));
    export_functions!((hex, "Computes hex value of the given column.", arg1));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![expm1(), hex()]
}
