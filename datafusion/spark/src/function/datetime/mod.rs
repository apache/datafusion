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

pub mod last_day;
pub mod next_day;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(last_day::SparkLastDay, last_day);
make_udf_function!(next_day::SparkNextDay, next_day);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        last_day,
        "Returns the last day of the month which the date belongs to.",
        arg1
    ));
    // TODO: add once ANSI support is added:
    // "When both of the input parameters are not NULL and day_of_week is an invalid input, the function throws SparkIllegalArgumentException if spark.sql.ansi.enabled is set to true, otherwise NULL."
    export_functions!((
        next_day,
        "Returns the first date which is later than start_date and named as indicated. The function returns NULL if at least one of the input parameters is NULL.",
        arg1 arg2
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![last_day(), next_day()]
}
