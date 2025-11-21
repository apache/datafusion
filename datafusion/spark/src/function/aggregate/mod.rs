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

use datafusion_expr::AggregateUDF;
use std::sync::Arc;

pub mod avg;
pub mod try_sum;

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((avg, "Returns the average value of a given column", arg1));
    export_functions!((
        try_sum,
        "Returns the sum of values for a column, or NULL if overflow occurs",
        arg1
    ));
}

// TODO: try use something like datafusion_functions_aggregate::create_func!()
pub fn avg() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(avg::SparkAvg::new()))
}
pub fn try_sum() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(try_sum::SparkTrySum::new()))
}

pub fn functions() -> Vec<Arc<AggregateUDF>> {
    vec![avg(), try_sum()]
}
