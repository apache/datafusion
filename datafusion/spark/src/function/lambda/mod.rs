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

pub mod array_exists;

use datafusion_expr::{Expr, HigherOrderUDF, ScalarUDF, expr::HigherOrderFunction};
use std::sync::{Arc, LazyLock};

pub fn array_exists_higher_order_function() -> Arc<dyn HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<dyn HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(array_exists::SparkArrayExists::new()));
    Arc::clone(&INSTANCE)
}

pub mod expr_fn {
    use super::*;

    /// Returns true if the predicate holds for at least one element of the array.
    pub fn array_exists(array: Expr, lambda: Expr) -> Expr {
        Expr::HigherOrderFunction(HigherOrderFunction::new(
            array_exists_higher_order_function(),
            vec![array, lambda],
        ))
    }
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![]
}

pub fn higher_order_functions() -> Vec<Arc<dyn HigherOrderUDF>> {
    vec![array_exists_higher_order_function()]
}
