// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

mod equal_null;

make_udf_function!(equal_null::SparkEqualNull, equal_null);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        equal_null,
        "Returns true if arg1 equals arg2, or if both are NULL; false otherwise",
        arg1 arg2
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![equal_null()]
}
