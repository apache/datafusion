// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

pub mod stack;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(stack::SparkStack, stack);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        stack,
        "Separates the values into `n` rows, where `n` is an integer literal, and returns them as a list of structs with fields `col0`, `col1`, and so on, which `unnest` expands.",
        args,
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![stack()]
}
