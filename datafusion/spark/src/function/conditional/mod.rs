// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

mod r#if;

make_udf_function!(r#if::SparkIf, r#if);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((r#if, "If arg1 evaluates to true, then returns arg2; otherwise returns arg3", arg1 arg2 arg3));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![r#if()]
}
