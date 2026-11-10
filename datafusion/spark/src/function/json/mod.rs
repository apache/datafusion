// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

pub mod json_tuple;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(json_tuple::JsonTuple, json_tuple);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        json_tuple,
        "Extracts top-level fields from a JSON string and returns them as a struct.",
        args,
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![json_tuple()]
}
