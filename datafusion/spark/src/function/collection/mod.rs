// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

pub mod size;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(size::SparkSize, size);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((size, "Return the size of an array or map.", arg));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![size()]
}
