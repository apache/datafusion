// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod cast;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function_with_config;
use std::sync::Arc;

make_udf_function_with_config!(cast::SparkCast, spark_cast);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        spark_cast,
        "Casts given value to the specified type following Spark-compatible semantics",
        @config arg1 arg2
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    use datafusion_common::config::ConfigOptions;
    let config = ConfigOptions::default();
    vec![spark_cast(&config)]
}
