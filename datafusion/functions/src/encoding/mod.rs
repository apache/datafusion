// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::ScalarUDF;
use std::sync::Arc;

pub mod inner;

// create `encode` and `decode` UDFs
make_udf_function!(inner::EncodeFunc, encode);
make_udf_function!(inner::DecodeFunc, decode);

// Export the functions out of this package, both as expr_fn as well as a list of functions
pub mod expr_fn {
    export_functions!( (
        encode,
        "encode the `input`, using the `encoding`. encoding can be base64 or hex",
        input encoding
    ),(
        decode,
        "decode the `input`, using the `encoding`. encoding can be base64 or hex",
        input encoding
    ));
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![encode(), decode()]
}
