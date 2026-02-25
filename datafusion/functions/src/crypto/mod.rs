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

//! "crypto" DataFusion functions

use datafusion_expr::ScalarUDF;
use std::sync::Arc;

pub mod basic;
pub mod digest;
pub mod md5;
pub mod sha;
make_udf_function!(digest::DigestFunc, digest);
make_udf_function!(md5::Md5Func, md5);
make_udf_function!(sha::SHAFunc, sha224, sha::SHAFunc::sha224);
make_udf_function!(sha::SHAFunc, sha256, sha::SHAFunc::sha256);
make_udf_function!(sha::SHAFunc, sha384, sha::SHAFunc::sha384);
make_udf_function!(sha::SHAFunc, sha512, sha::SHAFunc::sha512);

pub mod expr_fn {
    export_functions!((
        digest,
        "Computes the binary hash of an expression using the specified algorithm.",
        input_arg1 input_arg2
    ),(
        md5,
        "Computes an MD5 128-bit checksum for a string expression.",
        input_arg
    ),(
        sha224,
        "Computes the SHA-224 hash of a binary string.",
        input_arg1
    ),(
        sha256,
        "Computes the SHA-256 hash of a binary string.",
        input_arg1
    ),(
        sha384,
        "Computes the SHA-384 hash of a binary string.",
        input_arg1
    ),(
        sha512,
        "Computes the SHA-512 hash of a binary string.",
        input_arg1
    ));
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![digest(), md5(), sha224(), sha256(), sha384(), sha512()]
}
