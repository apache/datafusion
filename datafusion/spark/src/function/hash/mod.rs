// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

pub mod crc32;
pub mod sha1;
pub mod sha2;
pub(crate) mod utils;
pub mod xxhash64;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(crc32::SparkCrc32, crc32);
make_udf_function!(sha1::SparkSha1, sha1);
make_udf_function!(sha2::SparkSha2, sha2);
make_udf_function!(xxhash64::SparkXxhash64, xxhash64);

pub mod expr_fn {
    use datafusion_functions::export_functions;
    export_functions!(
        (crc32, "crc32(expr) - Returns a cyclic redundancy check value of the expr as a bigint.", arg1),
        (sha1, "sha1(expr) - Returns a SHA-1 hash value of the expr as a hex string.", arg1),
        (sha2, "sha2(expr, bitLength) - Returns a checksum of SHA-2 family as a hex string of expr. SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent to 256.", arg1 arg2),
        (xxhash64, "xxhash64(expr1, expr2, ...) - Returns a 64-bit hash value of the arguments using xxHash.", args)
    );
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![crc32(), sha1(), sha2(), xxhash64()]
}
