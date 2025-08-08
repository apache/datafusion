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

pub mod murmur3;
pub mod sha2;
pub mod utils;
pub mod xxhash64;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(sha2::SparkSha2, sha2);
make_udf_function!(xxhash64::SparkXxHash64, xxhash64);
make_udf_function!(murmur3::SparkMurmur3Hash, hash);

pub mod expr_fn {
    use datafusion_functions::export_functions;
    export_functions!(
        (sha2, "sha2(expr, bitLength) - Returns a checksum of SHA-2 family as a hex string of expr. SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent to 256.", arg1 arg2),
        (xxhash64, "xxhash64(*expr) - Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column. The hash computation uses an initial seed of 42.", args),
        (hash, "hash(*expr) - Calculates the hash code of given columns using the 32-bit variant of the MurmurHash3 algorithm, and returns the result as an integer column. The hash computation uses an initial seed of 42.", args)
    );
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![sha2(), xxhash64(), hash()]
}
