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

pub mod bitmap_bit_position;
pub mod bitmap_bucket_number;
pub mod bitmap_count;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(bitmap_count::BitmapCount, bitmap_count);
make_udf_function!(bitmap_bit_position::BitmapBitPosition, bitmap_bit_position);
make_udf_function!(
    bitmap_bucket_number::BitmapBucketNumber,
    bitmap_bucket_number
);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        bitmap_count,
        "Returns the number of set bits in the input bitmap.",
        arg
    ));
    export_functions!((
        bitmap_bit_position,
        "Returns the bit position for the given input child expression.",
        arg
    ));
    export_functions!((
        bitmap_bucket_number,
        "Returns the bucket number for the given input child expression.",
        arg
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        bitmap_count(),
        bitmap_bit_position(),
        bitmap_bucket_number(),
    ]
}
