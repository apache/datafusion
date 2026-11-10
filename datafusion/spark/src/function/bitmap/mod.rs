// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
