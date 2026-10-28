// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

pub mod array_contains;
pub mod repeat;
pub mod shuffle;
pub mod slice;
pub mod spark_array;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(array_contains::SparkArrayContains, spark_array_contains);
make_udf_function!(spark_array::SparkArray, array);
make_udf_function!(shuffle::SparkShuffle, shuffle);
make_udf_function!(repeat::SparkArrayRepeat, array_repeat);
make_udf_function!(slice::SparkSlice, slice);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        spark_array_contains,
        "Returns true if the array contains the element (Spark semantics).",
        array element
    ));
    export_functions!((array, "Returns an array with the given elements.", args));
    export_functions!((
        shuffle,
        "Returns a random permutation of the given array.",
        args
    ));
    export_functions!((
        array_repeat,
        "returns an array containing element count times.",
        element count
    ));
    export_functions!((
        slice,
        "Returns a slice of the array from the start index with the given length.",
        array start length
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        spark_array_contains(),
        array(),
        shuffle(),
        array_repeat(),
        slice(),
    ]
}
