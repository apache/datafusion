// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::AggregateUDF;
use std::sync::Arc;

pub mod avg;
pub mod collect;
pub mod try_sum;

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((avg, "Returns the average value of a given column", arg1));
    export_functions!((
        try_sum,
        "Returns the sum of values for a column, or NULL if overflow occurs",
        arg1
    ));
    export_functions!((
        collect_list,
        "Returns a list created from the values in a column",
        arg1
    ));
    export_functions!((
        collect_set,
        "Returns a set created from the values in a column",
        arg1
    ));
}

// TODO: try use something like datafusion_functions_aggregate::create_func!()
pub fn avg() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(avg::SparkAvg::new()))
}
pub fn try_sum() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(try_sum::SparkTrySum::new()))
}
pub fn collect_list() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(collect::SparkCollectList::new()))
}
pub fn collect_set() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(collect::SparkCollectSet::new()))
}

pub fn functions() -> Vec<Arc<AggregateUDF>> {
    vec![avg(), try_sum(), collect_list(), collect_set()]
}
