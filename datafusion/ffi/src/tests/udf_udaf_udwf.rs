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

use crate::{
    udaf::FFI_AggregateUDF, udf::FFI_ScalarUDF, udtf::FFI_TableFunction,
    udwf::FFI_WindowUDF,
};

use crate::session::task_ctx_accessor::FFI_TaskContextAccessor;
use datafusion_catalog::TableFunctionImpl;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_functions::math::abs::AbsFunc;
use datafusion_functions::math::random::RandomFunc;
use datafusion_functions_aggregate::stddev::Stddev;
use datafusion_functions_aggregate::sum::Sum;
use datafusion_functions_table::generate_series::RangeFunc;
use datafusion_functions_window::rank::Rank;
use std::sync::Arc;

pub(crate) extern "C" fn create_ffi_abs_func() -> FFI_ScalarUDF {
    let udf: Arc<ScalarUDF> = Arc::new(AbsFunc::new().into());

    udf.into()
}

pub(crate) extern "C" fn create_ffi_random_func() -> FFI_ScalarUDF {
    let udf: Arc<ScalarUDF> = Arc::new(RandomFunc::new().into());

    udf.into()
}

pub(crate) extern "C" fn create_ffi_table_func(
    task_ctx_accessor: FFI_TaskContextAccessor,
) -> FFI_TableFunction {
    let udtf: Arc<dyn TableFunctionImpl> = Arc::new(RangeFunc {});

    FFI_TableFunction::new(udtf, None, task_ctx_accessor)
}

pub(crate) extern "C" fn create_ffi_sum_func(
    task_ctx_accessor: FFI_TaskContextAccessor,
) -> FFI_AggregateUDF {
    let udaf: Arc<AggregateUDF> = Arc::new(Sum::new().into());

    FFI_AggregateUDF::new(udaf, task_ctx_accessor)
}

pub(crate) extern "C" fn create_ffi_stddev_func(
    task_ctx_accessor: FFI_TaskContextAccessor,
) -> FFI_AggregateUDF {
    let udaf: Arc<AggregateUDF> = Arc::new(Stddev::new().into());

    FFI_AggregateUDF::new(udaf, task_ctx_accessor)
}

pub(crate) extern "C" fn create_ffi_rank_func(
    task_ctx_accessor: FFI_TaskContextAccessor,
) -> FFI_WindowUDF {
    let udwf: Arc<WindowUDF> = Arc::new(
        Rank::new(
            "rank_demo".to_string(),
            datafusion_functions_window::rank::RankType::Basic,
        )
        .into(),
    );

    FFI_WindowUDF::new(udwf, task_ctx_accessor)
}
