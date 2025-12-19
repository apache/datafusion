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
use datafusion::{
    catalog::TableFunctionImpl,
    functions::math::{abs::AbsFunc, random::RandomFunc},
    functions_aggregate::{stddev::Stddev, sum::Sum},
    functions_table::generate_series::RangeFunc,
    functions_window::rank::Rank,
    logical_expr::{AggregateUDF, ScalarUDF, WindowUDF},
};
use std::any::Any;

use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Signature};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::sync::Arc;

pub(crate) extern "C" fn create_ffi_abs_func() -> FFI_ScalarUDF {
    let inner = WrappedAbs(Arc::new(AbsFunc::new().into()));
    let udf: Arc<ScalarUDF> = Arc::new(inner.into());

    udf.into()
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct WrappedAbs(Arc<ScalarUDF>);

impl ScalarUDFImpl for WrappedAbs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ffi_abs"
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        self.0.return_type(arg_types)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        self.0.invoke_with_args(args)
    }
}

pub(crate) extern "C" fn create_ffi_random_func() -> FFI_ScalarUDF {
    let udf: Arc<ScalarUDF> = Arc::new(RandomFunc::new().into());

    udf.into()
}

pub(crate) extern "C" fn create_ffi_table_func(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableFunction {
    let udtf: Arc<dyn TableFunctionImpl> = Arc::new(RangeFunc {});

    FFI_TableFunction::new_with_ffi_codec(udtf, None, codec)
}

pub(crate) extern "C" fn create_ffi_sum_func() -> FFI_AggregateUDF {
    let udaf: Arc<AggregateUDF> = Arc::new(Sum::new().into());

    udaf.into()
}

pub(crate) extern "C" fn create_ffi_stddev_func() -> FFI_AggregateUDF {
    let udaf: Arc<AggregateUDF> = Arc::new(Stddev::new().into());

    udaf.into()
}

pub(crate) extern "C" fn create_ffi_rank_func() -> FFI_WindowUDF {
    let udwf: Arc<WindowUDF> = Arc::new(
        Rank::new(
            "rank_demo".to_string(),
            datafusion::functions_window::rank::RankType::Basic,
        )
        .into(),
    );

    udwf.into()
}
