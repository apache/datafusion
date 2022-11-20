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

//! This module contains functions that change types or names of other
//! kernels to make them compatible with the main dispatch logic

use std::sync::Arc;

use arrow::array::*;
use datafusion_common::Result;

/// create a `dyn_op` wrapper function for the specified operation
/// that call the underlying dyn_op arrow kernel if the type is
/// supported, and translates ArrowError to DataFusionError
macro_rules! make_dyn_comp_op {
    ($OP:tt) => {
        paste::paste! {
            /// wrapper over arrow compute kernel that maps Error types and
            /// patches missing support in arrow
            pub(crate) fn [<$OP _dyn>] (left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
                arrow::compute::kernels::comparison::[<$OP _dyn>](left, right)
                            .map_err(|e| e.into())
                            .map(|a| Arc::new(a) as ArrayRef)
            }
        }
    };
}

// create eq_dyn, gt_dyn, wrappers etc
make_dyn_comp_op!(eq);
make_dyn_comp_op!(gt);
make_dyn_comp_op!(gt_eq);
make_dyn_comp_op!(lt);
make_dyn_comp_op!(lt_eq);
make_dyn_comp_op!(neq);
