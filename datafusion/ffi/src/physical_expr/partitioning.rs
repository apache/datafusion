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

use std::sync::Arc;

use abi_stable::StableAbi;
use abi_stable::std_types::RVec;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::physical_expr::FFI_PhysicalExpr;

/// A stable struct for sharing [`Partitioning`] across FFI boundaries.
/// See ['Partitioning'] for the meaning of each variant.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_Partitioning {
    RoundRobinBatch(usize),
    Hash(RVec<FFI_PhysicalExpr>, usize),
    UnknownPartitioning(usize),
}

impl From<&Partitioning> for FFI_Partitioning {
    fn from(value: &Partitioning) -> Self {
        match value {
            Partitioning::RoundRobinBatch(size) => Self::RoundRobinBatch(*size),
            Partitioning::Hash(exprs, size) => {
                let exprs = exprs
                    .iter()
                    .map(Arc::clone)
                    .map(FFI_PhysicalExpr::from)
                    .collect();
                Self::Hash(exprs, *size)
            }
            Partitioning::UnknownPartitioning(size) => Self::UnknownPartitioning(*size),
        }
    }
}

impl From<&FFI_Partitioning> for Partitioning {
    fn from(value: &FFI_Partitioning) -> Self {
        match value {
            FFI_Partitioning::RoundRobinBatch(size) => {
                Partitioning::RoundRobinBatch(*size)
            }
            FFI_Partitioning::Hash(exprs, size) => {
                let exprs = exprs.iter().map(<Arc<dyn PhysicalExpr>>::from).collect();
                Self::Hash(exprs, *size)
            }
            FFI_Partitioning::UnknownPartitioning(size) => {
                Self::UnknownPartitioning(*size)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_physical_expr::Partitioning;
    use datafusion_physical_expr::expressions::lit;

    use crate::physical_expr::partitioning::FFI_Partitioning;

    #[test]
    fn round_trip_ffi_partitioning() {
        for partitioning in [
            Partitioning::RoundRobinBatch(10),
            Partitioning::Hash(vec![lit(1)], 10),
            Partitioning::UnknownPartitioning(10),
        ] {
            let ffi_partitioning: FFI_Partitioning = (&partitioning).into();
            let returned: Partitioning = (&ffi_partitioning).into();

            if let Partitioning::UnknownPartitioning(return_size) = returned {
                let Partitioning::UnknownPartitioning(original_size) = partitioning
                else {
                    panic!("Expected unknown partitioning")
                };
                assert_eq!(return_size, original_size);
            } else {
                assert_eq!(partitioning, returned);
            }
        }
    }
}
