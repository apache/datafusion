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

use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use stabby::vec::Vec as SVec;

use crate::{arrow_wrappers::WrappedArray, physical_expr::{FFI_PhysicalExpr, sort::FFI_PhysicalSortExpr}};

/// A stable struct for sharing [`RangePartitioning`] across FFI boundaries.
/// See ['RangePartitioning'] for the descriptions of each field.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_RangePartitioning {
    split_points: SVec<SVec<WrappedArray>>,
    ordering: SVec<FFI_PhysicalSortExpr>,
}

/// A stable struct for sharing [`Partitioning`] across FFI boundaries.
/// See ['Partitioning'] for the meaning of each variant.
#[repr(C)]
#[derive(Debug)]
pub enum FFI_Partitioning {
    RoundRobinBatch(usize),
    Hash(SVec<FFI_PhysicalExpr>, usize),
    UnknownPartitioning(usize),
    Range(FFI_RangePartitioning),
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
            Partitioning::Range(range) => {
                let split_points = range.split_points().iter().map(|s| s.values().iter().map(|v| v.try_into().expect("Fail") ).collect() ).collect();
                let ordering = range.ordering().iter().map(|e| FFI_PhysicalSortExpr::from(e)).collect();
                Self::Range(FFI_RangePartitioning { split_points, ordering})
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
            FFI_Partitioning::Range()
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
