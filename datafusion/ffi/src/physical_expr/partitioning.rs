use crate::physical_expr::{FFI_PhysicalExpr, ForeignPhysicalExpr};
use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use std::sync::Arc;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions
    Hash(RVec<FFI_PhysicalExpr>, usize),
    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl From<&Partitioning> for FFI_Partitioning {
    fn from(partitioning: &Partitioning) -> Self {
        match partitioning {
            Partitioning::UnknownPartitioning(size) => Self::UnknownPartitioning(*size),
            Partitioning::RoundRobinBatch(size) => Self::RoundRobinBatch(*size),
            Partitioning::Hash(exprs, size) => {
                let exprs = exprs.iter().cloned().map(Into::into).collect();
                Self::Hash(exprs, *size)
            }
        }
    }
}

impl From<FFI_Partitioning> for Partitioning {
    fn from(partitioning: FFI_Partitioning) -> Self {
        match partitioning {
            FFI_Partitioning::UnknownPartitioning(size) => {
                Self::UnknownPartitioning(size)
            }
            FFI_Partitioning::RoundRobinBatch(size) => Self::RoundRobinBatch(size),
            FFI_Partitioning::Hash(exprs, size) => {
                let exprs = exprs
                    .into_iter()
                    .map(|expr| {
                        <Arc<dyn PhysicalExpr>>::from(expr)
                    })
                    .collect();
                Self::Hash(exprs, size)
            }
        }
    }
}
