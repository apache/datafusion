use crate::expr::expr_properties::FFI_SortOptions;
use crate::physical_expr::{FFI_PhysicalExpr, ForeignPhysicalExpr};
use abi_stable::StableAbi;
use arrow_schema::SortOptions;
use datafusion::physical_expr::PhysicalSortExpr;
use std::sync::Arc;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PhysicalSortExpr {
    pub expr: FFI_PhysicalExpr,
    pub options: FFI_SortOptions,
}

impl From<&PhysicalSortExpr> for FFI_PhysicalSortExpr {
    fn from(value: &PhysicalSortExpr) -> Self {
        let expr = FFI_PhysicalExpr::from(value.clone().expr);
        let options = FFI_SortOptions::from(&value.options);

        Self { expr, options }
    }
}

impl From<&FFI_PhysicalSortExpr> for PhysicalSortExpr {
    fn from(value: &FFI_PhysicalSortExpr) -> Self {
        let expr = Arc::new(ForeignPhysicalExpr::from(value.expr.clone()));
        let options = SortOptions::from(&value.options);

        Self { expr, options }
    }
}
