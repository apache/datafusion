// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_schema::SortOptions;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::expr::expr_properties::FFI_SortOptions;
use crate::physical_expr::FFI_PhysicalExpr;

/// A stable struct for sharing [`PhysicalSortExpr`] across FFI boundaries.
/// See [`PhysicalSortExpr`] for the meaning of each field.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PhysicalSortExpr {
    expr: FFI_PhysicalExpr,
    options: FFI_SortOptions,
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
        let expr: Arc<dyn PhysicalExpr> = (&value.expr).into();
        let options = SortOptions::from(&value.options);

        Self { expr, options }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::SortOptions;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

    use crate::physical_expr::sort::FFI_PhysicalSortExpr;

    #[test]
    fn ffi_sort_expr_round_trip() {
        let col_expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let expr = PhysicalSortExpr::new(col_expr, SortOptions::default());

        let ffi_expr = FFI_PhysicalSortExpr::from(&expr);
        let foreign_expr = PhysicalSortExpr::from(&ffi_expr);

        assert_eq!(expr, foreign_expr);
    }
}
