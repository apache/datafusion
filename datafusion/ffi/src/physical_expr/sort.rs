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
use arrow_schema::SortOptions;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::expr::expr_properties::FFI_SortOptions;
use crate::physical_expr::FFI_PhysicalExpr;

/// A stable struct for sharing [`PhysicalSortExpr`] across FFI boundaries.
/// See [`PhysicalSortExpr`] for the meaning of each field.
#[repr(C)]
#[derive(Debug, StableAbi)]
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
