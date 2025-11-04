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

use crate::expr::expr_properties::FFI_SortOptions;
use crate::physical_expr::{FFI_PhysicalExpr, ForeignPhysicalExpr};
use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use arrow_schema::SortOptions;
use datafusion_common::{exec_datafusion_err, DataFusionError};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use std::sync::Arc;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

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
        let expr = <Arc<dyn PhysicalExpr>>::from(value.expr.clone());
        let options = SortOptions::from(&value.options);

        Self { expr, options }
    }
}

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_LexOrdering {
    pub expr: RVec<FFI_PhysicalSortExpr>,
}

impl From<&LexOrdering> for FFI_LexOrdering {
    fn from(value: &LexOrdering) -> Self {
        let expr = value.iter().map(FFI_PhysicalSortExpr::from).collect();
        FFI_LexOrdering { expr }
    }
}

impl TryFrom<&FFI_LexOrdering> for LexOrdering {
    type Error = DataFusionError;
    fn try_from(value: &FFI_LexOrdering) -> Result<Self, Self::Error> {
        LexOrdering::new(value.expr.iter().map(PhysicalSortExpr::from))
            .ok_or(exec_datafusion_err!("FFI_LexOrdering was empty"))
    }
}
