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

use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::FieldRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::function::ExpressionArgs;
use datafusion_physical_expr::PhysicalExpr;
use stabby::vec::Vec as SVec;

use crate::arrow_wrappers::WrappedSchema;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::util::rvec_wrapped_to_vec_fieldref;

/// A stable struct for sharing [`ExpressionArgs`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ExpressionArgs {
    input_exprs: SVec<FFI_PhysicalExpr>,
    input_fields: SVec<WrappedSchema>,
}

impl TryFrom<ExpressionArgs<'_>> for FFI_ExpressionArgs {
    type Error = DataFusionError;

    fn try_from(args: ExpressionArgs) -> Result<Self> {
        let input_exprs = args
            .input_exprs()
            .iter()
            .map(Arc::clone)
            .map(FFI_PhysicalExpr::from)
            .collect();

        let input_fields = args
            .input_fields()
            .iter()
            .map(|field| FFI_ArrowSchema::try_from(field.as_ref()).map(WrappedSchema))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect();

        Ok(Self {
            input_exprs,
            input_fields,
        })
    }
}

pub struct ForeignExpressionArgs {
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    input_fields: Vec<FieldRef>,
}

impl TryFrom<FFI_ExpressionArgs> for ForeignExpressionArgs {
    type Error = DataFusionError;

    fn try_from(value: FFI_ExpressionArgs) -> Result<Self> {
        let input_exprs = value.input_exprs.iter().map(Into::into).collect();

        let input_fields = rvec_wrapped_to_vec_fieldref(&value.input_fields)?;

        Ok(Self {
            input_exprs,
            input_fields,
        })
    }
}

impl<'a> From<&'a ForeignExpressionArgs> for ExpressionArgs<'a> {
    fn from(value: &'a ForeignExpressionArgs) -> Self {
        ExpressionArgs::new(&value.input_exprs, &value.input_fields)
    }
}
