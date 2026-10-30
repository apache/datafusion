// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::FieldRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::function::PartitionEvaluatorArgs;
use datafusion_physical_plan::PhysicalExpr;
use stabby::vec::Vec as SVec;

use crate::arrow_wrappers::WrappedSchema;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::util::rvec_wrapped_to_vec_fieldref;

/// A stable struct for sharing [`PartitionEvaluatorArgs`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`PartitionEvaluatorArgs`].
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PartitionEvaluatorArgs {
    input_exprs: SVec<FFI_PhysicalExpr>,
    input_fields: SVec<WrappedSchema>,
    is_reversed: bool,
    ignore_nulls: bool,
}

impl TryFrom<PartitionEvaluatorArgs<'_>> for FFI_PartitionEvaluatorArgs {
    type Error = DataFusionError;

    fn try_from(args: PartitionEvaluatorArgs) -> Result<Self, DataFusionError> {
        let input_exprs = args
            .input_exprs()
            .iter()
            .map(Arc::clone)
            .map(FFI_PhysicalExpr::from)
            .collect();

        let input_fields = args
            .input_fields()
            .iter()
            .map(|input_type| FFI_ArrowSchema::try_from(input_type).map(WrappedSchema))
            .collect::<Result<Vec<_>, ArrowError>>()?
            .into_iter()
            .collect();

        Ok(Self {
            input_exprs,
            input_fields,
            is_reversed: args.is_reversed(),
            ignore_nulls: args.ignore_nulls(),
        })
    }
}

/// This struct mirrors PartitionEvaluatorArgs except that it contains owned data.
/// It is necessary to create this struct so that we can parse the protobuf
/// data across the FFI boundary and turn it into owned data that
/// PartitionEvaluatorArgs can then reference.
pub struct ForeignPartitionEvaluatorArgs {
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    input_fields: Vec<FieldRef>,
    is_reversed: bool,
    ignore_nulls: bool,
}

impl TryFrom<FFI_PartitionEvaluatorArgs> for ForeignPartitionEvaluatorArgs {
    type Error = DataFusionError;

    fn try_from(value: FFI_PartitionEvaluatorArgs) -> Result<Self> {
        let input_exprs = value.input_exprs.iter().map(Into::into).collect();

        let input_fields = rvec_wrapped_to_vec_fieldref(&value.input_fields)?;

        Ok(Self {
            input_exprs,
            input_fields,
            is_reversed: value.is_reversed,
            ignore_nulls: value.ignore_nulls,
        })
    }
}

impl<'a> From<&'a ForeignPartitionEvaluatorArgs> for PartitionEvaluatorArgs<'a> {
    fn from(value: &'a ForeignPartitionEvaluatorArgs) -> Self {
        PartitionEvaluatorArgs::new(
            &value.input_exprs,
            &value.input_fields,
            value.is_reversed,
            value.ignore_nulls,
        )
    }
}

#[cfg(test)]
mod tests {}
