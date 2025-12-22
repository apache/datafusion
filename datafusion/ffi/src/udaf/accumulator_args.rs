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

use abi_stable::{
    StableAbi,
    std_types::{RString, RVec},
};
use arrow::{datatypes::Schema, ffi::FFI_ArrowSchema};
use arrow_schema::FieldRef;
use datafusion_common::error::DataFusionError;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

use crate::{
    arrow_wrappers::WrappedSchema,
    physical_expr::{FFI_PhysicalExpr, sort::FFI_PhysicalSortExpr},
    util::{rvec_wrapped_to_vec_fieldref, vec_fieldref_to_rvec_wrapped},
};

/// A stable struct for sharing [`AccumulatorArgs`] across FFI boundaries.
/// For an explanation of each field, see the corresponding field
/// defined in [`AccumulatorArgs`].
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_AccumulatorArgs {
    return_field: WrappedSchema,
    schema: WrappedSchema,
    ignore_nulls: bool,
    order_bys: RVec<FFI_PhysicalSortExpr>,
    is_reversed: bool,
    name: RString,
    is_distinct: bool,
    exprs: RVec<FFI_PhysicalExpr>,
    expr_fields: RVec<WrappedSchema>,
}

impl TryFrom<AccumulatorArgs<'_>> for FFI_AccumulatorArgs {
    type Error = DataFusionError;
    fn try_from(args: AccumulatorArgs) -> Result<Self, DataFusionError> {
        let return_field =
            WrappedSchema(FFI_ArrowSchema::try_from(args.return_field.as_ref())?);
        let schema = WrappedSchema(FFI_ArrowSchema::try_from(args.schema)?);

        let order_bys: RVec<_> = args
            .order_bys
            .iter()
            .map(FFI_PhysicalSortExpr::from)
            .collect();

        let exprs = args
            .exprs
            .iter()
            .map(Arc::clone)
            .map(FFI_PhysicalExpr::from)
            .collect();

        let expr_fields = vec_fieldref_to_rvec_wrapped(args.expr_fields)?;

        Ok(Self {
            return_field,
            schema,
            ignore_nulls: args.ignore_nulls,
            order_bys,
            is_reversed: args.is_reversed,
            name: args.name.into(),
            is_distinct: args.is_distinct,
            exprs,
            expr_fields,
        })
    }
}

/// This struct mirrors AccumulatorArgs except that it contains owned data.
/// It is necessary to create this struct so that we can parse the protobuf
/// data across the FFI boundary and turn it into owned data that
/// AccumulatorArgs can then reference.
pub struct ForeignAccumulatorArgs {
    pub return_field: FieldRef,
    pub schema: Schema,
    pub expr_fields: Vec<FieldRef>,
    pub ignore_nulls: bool,
    pub order_bys: Vec<PhysicalSortExpr>,
    pub is_reversed: bool,
    pub name: String,
    pub is_distinct: bool,
    pub exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl TryFrom<FFI_AccumulatorArgs> for ForeignAccumulatorArgs {
    type Error = DataFusionError;

    fn try_from(value: FFI_AccumulatorArgs) -> Result<Self, Self::Error> {
        let return_field = Arc::new((&value.return_field.0).try_into()?);
        let schema = Schema::try_from(&value.schema.0)?;

        let order_bys = value.order_bys.iter().map(PhysicalSortExpr::from).collect();

        let exprs = value
            .exprs
            .iter()
            .map(<Arc<dyn PhysicalExpr>>::from)
            .collect();

        let expr_fields = rvec_wrapped_to_vec_fieldref(&value.expr_fields)?;

        Ok(Self {
            return_field,
            schema,
            expr_fields,
            ignore_nulls: value.ignore_nulls,
            order_bys,
            is_reversed: value.is_reversed,
            name: value.name.to_string(),
            is_distinct: value.is_distinct,
            exprs,
        })
    }
}

impl<'a> From<&'a ForeignAccumulatorArgs> for AccumulatorArgs<'a> {
    fn from(value: &'a ForeignAccumulatorArgs) -> Self {
        Self {
            return_field: Arc::clone(&value.return_field),
            schema: &value.schema,
            expr_fields: &value.expr_fields,
            ignore_nulls: value.ignore_nulls,
            order_bys: &value.order_bys,
            is_reversed: value.is_reversed,
            name: value.name.as_str(),
            is_distinct: value.is_distinct,
            exprs: &value.exprs,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        error::Result, logical_expr::function::AccumulatorArgs,
        physical_expr::PhysicalSortExpr, physical_plan::expressions::col,
    };

    use super::{FFI_AccumulatorArgs, ForeignAccumulatorArgs};

    #[test]
    fn test_round_trip_accumulator_args() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let orig_args = AccumulatorArgs {
            return_field: Field::new("f", DataType::Float64, true).into(),
            schema: &schema,
            expr_fields: &[Field::new("a", DataType::Int32, true).into()],
            ignore_nulls: false,
            order_bys: &[PhysicalSortExpr::new_default(col("a", &schema)?)],
            is_reversed: false,
            name: "round_trip",
            is_distinct: true,
            exprs: &[col("a", &schema)?],
        };
        let orig_str = format!("{orig_args:?}");

        let ffi_args = FFI_AccumulatorArgs::try_from(orig_args)?;
        let foreign_args: ForeignAccumulatorArgs = ffi_args.try_into()?;
        let round_trip_args: AccumulatorArgs = (&foreign_args).into();

        let round_trip_str = format!("{round_trip_args:?}");

        // Since AccumulatorArgs doesn't implement Eq, simply compare
        // the debug strings.
        assert_eq!(orig_str, round_trip_str);

        Ok(())
    }
}
