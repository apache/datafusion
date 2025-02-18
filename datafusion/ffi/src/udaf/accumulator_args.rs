use std::sync::Arc;

use abi_stable::{
    std_types::{RString, RVec},
    StableAbi,
};
use arrow::{datatypes::Schema, ffi::FFI_ArrowSchema};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::function::AccumulatorArgs,
    prelude::SessionContext,
};
use datafusion_proto::{
    physical_plan::{
        from_proto::{parse_physical_exprs, parse_physical_sort_exprs},
        to_proto::{serialize_physical_exprs, serialize_physical_sort_exprs},
        DefaultPhysicalExtensionCodec,
    },
    protobuf::PhysicalAggregateExprNode,
};
use prost::Message;

use crate::{arrow_wrappers::WrappedSchema, rresult_return};

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_AccumulatorArgs {
    return_type: WrappedSchema,
    schema: WrappedSchema,
    is_reversed: bool,
    name: RString,
    physical_expr_def: RVec<u8>,
}

impl FFI_AccumulatorArgs {
    pub fn to_accumulator_args(&self) -> Result<AccumulatorArgs> {
        let proto_def =
            PhysicalAggregateExprNode::decode(self.physical_expr_def.as_ref())
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let return_type = &(&self.return_type.0).try_into()?;
        let schema = &Arc::new(Schema::try_from(&self.schema.0)?);

        let default_ctx = SessionContext::new();
        let codex = DefaultPhysicalExtensionCodec {};

        // let proto_ordering_req =
        //     rresult_return!(PhysicalSortExprNodeCollection::decode(ordering_req.as_ref()));
        let ordering_req = &parse_physical_sort_exprs(
            &proto_def.ordering_req,
            &default_ctx,
            &schema,
            &codex,
        )?;

        let exprs = &rresult_return!(parse_physical_exprs(
            &proto_def.expr,
            &default_ctx,
            &schema,
            &codex
        ));

        Ok(AccumulatorArgs {
            return_type,
            schema,
            ignore_nulls: proto_def.ignore_nulls,
            ordering_req,
            is_reversed: self.is_reversed,
            name: self.name.as_str(),
            is_distinct: proto_def.distinct,
            exprs,
        })
    }
}

impl<'a> TryFrom<AccumulatorArgs<'a>> for FFI_AccumulatorArgs {
    type Error = DataFusionError;

    fn try_from(args: AccumulatorArgs) -> std::result::Result<Self, Self::Error> {
        let return_type = WrappedSchema(FFI_ArrowSchema::try_from(args.return_type)?);
        let schema = WrappedSchema(FFI_ArrowSchema::try_from(args.schema)?);

        let codec = DefaultPhysicalExtensionCodec {};
        let ordering_req =
            serialize_physical_sort_exprs(args.ordering_req.to_owned(), &codec)?;

        let expr = serialize_physical_exprs(args.exprs, &codec)?;

        let physical_expr_def = PhysicalAggregateExprNode {
            expr,
            ordering_req,
            distinct: args.is_distinct,
            ignore_nulls: args.ignore_nulls,
            fun_definition: None,
            aggregate_function: None,
        };
        let physical_expr_def = physical_expr_def.encode_to_vec().into();

        Ok(Self {
            return_type,
            schema,
            is_reversed: args.is_reversed,
            name: args.name.into(),
            physical_expr_def,
        })
    }
}
