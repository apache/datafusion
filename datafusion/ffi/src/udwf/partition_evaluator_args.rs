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

use std::{collections::HashMap, sync::Arc};

use crate::arrow_wrappers::WrappedSchema;
use abi_stable::{StableAbi, std_types::RVec};
use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ffi::FFI_ArrowSchema,
};
use arrow_schema::FieldRef;
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::function::PartitionEvaluatorArgs,
    physical_plan::{PhysicalExpr, expressions::Column},
    prelude::SessionContext,
};
use datafusion_common::ffi_datafusion_err;
use datafusion_proto::{
    physical_plan::{
        DefaultPhysicalExtensionCodec, from_proto::parse_physical_expr,
        to_proto::serialize_physical_exprs,
    },
    protobuf::PhysicalExprNode,
};
use prost::Message;

/// A stable struct for sharing [`PartitionEvaluatorArgs`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`PartitionEvaluatorArgs`].
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PartitionEvaluatorArgs {
    input_exprs: RVec<RVec<u8>>,
    input_fields: RVec<WrappedSchema>,
    is_reversed: bool,
    ignore_nulls: bool,
    schema: WrappedSchema,
}

impl TryFrom<PartitionEvaluatorArgs<'_>> for FFI_PartitionEvaluatorArgs {
    type Error = DataFusionError;
    fn try_from(args: PartitionEvaluatorArgs) -> Result<Self, DataFusionError> {
        // This is a bit of a hack. Since PartitionEvaluatorArgs does not carry a schema
        // around, and instead passes the data types directly we are unable to decode the
        // protobuf PhysicalExpr correctly. In evaluating the code the only place these
        // appear to be really used are the Column data types. So here we will find all
        // of the required columns and create a schema that has empty fields except for
        // the ones we require. Ideally we would enhance PartitionEvaluatorArgs to just
        // pass along the schema, but that is a larger breaking change.
        let required_columns: HashMap<usize, (&str, &DataType)> = args
            .input_exprs()
            .iter()
            .zip(args.input_fields())
            .filter_map(|(expr, field)| {
                expr.as_any()
                    .downcast_ref::<Column>()
                    .map(|column| (column.index(), (column.name(), field.data_type())))
            })
            .collect();

        let max_column = required_columns.keys().max();
        let fields: Vec<_> = max_column
            .map(|max_column| {
                (0..(max_column + 1))
                    .map(|idx| match required_columns.get(&idx) {
                        Some((name, data_type)) => {
                            Field::new(*name, (*data_type).clone(), true)
                        }
                        None => Field::new(
                            format!("ffi_partition_evaluator_col_{idx}"),
                            DataType::Null,
                            true,
                        ),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let schema = Arc::new(Schema::new(fields));

        let codec = DefaultPhysicalExtensionCodec {};
        let input_exprs = serialize_physical_exprs(args.input_exprs(), &codec)?
            .into_iter()
            .map(|expr_node| expr_node.encode_to_vec().into())
            .collect();

        let input_fields = args
            .input_fields()
            .iter()
            .map(|input_type| FFI_ArrowSchema::try_from(input_type).map(WrappedSchema))
            .collect::<Result<Vec<_>, ArrowError>>()?
            .into();

        let schema: WrappedSchema = schema.into();

        Ok(Self {
            input_exprs,
            input_fields,
            schema,
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
        let default_ctx = SessionContext::new();
        let codec = DefaultPhysicalExtensionCodec {};

        let schema: SchemaRef = value.schema.into();

        let input_exprs = value
            .input_exprs
            .into_iter()
            .map(|input_expr_bytes| PhysicalExprNode::decode(input_expr_bytes.as_ref()))
            .collect::<std::result::Result<Vec<_>, prost::DecodeError>>()
            .map_err(|e| ffi_datafusion_err!("Failed to decode PhysicalExprNode: {e}"))?
            .iter()
            .map(|expr_node| {
                parse_physical_expr(expr_node, &default_ctx.task_ctx(), &schema, &codec)
            })
            .collect::<Result<Vec<_>>>()?;

        let input_fields = input_exprs
            .iter()
            .map(|expr| expr.return_field(&schema))
            .collect::<Result<Vec<_>>>()?;

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
