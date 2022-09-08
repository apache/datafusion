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

//! Serialization / Deserialization to Bytes
use crate::{
    from_proto::parse_expr,
    logical_plan::{AsLogicalPlan, LogicalExtensionCodec},
    protobuf,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Expr, LogicalPlan};
use prost::{
    bytes::{Bytes, BytesMut},
    Message,
};

// Reexport Bytes which appears in the API
use datafusion::logical_plan::FunctionRegistry;
use datafusion::prelude::SessionContext;
use datafusion_expr::logical_plan::Extension;

mod registry;

/// Encodes something (such as [`Expr`]) to/from a stream of
/// bytes.
///
/// ```
/// use datafusion_expr::{col, lit, Expr};
/// use datafusion_proto::bytes::Serializeable;
///
/// // Create a new `Expr` a < 32
/// let expr = col("a").lt(lit(5i32));
///
/// // Convert it to an opaque form
/// let bytes = expr.to_bytes().unwrap();
///
/// // Decode bytes from somewhere (over network, etc.
/// let decoded_expr = Expr::from_bytes(&bytes).unwrap();
/// assert_eq!(expr, decoded_expr);
/// ```
pub trait Serializeable: Sized {
    /// Convert `self` to an opaque byt stream
    fn to_bytes(&self) -> Result<Bytes>;

    /// Convert `bytes` (the output of [`to_bytes`] back into an
    /// object. This will error if the serialized bytes contain any
    /// user defined functions, in which case use
    /// [`from_bytes_with_registry`]
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_bytes_with_registry(bytes, &registry::NoRegistry {})
    }

    /// Convert `bytes` (the output of [`to_bytes`] back into an
    /// object resolving user defined functions with the specified
    /// `registry`
    fn from_bytes_with_registry(
        bytes: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Self>;
}

impl Serializeable for Expr {
    fn to_bytes(&self) -> Result<Bytes> {
        let mut buffer = BytesMut::new();
        let protobuf: protobuf::LogicalExprNode = self.try_into().map_err(|e| {
            DataFusionError::Plan(format!("Error encoding expr as protobuf: {}", e))
        })?;

        protobuf.encode(&mut buffer).map_err(|e| {
            DataFusionError::Plan(format!("Error encoding protobuf as bytes: {}", e))
        })?;

        Ok(buffer.into())
    }

    fn from_bytes_with_registry(
        bytes: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Self> {
        let protobuf = protobuf::LogicalExprNode::decode(bytes).map_err(|e| {
            DataFusionError::Plan(format!("Error decoding expr as protobuf: {}", e))
        })?;

        parse_expr(&protobuf, registry).map_err(|e| {
            DataFusionError::Plan(format!("Error parsing protobuf into Expr: {}", e))
        })
    }
}

/// Serialize a LogicalPlan as bytes
pub fn logical_plan_to_bytes(plan: &LogicalPlan) -> Result<Bytes> {
    let extension_codec = DefaultExtensionCodec {};
    logical_plan_to_bytes_with_extension_codec(plan, &extension_codec)
}

/// Serialize a LogicalPlan as json
#[cfg(feature = "json")]
pub fn logical_plan_to_json(plan: &LogicalPlan) -> Result<String> {
    let extension_codec = DefaultExtensionCodec {};
    let protobuf =
        protobuf::LogicalPlanNode::try_from_logical_plan(plan, &extension_codec)
            .map_err(|e| {
                DataFusionError::Plan(format!("Error serializing plan: {}", e))
            })?;
    serde_json::to_string(&protobuf)
        .map_err(|e| DataFusionError::Plan(format!("Error serializing plan: {}", e)))
}

/// Serialize a LogicalPlan as bytes, using the provided extension codec
pub fn logical_plan_to_bytes_with_extension_codec(
    plan: &LogicalPlan,
    extension_codec: &dyn LogicalExtensionCodec,
) -> Result<Bytes> {
    let protobuf =
        protobuf::LogicalPlanNode::try_from_logical_plan(plan, extension_codec)?;
    let mut buffer = BytesMut::new();
    protobuf.encode(&mut buffer).map_err(|e| {
        DataFusionError::Plan(format!("Error encoding protobuf as bytes: {}", e))
    })?;
    Ok(buffer.into())
}

/// Deserialize a LogicalPlan from json
#[cfg(feature = "json")]
pub fn logical_plan_from_json(json: &str, ctx: &SessionContext) -> Result<LogicalPlan> {
    let back: protobuf::LogicalPlanNode = serde_json::from_str(json)
        .map_err(|e| DataFusionError::Plan(format!("Error serializing plan: {}", e)))?;
    let extension_codec = DefaultExtensionCodec {};
    back.try_into_logical_plan(ctx, &extension_codec)
}

/// Deserialize a LogicalPlan from bytes
pub fn logical_plan_from_bytes(
    bytes: &[u8],
    ctx: &SessionContext,
) -> Result<LogicalPlan> {
    let extension_codec = DefaultExtensionCodec {};
    logical_plan_from_bytes_with_extension_codec(bytes, ctx, &extension_codec)
}

/// Deserialize a LogicalPlan from bytes
pub fn logical_plan_from_bytes_with_extension_codec(
    bytes: &[u8],
    ctx: &SessionContext,
    extension_codec: &dyn LogicalExtensionCodec,
) -> Result<LogicalPlan> {
    let protobuf = protobuf::LogicalPlanNode::decode(bytes).map_err(|e| {
        DataFusionError::Plan(format!("Error decoding expr as protobuf: {}", e))
    })?;
    protobuf.try_into_logical_plan(ctx, extension_codec)
}

#[derive(Debug)]
struct DefaultExtensionCodec {}

impl LogicalExtensionCodec for DefaultExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> Result<Extension> {
        Err(DataFusionError::NotImplemented(
            "No extension codec provided".to_string(),
        ))
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "No extension codec provided".to_string(),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::{array::ArrayRef, datatypes::DataType};
    use datafusion::prelude::SessionContext;
    use datafusion::{
        logical_plan::create_udf, physical_plan::functions::make_scalar_function,
    };
    use datafusion_expr::{lit, Volatility};
    use std::sync::Arc;

    #[test]
    #[should_panic(
        expected = "Error decoding expr as protobuf: failed to decode Protobuf message"
    )]
    fn bad_decode() {
        Expr::from_bytes(b"Leet").unwrap();
    }

    #[test]
    #[cfg(feature = "json")]
    fn plan_to_json() {
        use datafusion_common::DFSchema;
        use datafusion_expr::logical_plan::EmptyRelation;

        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let actual = logical_plan_to_json(&plan).unwrap();
        let expected = r#"{"emptyRelation":{}}"#.to_string();
        assert_eq!(actual, expected);
    }

    #[test]
    #[cfg(feature = "json")]
    fn json_to_plan() {
        let input = r#"{"emptyRelation":{}}"#.to_string();
        let ctx = SessionContext::new();
        let actual = logical_plan_from_json(&input, &ctx).unwrap();
        let result = matches!(actual, LogicalPlan::EmptyRelation(_));
        assert!(result, "Should parse empty relation");
    }

    #[test]
    fn udf_roundtrip_with_registry() {
        let ctx = context_with_udf();

        let expr = ctx
            .udf("dummy")
            .expect("could not find udf")
            .call(vec![lit("")]);

        let bytes = expr.to_bytes().unwrap();
        let deserialized_expr = Expr::from_bytes_with_registry(&bytes, &ctx).unwrap();

        assert_eq!(expr, deserialized_expr);
    }

    #[test]
    #[should_panic(
        expected = "No function registry provided to deserialize, so can not deserialize User Defined Function 'dummy'"
    )]
    fn udf_roundtrip_without_registry() {
        let ctx = context_with_udf();

        let expr = ctx
            .udf("dummy")
            .expect("could not find udf")
            .call(vec![lit("")]);

        let bytes = expr.to_bytes().unwrap();
        // should explode
        Expr::from_bytes(&bytes).unwrap();
    }

    /// return a `SessionContext` with a `dummy` function registered as a UDF
    fn context_with_udf() -> SessionContext {
        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        let scalar_fn = make_scalar_function(fn_impl);

        let udf = create_udf(
            "dummy",
            vec![DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            scalar_fn,
        );

        let mut ctx = SessionContext::new();
        ctx.register_udf(udf);

        ctx
    }
}
