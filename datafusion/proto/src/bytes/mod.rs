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
use crate::{from_proto::parse_expr, protobuf};
use datafusion::{
    common::{DataFusionError, Result},
    logical_plan::{Expr, FunctionRegistry},
};
use prost::{bytes::BytesMut, Message};

// Reexport Bytes which appears in the API
pub use prost::bytes::Bytes;

mod registry;

/// Encodes something (such as [`Expr`]) to/from a stream of
/// bytes.
///
/// ```
/// use datafusion::prelude::*;
/// use datafusion::logical_plan::Expr;
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

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;

    use datafusion::{
        arrow::array::ArrayRef, arrow::datatypes::DataType, logical_expr::Volatility,
        logical_plan::create_udf, physical_plan::functions::make_scalar_function,
        prelude::*,
    };

    #[test]
    #[should_panic(
        expected = "Error decoding expr as protobuf: failed to decode Protobuf message"
    )]
    fn bad_decode() {
        Expr::from_bytes(b"Leet").unwrap();
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
