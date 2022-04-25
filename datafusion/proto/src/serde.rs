use std::{collections::HashSet, sync::Arc};

use crate::{from_proto::parse_expr, protobuf};
use datafusion::{
    common::{DataFusionError, Result},
    logical_expr::{AggregateUDF, ScalarUDF},
    logical_plan::{Expr, FunctionRegistry},
};
use prost::{bytes::BytesMut, Message};

/// Encodes an [`Expr`] into a stream of bytes. See
/// [`deserialize_expr`] to convert a stream of bytes back to an Expr
///
/// Open Questions:
/// Should this be its own crate / API (aka datafusion-serde?) that can be implemented using proto?
///
///
/// Example:
///
/// ```
/// use datafusion::prelude::*;
/// use datafusion_proto::serde::Serializeable;
///
/// // Create a new `Expr` a < 32
/// let expr = col("a").lt(lit(5i32));
///
/// // Convert it to an opaque form
/// let bytes = expr.serialize().unwrap();
///
/// // Decode bytes from somewhere (over network, etc.
/// let decoded_expr = Expr::deserialize(&bytes).unwrap();
/// assert_eq!(expr, decoded_expr);
/// ```
pub trait Serializeable: Sized {
    /// Convert this to some serialized form (the internal format is not guaranteed)
    fn serialize(&self) -> Result<Vec<u8>>;

    /// convert the output of serialize back into an Expr, assuming
    /// Expr has no user defined functions
    fn deserialize(bytes: &[u8]) -> Result<Self> {
        Self::deserialize_with_registry(bytes, &NoRegistry {})
    }

    /// convert the output of serialize back into an Expr, given the
    /// specfified function registry for resolving UDFs
    fn deserialize_with_registry(
        bytes: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Self>;
}

impl Serializeable for Expr {
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buffer = BytesMut::new();
        let protobuf: protobuf::LogicalExprNode = self.try_into().map_err(|e| {
            DataFusionError::Plan(format!("Error encoding expr as protobuf: {}", e))
        })?;

        protobuf.encode(&mut buffer).map_err(|e| {
            DataFusionError::Plan(format!("Error encoding protobuf as bytes: {}", e))
        })?;

        // this copies the expr -- maybe it would be nice to avoid doing so
        Ok(buffer.to_vec())
    }

    fn deserialize_with_registry(
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

/// TODO Move this function registry into its own module
///
/// A default registry that does not have any user defined functions supplied
struct NoRegistry {}

impl FunctionRegistry for NoRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Function '{}'", name))
        )
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Aggregate Function '{}'", name))
        )
    }
}
