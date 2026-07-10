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

//! Declaration of built-in (scalar) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::PhysicalExpr;
use crate::expressions::Literal;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Volatility, expr_vec_fmt,
};

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: Arc<ScalarUDF>,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_field", &self.return_field)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: Arc<ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_field: FieldRef,
        config_options: Arc<ConfigOptions>,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_field,
            config_options,
        }
    }

    /// Create a new Scalar function
    pub fn try_new(
        fun: Arc<ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let name = fun.name().to_string();
        let arg_fields = args
            .iter()
            .map(|e| e.return_field(schema))
            .collect::<Result<Vec<_>>>()?;

        // verify that input data types is consistent with function's `TypeSignature`
        fields_with_udf(&arg_fields, fun.as_ref())?;

        let arguments = args
            .iter()
            .map(|e| e.downcast_ref::<Literal>().map(|literal| literal.value()))
            .collect::<Vec<_>>();
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &arguments,
        };
        let return_field = fun.return_field_from_args(ret_args)?;
        Ok(Self {
            fun,
            name,
            args,
            return_field,
            config_options,
        })
    }

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarUDF {
        &self.fun
    }

    /// The name for this expression
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Input arguments
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.return_field = self
            .return_field
            .as_ref()
            .clone()
            .with_nullable(nullable)
            .into();
        self
    }

    pub fn nullable(&self) -> bool {
        self.return_field.is_nullable()
    }

    pub fn config_options(&self) -> &ConfigOptions {
        &self.config_options
    }

    /// Given an arbitrary PhysicalExpr attempt to downcast it to a ScalarFunctionExpr
    /// and verify that its inner function is of type T.
    /// If the downcast fails, or the function is not of type T, returns `None`.
    /// Otherwise returns `Some(ScalarFunctionExpr)`.
    pub fn try_downcast_func<T>(expr: &dyn PhysicalExpr) -> Option<&ScalarFunctionExpr>
    where
        T: ScalarUDFImpl,
    {
        match expr.downcast_ref::<ScalarFunctionExpr>() {
            Some(scalar_expr) if scalar_expr.fun().inner().is::<T>() => Some(scalar_expr),
            _ => None,
        }
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl PartialEq for ScalarFunctionExpr {
    fn eq(&self, o: &Self) -> bool {
        if std::ptr::eq(self, o) {
            // The equality implementation is somewhat expensive, so let's short-circuit when possible.
            return true;
        }
        let Self {
            fun,
            name,
            args,
            return_field,
            config_options,
        } = self;
        fun.eq(&o.fun)
            && name.eq(&o.name)
            && args.eq(&o.args)
            && return_field.eq(&o.return_field)
            && (Arc::ptr_eq(config_options, &o.config_options)
                || sorted_config_entries(config_options)
                    == sorted_config_entries(&o.config_options))
    }
}
impl Eq for ScalarFunctionExpr {}
impl Hash for ScalarFunctionExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            fun,
            name,
            args,
            return_field,
            config_options: _, // expensive to hash, and often equal
        } = self;
        fun.hash(state);
        name.hash(state);
        args.hash(state);
        return_field.hash(state);
    }
}

fn sorted_config_entries(config_options: &ConfigOptions) -> Vec<ConfigEntry> {
    let mut entries = config_options.entries();
    entries.sort_by(|l, r| l.key.cmp(&r.key));
    entries
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let args = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        let arg_fields = self
            .args
            .iter()
            .map(|e| e.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;

        let input_empty = args.is_empty();
        let input_all_scalar = args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));

        // evaluate the function
        let output = self.fun.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: batch.num_rows(),
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        })?;

        if let ColumnarValue::Array(array) = &output
            && array.len() != batch.num_rows()
        {
            // If the arguments are a non-empty slice of scalar values, we can assume that
            // returning a one-element array is equivalent to returning a scalar.
            let preserve_scalar = array.len() == 1 && !input_empty && input_all_scalar;
            return if preserve_scalar {
                ScalarValue::try_from_array(array, 0).map(ColumnarValue::Scalar)
            } else {
                internal_err!(
                    "UDF {} returned a different number of rows than expected. Expected: {}, Got: {}",
                    self.name,
                    batch.num_rows(),
                    array.len()
                )
            };
        }
        Ok(output)
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::new(
            &self.name,
            Arc::clone(&self.fun),
            children,
            Arc::clone(&self.return_field),
            Arc::clone(&self.config_options),
        )))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.fun.evaluate_bounds(children)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.fun.propagate_constraints(interval, children)
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let sort_properties = self.fun.output_ordering(children)?;
        let preserves_lex_ordering = self.fun.preserves_lex_ordering(children)?;
        let children_range = children
            .iter()
            .map(|props| &props.range)
            .collect::<Vec<_>>();
        let range = self.fun().evaluate_bounds(&children_range)?;

        Ok(ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering,
        })
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, expr) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            expr.fmt_sql(f)?;
        }
        write!(f, ")")
    }

    fn is_volatile_node(&self) -> bool {
        self.fun.signature().volatility == Volatility::Volatile
    }

    fn placement(&self) -> ExpressionPlacement {
        let arg_placements: Vec<_> =
            self.args.iter().map(|arg| arg.placement()).collect();
        self.fun.placement(&arg_placements)
    }
}

/// Typed protobuf serialization for [`ScalarFunctionExpr`].
///
/// Most built-in expressions serialize themselves through the crate-wide
/// [`PhysicalExpr::try_to_proto`] hook, whose encode/decode contexts live in
/// `physical-expr-common`. `ScalarFunctionExpr` cannot: its ser/de needs
/// session-level objects — the embedded [`ScalarUDF`] round-trips through
/// `datafusion-proto`'s extension codec + function registry, and
/// reconstruction needs the session [`ConfigOptions`]. Those types are
/// defined *above* `physical-expr-common` in the crate graph
/// (`datafusion-expr` depends on `physical-expr-common`, not the other way
/// round), so they cannot appear on the crate-wide contexts without a
/// dependency cycle.
///
/// The expression still declares its own ser/de here, but against the
/// **fully-typed** [`ScalarUdfProtoEncoder`] / [`ScalarUdfProtoDecoder`]
/// bridge traits — implemented in `datafusion-proto`, which can name
/// [`ScalarUDF`] and the codec directly. Dispatch stays a downcast match in
/// `datafusion-proto` (symmetric with the decode side), and no type erasure
/// (`dyn Any`) is involved anywhere. The trade versus routing through the
/// hook is one surviving downcast arm in `to_proto.rs` in exchange for a
/// fully-typed, erasure-free surface. See #22430 / the epic in #22418.
#[cfg(feature = "proto")]
mod proto {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::internal_datafusion_err;
    use datafusion_proto_models::protobuf;

    /// Encode-side serialization bridge for [`ScalarFunctionExpr`].
    ///
    /// Implemented by `datafusion-proto`, where the extension codec and
    /// [`ScalarUDF`] are both nameable. Keeps this crate free of a
    /// `datafusion-proto` dependency while staying fully typed.
    pub trait ScalarUdfProtoEncoder {
        /// Encode a child expression to a protobuf node (dedup-aware, via the
        /// same converter used for the rest of the plan).
        fn encode_child(
            &self,
            expr: &Arc<dyn PhysicalExpr>,
        ) -> Result<protobuf::PhysicalExprNode>;

        /// Encode a [`ScalarUDF`]'s custom payload via the extension codec.
        /// Returns `None` when the codec writes no bytes (the function is then
        /// resolved by name alone on decode).
        fn encode_udf(&self, udf: &ScalarUDF) -> Result<Option<Vec<u8>>>;
    }

    /// Decode-side counterpart to [`ScalarUdfProtoEncoder`].
    pub trait ScalarUdfProtoDecoder {
        /// Decode a child expression node, recursing through the converter.
        fn decode_child(
            &self,
            node: &protobuf::PhysicalExprNode,
        ) -> Result<Arc<dyn PhysicalExpr>>;

        /// Resolve a [`ScalarUDF`] by name, using the optional custom payload.
        /// Implementors reproduce the registry-then-codec lookup order.
        fn decode_udf(
            &self,
            name: &str,
            fun_definition: Option<&[u8]>,
        ) -> Result<Arc<ScalarUDF>>;

        /// Session configuration used to reconstruct the expression.
        fn config_options(&self) -> Arc<ConfigOptions>;
    }

    impl ScalarFunctionExpr {
        /// Serialize this expression to a [`protobuf::PhysicalExprNode`] via a
        /// typed encoder.
        ///
        /// Called from `datafusion-proto`'s downcast dispatch rather than the
        /// crate-wide `try_to_proto` hook — see the [module docs](self) for
        /// why. The wire shape is identical to the pre-existing inline arm.
        pub fn try_to_proto(
            &self,
            encoder: &dyn ScalarUdfProtoEncoder,
        ) -> Result<protobuf::PhysicalExprNode> {
            let args = self
                .args()
                .iter()
                .map(|arg| encoder.encode_child(arg))
                .collect::<Result<Vec<_>>>()?;
            Ok(protobuf::PhysicalExprNode {
                // `ScalarFunctionExpr` never overrides `expression_id`, so this
                // is always `None` (matching the old inline arm).
                expr_id: None,
                expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
                    protobuf::PhysicalScalarUdfNode {
                        name: self.name().to_string(),
                        args,
                        fun_definition: encoder.encode_udf(self.fun())?,
                        return_type: Some(self.return_type().try_into()?),
                        nullable: self.nullable(),
                        return_field_name: self
                            .return_field(&Schema::empty())?
                            .name()
                            .to_string(),
                    },
                )),
            })
        }

        /// Reconstruct a [`ScalarFunctionExpr`] from its protobuf
        /// representation via a typed decoder. The inverse of
        /// [`Self::try_to_proto`].
        pub fn try_from_proto(
            node: &protobuf::PhysicalScalarUdfNode,
            decoder: &dyn ScalarUdfProtoDecoder,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            let udf = decoder.decode_udf(&node.name, node.fun_definition.as_deref())?;
            let args = node
                .args
                .iter()
                .map(|arg| decoder.decode_child(arg))
                .collect::<Result<Vec<_>>>()?;
            let return_type = node.return_type.as_ref().ok_or_else(|| {
                internal_datafusion_err!(
                    "ScalarFunctionExpr is missing required field 'return_type'"
                )
            })?;
            Ok(Arc::new(
                ScalarFunctionExpr::new(
                    node.name.as_str(),
                    udf,
                    args,
                    Field::new(&node.return_field_name, return_type.try_into()?, true)
                        .into(),
                    decoder.config_options(),
                )
                .with_nullable(node.nullable),
            ))
        }
    }
}

#[cfg(feature = "proto")]
pub use proto::{ScalarUdfProtoDecoder, ScalarUdfProtoEncoder};

/// Direct tests for the typed `try_to_proto` / `try_from_proto` bridge.
///
/// These drive the inherent methods against tiny mock encoder/decoder
/// implementations, so they cover the marshalling and error paths without a
/// running session. End-to-end round-trips (through `datafusion-proto`'s real
/// codec) are covered by `datafusion-proto`'s `proto_integration` suite.
#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::proto::{ScalarUdfProtoDecoder, ScalarUdfProtoEncoder};
    use super::*;
    use crate::expressions::Column;
    use arrow::datatypes::Field;
    use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
    use datafusion_proto_models::protobuf;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestUdf {
        signature: Signature,
    }

    impl TestUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for TestUdf {
        fn name(&self) -> &str {
            "test_udf"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))))
        }
    }

    fn test_udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(TestUdf::new()))
    }

    fn sample_expr() -> ScalarFunctionExpr {
        let args: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1))];
        ScalarFunctionExpr::new(
            "test_udf",
            test_udf(),
            args,
            Field::new("out", DataType::Int32, true).into(),
            Arc::new(ConfigOptions::new()),
        )
        .with_nullable(true)
    }

    /// Encoder that yields a fixed child node and a configurable UDF payload.
    struct MockEncoder {
        udf_payload: Option<Vec<u8>>,
        fail_child: bool,
    }

    impl ScalarUdfProtoEncoder for MockEncoder {
        fn encode_child(
            &self,
            _expr: &Arc<dyn PhysicalExpr>,
        ) -> Result<protobuf::PhysicalExprNode> {
            if self.fail_child {
                return internal_err!("child encode failed");
            }
            Ok(protobuf::PhysicalExprNode {
                expr_id: None,
                expr_type: None,
            })
        }

        fn encode_udf(&self, _udf: &ScalarUDF) -> Result<Option<Vec<u8>>> {
            Ok(self.udf_payload.clone())
        }
    }

    /// Decoder that resolves a fixed UDF, asserting the payload it was handed.
    struct MockDecoder {
        expected_payload: Option<Vec<u8>>,
        fail_child: bool,
    }

    impl ScalarUdfProtoDecoder for MockDecoder {
        fn decode_child(
            &self,
            _node: &protobuf::PhysicalExprNode,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            if self.fail_child {
                return internal_err!("child decode failed");
            }
            Ok(Arc::new(Column::new("a", 0)))
        }

        fn decode_udf(
            &self,
            _name: &str,
            fun_definition: Option<&[u8]>,
        ) -> Result<Arc<ScalarUDF>> {
            if fun_definition.map(|b| b.to_vec()) != self.expected_payload {
                return internal_err!("decoder received unexpected fun_definition");
            }
            Ok(test_udf())
        }

        fn config_options(&self) -> Arc<ConfigOptions> {
            Arc::new(ConfigOptions::new())
        }
    }

    fn unwrap_udf_node(
        node: protobuf::PhysicalExprNode,
    ) -> protobuf::PhysicalScalarUdfNode {
        match node.expr_type {
            Some(protobuf::physical_expr_node::ExprType::ScalarUdf(n)) => *Box::new(n),
            other => panic!("expected ScalarUdf node, got {other:?}"),
        }
    }

    #[test]
    fn try_to_proto_encodes_fields() {
        let node = sample_expr()
            .try_to_proto(&MockEncoder {
                udf_payload: None,
                fail_child: false,
            })
            .unwrap();
        assert_eq!(node.expr_id, None);
        let udf = unwrap_udf_node(node);
        assert_eq!(udf.name, "test_udf");
        assert_eq!(udf.args.len(), 2);
        assert_eq!(udf.fun_definition, None);
        assert!(udf.nullable);
        assert_eq!(udf.return_field_name, "out");
        assert!(udf.return_type.is_some());
    }

    #[test]
    fn try_to_proto_passes_codec_payload() {
        let node = sample_expr()
            .try_to_proto(&MockEncoder {
                udf_payload: Some(vec![1, 2, 3]),
                fail_child: false,
            })
            .unwrap();
        assert_eq!(unwrap_udf_node(node).fun_definition, Some(vec![1, 2, 3]));
    }

    #[test]
    fn try_to_proto_propagates_child_error() {
        let err = sample_expr()
            .try_to_proto(&MockEncoder {
                udf_payload: None,
                fail_child: true,
            })
            .unwrap_err();
        assert!(err.to_string().contains("child encode failed"));
    }

    #[test]
    fn try_from_proto_roundtrip() {
        let node = sample_expr()
            .try_to_proto(&MockEncoder {
                udf_payload: Some(vec![9]),
                fail_child: false,
            })
            .unwrap();
        let udf_node = unwrap_udf_node(node);
        let decoded = ScalarFunctionExpr::try_from_proto(
            &udf_node,
            &MockDecoder {
                expected_payload: Some(vec![9]),
                fail_child: false,
            },
        )
        .unwrap();
        let decoded = decoded
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("decoded to ScalarFunctionExpr");
        assert_eq!(decoded.name(), "test_udf");
        assert_eq!(decoded.args().len(), 2);
        assert!(decoded.nullable());
        assert_eq!(
            decoded.return_field(&Schema::empty()).unwrap().name(),
            "out"
        );
    }

    #[test]
    fn try_from_proto_errors_on_missing_return_type() {
        let mut udf_node = unwrap_udf_node(
            sample_expr()
                .try_to_proto(&MockEncoder {
                    udf_payload: None,
                    fail_child: false,
                })
                .unwrap(),
        );
        udf_node.return_type = None;
        let err = ScalarFunctionExpr::try_from_proto(
            &udf_node,
            &MockDecoder {
                expected_payload: None,
                fail_child: false,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("return_type"));
    }

    #[test]
    fn try_from_proto_propagates_child_error() {
        let udf_node = unwrap_udf_node(
            sample_expr()
                .try_to_proto(&MockEncoder {
                    udf_payload: None,
                    fail_child: false,
                })
                .unwrap(),
        );
        let err = ScalarFunctionExpr::try_from_proto(
            &udf_node,
            &MockDecoder {
                expected_payload: None,
                fail_child: true,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("child decode failed"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::datatypes::Field;
    use datafusion_expr::{ScalarUDFImpl, Signature};
    use datafusion_physical_expr_common::physical_expr::is_volatile;

    /// Test helper to create a mock UDF with a specific volatility
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockScalarUDF {
        signature: Signature,
    }

    impl ScalarUDFImpl for MockScalarUDF {
        fn name(&self) -> &str {
            "mock_function"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))))
        }
    }

    #[test]
    fn test_scalar_function_volatile_node() {
        // Create a volatile UDF
        let volatile_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32],
                Volatility::Volatile,
            ),
        }));

        // Create a non-volatile UDF
        let stable_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        }));

        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let args = vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>];
        let config_options = Arc::new(ConfigOptions::new());

        // Test volatile function
        let volatile_expr = ScalarFunctionExpr::try_new(
            volatile_udf,
            args.clone(),
            &schema,
            Arc::clone(&config_options),
        )
        .unwrap();

        assert!(volatile_expr.is_volatile_node());
        let volatile_arc: Arc<dyn PhysicalExpr> = Arc::new(volatile_expr);
        assert!(is_volatile(&volatile_arc));

        // Test non-volatile function
        let stable_expr =
            ScalarFunctionExpr::try_new(stable_udf, args, &schema, config_options)
                .unwrap();

        assert!(!stable_expr.is_volatile_node());
        let stable_arc: Arc<dyn PhysicalExpr> = Arc::new(stable_expr);
        assert!(!is_volatile(&stable_arc));
    }
}
