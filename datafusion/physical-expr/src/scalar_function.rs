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

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
                protobuf::PhysicalScalarUdfNode {
                    name: self.name.clone(),
                    args: ctx.encode_children_expressions(&self.args)?,
                    fun_definition: ctx.encode_udf(self.fun.as_ref())?,
                    return_type: Some(self.return_type().try_into()?),
                    nullable: self.nullable(),
                    return_field_name: self.return_field.name().to_string(),
                },
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl ScalarFunctionExpr {
    /// Reconstruct a [`ScalarFunctionExpr`] from its protobuf representation.
    ///
    /// Takes the whole [`PhysicalExprNode`] so the decode signature matches
    /// other migrated expressions and can inspect outer-node metadata if
    /// needed in the future. The UDF itself is resolved through
    /// [`PhysicalExprDecodeCtx::decode_udf`]: the extension codec's opaque
    /// `fun_definition` payload wins, falling back to the session's function
    /// registry by name.
    ///
    /// [`PhysicalExprNode`]: datafusion_proto_models::protobuf::PhysicalExprNode
    /// [`PhysicalExprDecodeCtx::decode_udf`]: datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx::decode_udf
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use arrow::datatypes::Field;
        use datafusion_common::internal_datafusion_err;
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_physical_expr_common::physical_expr::proto_decode::require_proto_field;
        use datafusion_proto_models::protobuf;

        let scalar_udf = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::ScalarUdf,
            "ScalarFunctionExpr",
        );

        let udf = ctx
            .decode_udf(&scalar_udf.name, scalar_udf.fun_definition.as_deref())?
            .downcast::<ScalarUDF>()
            .map_err(|_| {
                internal_datafusion_err!(
                    "decode_udf returned a non-ScalarUDF value for '{}'",
                    scalar_udf.name
                )
            })?;

        let args = ctx.decode_children_expressions(&scalar_udf.args)?;
        let return_type = require_proto_field(
            scalar_udf.return_type.as_ref(),
            "ScalarFunctionExpr",
            "return_type",
        )?;
        let return_field =
            Field::new(&scalar_udf.return_field_name, return_type.try_into()?, true)
                .into();

        Ok(Arc::new(
            ScalarFunctionExpr::new(
                &scalar_udf.name,
                udf,
                args,
                return_field,
                ctx.config_options(),
            )
            .with_nullable(scalar_udf.nullable),
        ))
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

/// Tests for the `try_to_proto` / `try_from_proto` hooks.
#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::*;
    use crate::expressions::Column;
    use crate::proto_test_util::{StubDecoder, StubEncoder, column_node};
    use arrow::datatypes::Field;
    use datafusion_expr::Signature;
    use datafusion_physical_expr_common::physical_expr::proto_decode::{
        PhysicalExprDecode, PhysicalExprDecodeCtx,
    };
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_proto_models::datafusion_common::ArrowType;
    use datafusion_proto_models::protobuf::{
        PhysicalExprNode, PhysicalScalarUdfNode, physical_expr_node,
    };

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestUdf {
        signature: Signature,
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
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))))
        }
    }

    fn test_udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(TestUdf {
            signature: Signature::uniform(
                1,
                vec![DataType::Int32],
                Volatility::Immutable,
            ),
        }))
    }

    /// A `ScalarFunctionExpr` calling [`test_udf`] over one `Int32` column.
    fn proto_scalar_fn_fixture() -> ScalarFunctionExpr {
        ScalarFunctionExpr::new(
            "test_udf",
            test_udf(),
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
            Field::new("test_udf", DataType::Int32, true).into(),
            Arc::new(ConfigOptions::default()),
        )
    }

    fn proto_int32_arrow_type() -> ArrowType {
        (&DataType::Int32).try_into().unwrap()
    }

    /// Build a `ScalarUdf` proto node with the given fields.
    fn proto_scalar_udf_node(
        args: Vec<PhysicalExprNode>,
        return_type: Option<ArrowType>,
        nullable: bool,
    ) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::ScalarUdf(
                PhysicalScalarUdfNode {
                    name: "test_udf".to_string(),
                    args,
                    fun_definition: None,
                    return_type,
                    nullable,
                    return_field_name: "test_udf".to_string(),
                },
            )),
        }
    }

    /// Decoder stub that resolves every UDF name to [`test_udf`], delegating
    /// child decoding to an inner [`StubDecoder`].
    struct UdfStubDecoder {
        inner: StubDecoder,
    }

    impl PhysicalExprDecode for UdfStubDecoder {
        fn decode(
            &self,
            node: &PhysicalExprNode,
            schema: &Schema,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            self.inner.decode(node, schema)
        }

        fn decode_udf(
            &self,
            _name: &str,
            _fun_definition: Option<&[u8]>,
        ) -> Result<Arc<dyn std::any::Any + Send + Sync>> {
            Ok(test_udf() as _)
        }
    }

    /// Decoder stub whose `decode_udf` returns a value that is not a
    /// `ScalarUDF`, to exercise the downcast reject path.
    struct NonUdfStubDecoder;

    impl PhysicalExprDecode for NonUdfStubDecoder {
        fn decode(
            &self,
            _node: &PhysicalExprNode,
            _schema: &Schema,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            unreachable!("child decoding must not be reached")
        }

        fn decode_udf(
            &self,
            _name: &str,
            _fun_definition: Option<&[u8]>,
        ) -> Result<Arc<dyn std::any::Any + Send + Sync>> {
            Ok(Arc::new("not a udf") as _)
        }
    }

    #[test]
    fn try_to_proto_encodes_scalar_function_expr() {
        let expr = proto_scalar_fn_fixture();
        let encoder = StubEncoder::ok();
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = expr
            .try_to_proto(&ctx)
            .unwrap()
            .expect("ScalarFunctionExpr must serialize itself");

        assert_eq!(node.expr_id, None);
        let Some(physical_expr_node::ExprType::ScalarUdf(udf_node)) = node.expr_type
        else {
            panic!("expected ScalarUdf node");
        };
        assert_eq!(udf_node.name, "test_udf");
        assert_eq!(udf_node.args, vec![column_node("child")]);
        assert_eq!(udf_node.fun_definition, None);
        assert_eq!(udf_node.return_type, Some(proto_int32_arrow_type()));
        assert!(udf_node.nullable);
        assert_eq!(udf_node.return_field_name, "test_udf");
    }

    #[test]
    fn try_to_proto_propagates_child_encode_error() {
        let expr = proto_scalar_fn_fixture();
        let encoder = StubEncoder::failing_on(1);
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let err = expr.try_to_proto(&ctx).unwrap_err();
        assert!(err.to_string().contains("stub encode failure"), "{err}");
    }

    #[test]
    fn try_from_proto_decodes_scalar_function_expr() {
        let node = proto_scalar_udf_node(
            vec![column_node("a")],
            Some(proto_int32_arrow_type()),
            false,
        );
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let decoder = UdfStubDecoder {
            inner: StubDecoder::ok(),
        };
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = ScalarFunctionExpr::try_from_proto(&node, &ctx).unwrap();
        let expr = decoded
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("expected a ScalarFunctionExpr");

        assert_eq!(expr.name(), "test_udf");
        assert_eq!(expr.fun().name(), "test_udf");
        assert_eq!(expr.args().len(), 1);
        assert_eq!(expr.return_type(), &DataType::Int32);
        assert!(!expr.nullable());
    }

    #[test]
    fn try_from_proto_rejects_non_scalar_udf_node() {
        let node = column_node("a");
        let schema = Schema::empty();
        let decoder = NonUdfStubDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = ScalarFunctionExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("PhysicalExprNode is not a ScalarFunctionExpr"),
            "{err}"
        );
    }

    #[test]
    fn try_from_proto_rejects_missing_return_type() {
        let node = proto_scalar_udf_node(vec![column_node("a")], None, true);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let decoder = UdfStubDecoder {
            inner: StubDecoder::ok(),
        };
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = ScalarFunctionExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing required field 'return_type'"),
            "{err}"
        );
    }

    #[test]
    fn try_from_proto_rejects_non_scalar_udf_value() {
        let node = proto_scalar_udf_node(
            vec![column_node("a")],
            Some(proto_int32_arrow_type()),
            true,
        );
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let decoder = NonUdfStubDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = ScalarFunctionExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("decode_udf returned a non-ScalarUDF value"),
            "{err}"
        );
    }

    #[test]
    fn try_from_proto_propagates_child_decode_error() {
        let node = proto_scalar_udf_node(
            vec![column_node("a")],
            Some(proto_int32_arrow_type()),
            true,
        );
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let decoder = UdfStubDecoder {
            inner: StubDecoder::failing_on(1),
        };
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = ScalarFunctionExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(err.to_string().contains("stub decode failure"), "{err}");
    }
}
