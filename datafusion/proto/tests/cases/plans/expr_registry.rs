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

//! Extension `PhysicalExpr`s decoded by name through a
//! [`ProtoDecoderRegistry`] instead of a [`PhysicalExtensionCodec`].

use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{BinaryExpr, col, lit};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::proto::PhysicalExprDecodeCtx;
use datafusion::physical_plan::proto::PhysicalExprEncodeCtx;
use datafusion::physical_plan::proto::{
    ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExtensionExprFromProto,
    ExtensionPlanFromProto, ProtoDecoderRegistry, extension_expr_node,
    physical_expr_names, register_execution_plan, register_physical_expr,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    DataFusionError, Result, internal_datafusion_err, internal_err, resources_err,
};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    AggregateUDF, ColumnarValue, Operator, ScalarUDF, Volatility, create_udf,
};
use datafusion_physical_expr_common::expect_expr_variant;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec,
    physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::physical_plan::{
    DeduplicatingProtoConverter, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::{
    PhysicalExprNode, PhysicalPlanNode, physical_expr_node,
};
use prost::Message;

/// The wire name of a [`TaggedExpr`], and whether its decoder fails on
/// purpose. Each kind is a distinct Rust type, which the registry's identity
/// rules need.
trait TagKind: Debug + Clone + Send + Sync + 'static {
    const NAME: &'static str;
    const DECODE_FAILS: bool = false;
}

#[derive(Debug, Clone)]
struct Tag;
impl TagKind for Tag {
    const NAME: &'static str = "datafusion.tests.TagExpr";
}

/// A different type that claims `Tag`'s name.
#[derive(Debug, Clone)]
struct TagClone;
impl TagKind for TagClone {
    const NAME: &'static str = Tag::NAME;
}

#[derive(Debug, Clone)]
struct Other;
impl TagKind for Other {
    const NAME: &'static str = "other_crate.OtherExpr";
}

#[derive(Debug, Clone)]
struct Unnamed;
impl TagKind for Unnamed {
    const NAME: &'static str = "";
}

/// Registered under `Tag`'s name, with a decoder that fails.
#[derive(Debug, Clone)]
struct Failing;
impl TagKind for Failing {
    const NAME: &'static str = Tag::NAME;
    const DECODE_FAILS: bool = true;
}

/// An extension expression with a payload of its own (`tag`) and one child.
#[derive(Debug, Clone)]
struct TaggedExpr<K: TagKind> {
    tag: String,
    child: Arc<dyn PhysicalExpr>,
    /// The session's batch size, read at decode time. Only the registry
    /// decoder sets it, so a test can tell the registry path from the codec
    /// path.
    decoded_batch_size: Option<usize>,
    kind: PhantomData<K>,
}

type TagExpr = TaggedExpr<Tag>;

impl<K: TagKind> TaggedExpr<K> {
    fn new(tag: &str, child: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            tag: tag.to_string(),
            child,
            decoded_batch_size: None,
            kind: PhantomData,
        }
    }
}

impl<K: TagKind> Display for TaggedExpr<K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaggedExpr({}, {})", self.tag, self.child)
    }
}

impl<K: TagKind> PartialEq for TaggedExpr<K> {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag && self.child.eq(&other.child)
    }
}

impl<K: TagKind> Eq for TaggedExpr<K> {}

impl<K: TagKind> std::hash::Hash for TaggedExpr<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
        self.child.hash(state);
    }
}

impl<K: TagKind> PhysicalExpr for TaggedExpr<K> {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.child.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            child: Arc::clone(&children[0]),
            ..(*self).clone()
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn try_to_proto(
        &self,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<PhysicalExprNode>> {
        Ok(Some(extension_expr_node::<Self>(
            ctx,
            name_payload(&self.tag),
            self.children(),
        )?))
    }
}

impl<K: TagKind> ExtensionExprFromProto for TaggedExpr<K> {
    const NAME: &'static str = K::NAME;

    fn try_from_proto(
        node: &PhysicalExprNode,
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if K::DECODE_FAILS {
            return internal_err!("registry decoder failed on purpose");
        }
        let extension = expect_expr_variant!(
            node,
            physical_expr_node::ExprType::Extension,
            "Extension"
        );
        let [child] =
            <[_; 1]>::try_from(ctx.decode_children_expressions(&extension.inputs)?)
                .map_err(|_| {
                    internal_datafusion_err!("TaggedExpr expects exactly one child")
                })?;
        Ok(Arc::new(Self {
            tag: decode_name_payload(&extension.expr)?,
            child,
            decoded_batch_size: Some(task_ctx(ctx)?.session_config().batch_size()),
            kind: PhantomData,
        }))
    }
}

/// A function the decoder resolves by name from the session.
#[derive(Debug, Clone)]
enum FunctionRef {
    Scalar(Arc<ScalarUDF>),
    Aggregate(Arc<AggregateUDF>),
}

/// An extension expression that holds a UDF, as an expression that wraps a
/// user-defined scalar or aggregate function would. Only the function name is
/// written. The decoder finds the function in the session's registry, so a
/// user-defined function decodes to the instance registered on the reading
/// session.
#[derive(Debug, Clone)]
struct FunctionRefExpr {
    function: FunctionRef,
    child: Arc<dyn PhysicalExpr>,
}

impl FunctionRefExpr {
    fn name(&self) -> &str {
        match &self.function {
            FunctionRef::Scalar(udf) => udf.name(),
            FunctionRef::Aggregate(udaf) => udaf.name(),
        }
    }
}

impl Display for FunctionRefExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FunctionRefExpr({}, {})", self.name(), self.child)
    }
}

impl PartialEq for FunctionRefExpr {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.child.eq(&other.child)
    }
}

impl Eq for FunctionRefExpr {}

impl std::hash::Hash for FunctionRefExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.child.hash(state);
    }
}

impl PhysicalExpr for FunctionRefExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.child.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            function: self.function.clone(),
            child: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn try_to_proto(
        &self,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<PhysicalExprNode>> {
        let payload = match &self.function {
            FunctionRef::Scalar(udf) => format!("scalar:{}", udf.name()),
            FunctionRef::Aggregate(udaf) => format!("aggregate:{}", udaf.name()),
        };
        Ok(Some(extension_expr_node::<Self>(
            ctx,
            name_payload(&payload),
            self.children(),
        )?))
    }
}

impl ExtensionExprFromProto for FunctionRefExpr {
    const NAME: &'static str = "datafusion.tests.FunctionRefExpr";

    fn try_from_proto(
        node: &PhysicalExprNode,
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let extension = expect_expr_variant!(
            node,
            physical_expr_node::ExprType::Extension,
            "Extension"
        );
        let payload = decode_name_payload(&extension.expr)?;
        let functions = task_ctx(ctx)?;
        let function = match payload.split_once(':') {
            Some(("scalar", name)) => FunctionRef::Scalar(functions.udf(name)?),
            Some(("aggregate", name)) => FunctionRef::Aggregate(functions.udaf(name)?),
            _ => return internal_err!("bad FunctionRefExpr payload: {payload}"),
        };
        let [child] =
            <[_; 1]>::try_from(ctx.decode_children_expressions(&extension.inputs)?)
                .map_err(|_| {
                    internal_datafusion_err!("FunctionRefExpr expects exactly one child")
                })?;
        Ok(Arc::new(Self { function, child }))
    }
}

fn task_ctx<'a>(ctx: &'a PhysicalExprDecodeCtx<'_>) -> Result<&'a TaskContext> {
    ctx.session::<TaskContext>()
        .ok_or_else(|| internal_datafusion_err!("decode has no TaskContext"))
}

/// Wire layout for the test expressions' own state. Children are written in
/// the extension node's `inputs`, not here.
#[derive(Clone, PartialEq, prost::Message)]
struct NamePayload {
    #[prost(string, tag = "1")]
    name: String,
}

fn name_payload(name: &str) -> Vec<u8> {
    NamePayload {
        name: name.to_string(),
    }
    .encode_to_vec()
}

fn decode_name_payload(buf: &[u8]) -> Result<String> {
    NamePayload::decode(buf)
        .map(|proto| proto.name)
        .map_err(|e| internal_datafusion_err!("decode NamePayload: {e}"))
}

/// A codec for the expression tests. `decode` is what `try_decode_expr` does.
/// `try_encode_expr` always fails: the test expressions write themselves, so
/// the codec must not be asked to encode them.
#[derive(Debug)]
enum TestCodec {
    /// Fails with an internal error. A successful decode through it proves
    /// the codec was not used.
    Refusing,
    /// Fails with an error kind of its own.
    Exhausted,
    /// Decodes a `TagExpr`, as a codec did before the registry existed.
    Tag,
}

impl PhysicalExtensionCodec for TestCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("TestCodec::try_decode must not be reached")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("TestCodec::try_encode must not be reached")
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        _ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match self {
            Self::Refusing => internal_err!("RefusingCodec must not be reached"),
            Self::Exhausted => resources_err!("the decode buffer pool is empty"),
            Self::Tag => {
                let [child] = inputs else {
                    return internal_err!("TagExpr expects exactly one child");
                };
                Ok(Arc::new(TagExpr::new(
                    &decode_name_payload(buf)?,
                    Arc::clone(child),
                )))
            }
        }
    }

    fn try_encode_expr(
        &self,
        _node: &Arc<dyn PhysicalExpr>,
        _buf: &mut Vec<u8>,
        _ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        internal_err!("TestCodec::try_encode_expr must not be reached")
    }
}

/// A session whose registry is filled by `register`.
fn session_with(
    config: SessionConfig,
    register: impl FnOnce(&mut ProtoDecoderRegistry) -> Result<()>,
) -> Result<SessionContext> {
    let mut registry = ProtoDecoderRegistry::new();
    register(&mut registry)?;
    Ok(SessionContext::new_with_config(
        config.with_extension(Arc::new(registry)),
    ))
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
}

/// Serialize `expr` and decode the prost bytes, as the wire would.
fn to_wire(
    expr: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<PhysicalExprNode> {
    let proto = DefaultPhysicalProtoConverter {}.physical_expr_to_proto(expr, codec)?;
    PhysicalExprNode::decode(proto.encode_to_vec().as_slice())
        .map_err(|e| internal_datafusion_err!("decode PhysicalExprNode: {e}"))
}

fn from_wire(
    node: &PhysicalExprNode,
    session: &SessionContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    let task_ctx = session.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), codec);
    DefaultPhysicalProtoConverter {}.proto_to_physical_expr(
        node,
        &test_schema(),
        &decode_ctx,
    )
}

fn tag_expr() -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(TagExpr::new("hello", col("a", &test_schema())?)))
}

fn set_expr_name(node: &mut PhysicalExprNode, name: Option<&str>) {
    match node.expr_type.as_mut().unwrap() {
        physical_expr_node::ExprType::Extension(extension) => {
            extension.expr_name = name.map(str::to_string);
        }
        other => panic!("expected an Extension node, got {other:?}"),
    }
}

/// An extension expression writes its name, and the session's registry
/// decodes it with no codec on either side. The decoder can read the session.
#[test]
fn registry_decodes_named_extension_expr() -> Result<()> {
    let node = to_wire(&tag_expr()?, &TestCodec::Refusing)?;
    match node.expr_type.as_ref().unwrap() {
        physical_expr_node::ExprType::Extension(extension) => {
            assert_eq!(extension.expr_name.as_deref(), Some(TagExpr::NAME))
        }
        other => panic!("expected an Extension node, got {other:?}"),
    }

    let session = session_with(
        SessionConfig::new().with_batch_size(1234),
        register_physical_expr::<TagExpr>,
    )?;
    let decoded = from_wire(&node, &session, &TestCodec::Refusing)?;
    let decoded = decoded.downcast_ref::<TagExpr>().unwrap();
    assert_eq!(decoded.tag, "hello");
    assert_eq!(decoded.child.to_string(), "a@0");
    assert_eq!(decoded.decoded_batch_size, Some(1234));
    Ok(())
}

/// A node with no name, or with a name the session does not know, decodes
/// through the codec as before.
#[test]
fn unnamed_or_unregistered_falls_back_to_codec() -> Result<()> {
    let named = to_wire(&tag_expr()?, &TestCodec::Tag)?;
    let mut unnamed = named.clone();
    set_expr_name(&mut unnamed, None);

    let empty = SessionContext::new();
    let registered =
        session_with(SessionConfig::new(), register_physical_expr::<TagExpr>)?;
    for (node, session) in [(&named, &empty), (&unnamed, &registered)] {
        let decoded = from_wire(node, session, &TestCodec::Tag)?;
        let decoded = decoded.downcast_ref::<TagExpr>().unwrap();
        assert_eq!(decoded.tag, "hello");
        assert_eq!(decoded.decoded_batch_size, None, "must come from the codec");
    }
    Ok(())
}

/// When the codec also fails, the error keeps the codec's own error kind and
/// message. If the node had a name, the error also says that no decoder is
/// registered for it and lists the names the session knows.
#[test]
fn codec_failure_keeps_its_error_and_names_the_missing_registration() -> Result<()> {
    let named = to_wire(&tag_expr()?, &TestCodec::Refusing)?;
    let session = session_with(
        SessionConfig::new(),
        register_physical_expr::<TaggedExpr<Other>>,
    )?;

    let err = from_wire(&named, &session, &TestCodec::Exhausted).unwrap_err();
    assert!(
        matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
        "the codec's error kind must survive, got: {err:?}"
    );
    let text = err.to_string();
    for expected in [
        "the decode buffer pool is empty",
        "No decoder is registered",
        TagExpr::NAME,
        "ProtoDecoderRegistry",
        Other::NAME,
    ] {
        assert!(text.contains(expected), "missing {expected:?} in: {text}");
    }

    let mut unnamed = named;
    set_expr_name(&mut unnamed, None);
    let text = from_wire(&unnamed, &session, &TestCodec::Refusing)
        .unwrap_err()
        .to_string();
    assert!(text.contains("RefusingCodec must not be reached"), "{text}");
    assert!(!text.contains("No decoder is registered"), "{text}");
    Ok(())
}

/// Registering one type twice is a no-op. Two types with the same name, or a
/// type with an empty name, are errors at registration.
#[test]
fn registration_rejects_collisions_and_empty_names() -> Result<()> {
    let mut registry = ProtoDecoderRegistry::new();
    register_physical_expr::<TagExpr>(&mut registry)?;
    register_physical_expr::<TagExpr>(&mut registry)?;

    let err = register_physical_expr::<TaggedExpr<TagClone>>(&mut registry)
        .unwrap_err()
        .to_string();
    assert!(err.contains("already registered"), "{err}");

    let err = register_physical_expr::<TaggedExpr<Unnamed>>(&mut registry)
        .unwrap_err()
        .to_string();
    assert!(err.contains("empty name"), "{err}");

    assert_eq!(
        physical_expr_names(&registry).collect::<Vec<_>>(),
        [Tag::NAME]
    );
    Ok(())
}

/// The children of a registry-decoded expression also decode through the
/// registry.
#[test]
fn nested_extension_exprs_both_decode_through_the_registry() -> Result<()> {
    let inner: Arc<dyn PhysicalExpr> =
        Arc::new(TagExpr::new("inner", col("a", &test_schema())?));
    let outer: Arc<dyn PhysicalExpr> = Arc::new(TagExpr::new("outer", inner));
    let node = to_wire(&outer, &TestCodec::Refusing)?;
    let session = session_with(SessionConfig::new(), register_physical_expr::<TagExpr>)?;

    let decoded = from_wire(&node, &session, &TestCodec::Refusing)?;
    let outer = decoded.downcast_ref::<TagExpr>().unwrap();
    let inner = outer.child.downcast_ref::<TagExpr>().unwrap();
    assert_eq!((outer.tag.as_str(), inner.tag.as_str()), ("outer", "inner"));
    assert!(inner.decoded_batch_size.is_some());
    Ok(())
}

/// If the registered decoder fails, the decode fails. The codec is not tried,
/// even though it could decode this node.
#[test]
fn a_failing_registry_decoder_does_not_fall_through_to_the_codec() -> Result<()> {
    let node = to_wire(&tag_expr()?, &TestCodec::Tag)?;
    let session = session_with(
        SessionConfig::new(),
        register_physical_expr::<TaggedExpr<Failing>>,
    )?;
    let err = from_wire(&node, &session, &TestCodec::Tag).unwrap_err();
    assert!(
        err.to_string()
            .contains("registry decoder failed on purpose"),
        "{err}"
    );
    Ok(())
}

/// A user-defined scalar function and an aggregate function, held by an
/// extension expression, decode to the instances registered on the reading
/// session.
#[test]
fn extension_expr_resolves_scalar_and_aggregate_udfs_from_the_session() -> Result<()> {
    let session = session_with(
        SessionConfig::new(),
        register_physical_expr::<FunctionRefExpr>,
    )?;
    session.register_udf(create_udf(
        "my_add_one",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| Ok(args[0].clone())),
    ));
    let state = session.state();
    let scalar = state.udf("my_add_one")?;
    let aggregate = state.udaf("sum")?;

    for function in [
        FunctionRef::Scalar(Arc::clone(&scalar)),
        FunctionRef::Aggregate(Arc::clone(&aggregate)),
    ] {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(FunctionRefExpr {
            function,
            child: col("a", &test_schema())?,
        });
        let node = to_wire(&expr, &TestCodec::Refusing)?;
        let decoded = from_wire(&node, &session, &TestCodec::Refusing)?;
        match &decoded.downcast_ref::<FunctionRefExpr>().unwrap().function {
            FunctionRef::Scalar(udf) => assert!(Arc::ptr_eq(udf, &scalar)),
            FunctionRef::Aggregate(udaf) => assert!(Arc::ptr_eq(udaf, &aggregate)),
        }
    }

    // A reader that does not have the UDF gets the registry's error.
    let expr: Arc<dyn PhysicalExpr> = Arc::new(FunctionRefExpr {
        function: FunctionRef::Scalar(scalar),
        child: col("a", &test_schema())?,
    });
    let node = to_wire(&expr, &TestCodec::Refusing)?;
    let reader = session_with(
        SessionConfig::new(),
        register_physical_expr::<FunctionRefExpr>,
    )?;
    let err = from_wire(&node, &reader, &TestCodec::Refusing).unwrap_err();
    assert!(err.to_string().contains("my_add_one"), "{err}");
    Ok(())
}

/// A whole plan, through `physical_plan_to_bytes` and
/// `physical_plan_from_bytes`.
#[test]
fn registry_expr_survives_a_whole_plan_roundtrip() -> Result<()> {
    let schema = test_schema();
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(TagExpr::new("in_plan", col("a", &schema)?)),
        Operator::Eq,
        lit(1i64),
    ));
    let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, input)?);
    let bytes = physical_plan_to_bytes_with_extension_codec(plan, &TestCodec::Refusing)?;

    let session = session_with(
        SessionConfig::new().with_batch_size(4096),
        register_physical_expr::<TagExpr>,
    )?;
    let decoded = physical_plan_from_bytes_with_extension_codec(
        &bytes,
        session.task_ctx().as_ref(),
        &TestCodec::Refusing,
    )?;
    let filter = decoded.downcast_ref::<FilterExec>().unwrap();
    let binary = filter.predicate().downcast_ref::<BinaryExpr>().unwrap();
    let tag = binary.left().downcast_ref::<TagExpr>().unwrap();
    assert_eq!(tag.tag, "in_plan");
    assert_eq!(tag.decoded_batch_size, Some(4096));
    Ok(())
}

/// An extension expression with an identity: two references with the same
/// `expression_id` share `state`, as two pushed-down copies of one dynamic
/// filter would. `with_new_children` keeps `state`, so a deduplicating decoder
/// that re-shares the first decode keeps the two copies linked.
#[derive(Debug, Clone)]
struct IdentityExpr {
    id: u64,
    state: Arc<String>,
    child: Arc<dyn PhysicalExpr>,
}

impl Display for IdentityExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdentityExpr({}, {})", self.id, self.child)
    }
}

impl PartialEq for IdentityExpr {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.child.eq(&other.child)
    }
}

impl Eq for IdentityExpr {}

impl std::hash::Hash for IdentityExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.child.hash(state);
    }
}

impl PhysicalExpr for IdentityExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.child.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            child: Arc::clone(&children[0]),
            ..(*self).clone()
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn expression_id(&self) -> Option<u64> {
        Some(self.id)
    }

    fn try_to_proto(
        &self,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<PhysicalExprNode>> {
        // The helper cannot see `expression_id`; the serializer stamps it.
        Ok(Some(extension_expr_node::<Self>(
            ctx,
            name_payload(&self.state),
            self.children(),
        )?))
    }
}

impl ExtensionExprFromProto for IdentityExpr {
    const NAME: &'static str = "datafusion-test.IdentityExpr";

    fn try_from_proto(
        node: &PhysicalExprNode,
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let extension = expect_expr_variant!(
            node,
            physical_expr_node::ExprType::Extension,
            "Extension"
        );
        let Some(id) = node.expr_id else {
            return internal_err!("IdentityExpr needs an expr_id");
        };
        let mut children = ctx.decode_children_expressions(&extension.inputs)?;
        Ok(Arc::new(Self {
            id,
            state: Arc::new(decode_name_payload(&extension.expr)?),
            child: children.remove(0),
        }))
    }
}

/// An extension expression built with `extension_expr_node` keeps its
/// `expression_id` on the wire. Two references to it, with different
/// children, decode through the deduplicating converter to two expressions
/// that share state, each with its own child.
#[test]
fn registry_expr_keeps_its_expression_id_for_deduplication() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let state = Arc::new("shared".to_string());
    let left: Arc<dyn PhysicalExpr> = Arc::new(IdentityExpr {
        id: 42,
        state: Arc::clone(&state),
        child: col("a", &schema)?,
    });
    let right: Arc<dyn PhysicalExpr> = Arc::new(IdentityExpr {
        id: 42,
        state,
        child: col("b", &schema)?,
    });
    let pair: Arc<dyn PhysicalExpr> =
        Arc::new(BinaryExpr::new(Arc::clone(&left), Operator::And, right));

    let codec = TestCodec::Refusing;
    let converter = DeduplicatingProtoConverter {};
    let node = converter.physical_expr_to_proto(&pair, &codec)?;
    assert_eq!(
        to_wire(&left, &codec)?.expr_id,
        Some(42),
        "the serializer must stamp the expression id"
    );

    let session =
        session_with(SessionConfig::new(), register_physical_expr::<IdentityExpr>)?;
    let task_ctx = session.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), &codec);
    let decoded = converter.proto_to_physical_expr(&node, &schema, &decode_ctx)?;
    let binary = decoded.downcast_ref::<BinaryExpr>().unwrap();
    let left = binary.left().downcast_ref::<IdentityExpr>().unwrap();
    let right = binary.right().downcast_ref::<IdentityExpr>().unwrap();
    assert!(
        Arc::ptr_eq(&left.state, &right.state),
        "state must be shared"
    );
    assert_eq!(left.child.to_string(), "a@0");
    assert_eq!(right.child.to_string(), "b@1");
    Ok(())
}

/// An extension plan whose wire name equals [`TagExpr::NAME`]. The registry
/// scopes names per kind, so the plan and the expression do not collide.
#[derive(Debug)]
struct SameNameExec {
    child: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for SameNameExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SameNameExec")
    }
}

impl ExecutionPlan for SameNameExec {
    fn name(&self) -> &str {
        "SameNameExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            child: children.remove(0),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("SameNameExec is a serde-only test plan")
    }

    fn try_to_proto(
        &self,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
        Ok(Some(ctx.extension_node::<Self>(vec![], self.children())?))
    }
}

impl ExtensionPlanFromProto for SameNameExec {
    const NAME: &'static str = Tag::NAME;

    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(PhysicalPlanType::Extension(extension)) = &node.physical_plan_type
        else {
            return internal_err!("expected an extension node");
        };
        let mut children = ctx.decode_children(&extension.inputs)?;
        Ok(Arc::new(Self {
            child: children.remove(0),
        }))
    }
}

/// A plan that holds a registered extension plan and a registered extension
/// expression with the same wire name. Both decode through the registry, each
/// to its own type.
#[test]
fn registry_plan_and_expr_with_the_same_name_roundtrip_together() -> Result<()> {
    let schema = test_schema();
    let extension_plan: Arc<dyn ExecutionPlan> = Arc::new(SameNameExec {
        child: Arc::new(EmptyExec::new(Arc::clone(&schema))),
    });
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(TagExpr::new("same_name", col("a", &schema)?)),
        Operator::Eq,
        lit(1i64),
    ));
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, extension_plan)?);
    let bytes = physical_plan_to_bytes_with_extension_codec(plan, &TestCodec::Refusing)?;

    let session = session_with(SessionConfig::new().with_batch_size(2048), |registry| {
        register_execution_plan::<SameNameExec>(registry)?;
        register_physical_expr::<TagExpr>(registry)
    })?;
    let decoded = physical_plan_from_bytes_with_extension_codec(
        &bytes,
        session.task_ctx().as_ref(),
        &TestCodec::Refusing,
    )?;
    let filter = decoded.downcast_ref::<FilterExec>().unwrap();
    let binary = filter.predicate().downcast_ref::<BinaryExpr>().unwrap();
    let tag = binary.left().downcast_ref::<TagExpr>().unwrap();
    assert_eq!(tag.tag, "same_name");
    assert_eq!(tag.decoded_batch_size, Some(2048));
    let extension = filter.input().downcast_ref::<SameNameExec>().unwrap();
    assert_eq!(extension.child.name(), "EmptyExec");
    Ok(())
}
