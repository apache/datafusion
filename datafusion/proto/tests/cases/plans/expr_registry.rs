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
//! [`PhysicalExprRegistry`] instead of a [`PhysicalExtensionCodec`].

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
use datafusion::physical_plan::proto::{
    ExprDecodeSession, ExtensionExprFromProto, ProtoDecoderRegistry, extension_expr_node,
    physical_expr_names, register_physical_expr,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{
    DataFusionError, Result, internal_datafusion_err, internal_err, resources_err,
};
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr_common::expect_expr_variant;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec,
    physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::physical_plan::{
    DeduplicatingProtoConverter, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{PhysicalExprNode, physical_expr_node};
use prost::Message;

/// The identity of a test expression type: its wire name, and whether its
/// decoder fails on purpose. Each kind makes [`TaggedExpr`] a distinct Rust
/// type, which the registry's identity rules (idempotent for one type, an
/// error for two types claiming one name) need.
trait TagKind: Debug + Clone + Send + Sync + 'static {
    const NAME: &'static str;
    const DECODE_FAILS: bool = false;
}

#[derive(Debug, Clone)]
struct Tag;
impl TagKind for Tag {
    const NAME: &'static str = "datafusion.tests.TagExpr";
}

/// A *different* Rust type claiming `Tag`'s name: the collision two
/// independent crates could hit.
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

/// An author who forgot to fill in the name: registration must reject it
/// rather than let an unnamed node reach the wire.
#[derive(Debug, Clone)]
struct Unnamed;
impl TagKind for Unnamed {
    const NAME: &'static str = "";
}

/// Registered under `Tag`'s name, so a `TagExpr` on the wire reaches a
/// decoder that fails.
#[derive(Debug, Clone)]
struct Failing;
impl TagKind for Failing {
    const NAME: &'static str = Tag::NAME;
    const DECODE_FAILS: bool = true;
}

/// An extension expression that decodes through the registry.
///
/// It passes its child through untouched and carries a `tag` of its own, so
/// a round-trip has both an opaque payload and a child expression to
/// reconstruct.
#[derive(Debug, Clone)]
struct TaggedExpr<K: TagKind> {
    tag: String,
    child: Arc<dyn PhysicalExpr>,
    /// Identity for deduplication, when the test wants one. The driver stamps
    /// it onto the node from `expression_id()` and `try_from_proto` reads it
    /// back.
    expression_id: Option<u64>,
    /// The session's configured batch size, read from `TaskContext` at decode
    /// time. `None` on any expression that did not come from the registry
    /// decoder, which is what lets a test tell the two decode paths apart.
    decoded_batch_size: Option<usize>,
    /// Stands in for state a real expression shares between derived copies
    /// (`DynamicFilterPhysicalExpr` shares its `Inner` this way). A fresh
    /// decode mints a new one and `with_new_children` carries it over, so two
    /// decoded expressions sharing it proves they came from one cache entry.
    shared_state: Arc<()>,
    kind: PhantomData<K>,
}

type TagExpr = TaggedExpr<Tag>;
type TagExprClone = TaggedExpr<TagClone>;
type OtherExpr = TaggedExpr<Other>;
type UnnamedExpr = TaggedExpr<Unnamed>;
type FailingTagExpr = TaggedExpr<Failing>;

impl<K: TagKind> TaggedExpr<K> {
    fn new(tag: &str, child: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            tag: tag.to_string(),
            child,
            expression_id: None,
            decoded_batch_size: None,
            shared_state: Arc::new(()),
            kind: PhantomData,
        }
    }

    fn with_expression_id(mut self, id: u64) -> Self {
        self.expression_id = Some(id);
        self
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
            tag: self.tag.clone(),
            child: Arc::clone(&children[0]),
            expression_id: self.expression_id,
            decoded_batch_size: self.decoded_batch_size,
            shared_state: Arc::clone(&self.shared_state),
            kind: PhantomData,
        }))
    }

    fn expression_id(&self) -> Option<u64> {
        self.expression_id
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn try_to_proto(
        &self,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<PhysicalExprNode>> {
        // `expr_id` is left for the driver to stamp from `expression_id()`.
        Ok(Some(extension_expr_node::<Self>(
            ctx,
            tag_payload(&self.tag),
            self.children(),
        )?))
    }
}

impl<K: TagKind> ExtensionExprFromProto for TaggedExpr<K> {
    const NAME: &'static str = K::NAME;

    fn try_from_proto(
        node: &PhysicalExprNode,
        ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if K::DECODE_FAILS {
            return internal_err!("registry decoder failed on purpose");
        }
        let extension = expect_expr_variant!(
            node,
            physical_expr_node::ExprType::Extension,
            "Extension"
        );
        let tag = decode_tag_payload(&extension.expr)?;
        let children = ctx.decode_children_expressions(&extension.inputs)?;
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            internal_datafusion_err!("TaggedExpr expects exactly one child")
        })?;

        // The load-bearing part of this test: reach session state from a
        // registry-decoded expression.
        let batch_size = ctx.session().config_options().execution.batch_size.get();

        Ok(Arc::new(Self {
            tag,
            child,
            expression_id: node.expr_id,
            decoded_batch_size: Some(batch_size),
            shared_state: Arc::new(()),
            kind: PhantomData,
        }))
    }
}

/// Wire layout for [`TagExpr`]'s own state. The child rides along in the
/// extension node's `inputs`, not in here.
#[derive(Clone, PartialEq, prost::Message)]
struct TagExprProto {
    #[prost(string, tag = "1")]
    tag: String,
}

fn tag_payload(tag: &str) -> Vec<u8> {
    TagExprProto {
        tag: tag.to_string(),
    }
    .encode_to_vec()
}

fn decode_tag_payload(buf: &[u8]) -> Result<String> {
    TagExprProto::decode(buf)
        .map(|proto| proto.tag)
        .map_err(|e| internal_datafusion_err!("decode TagExprProto: {e}"))
}

/// A codec that refuses every expression, so a test that decodes successfully
/// through it proves the codec was never consulted.
#[derive(Debug)]
struct RefusingCodec;

impl PhysicalExtensionCodec for RefusingCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("RefusingCodec::try_decode must not be reached")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("RefusingCodec::try_encode must not be reached")
    }

    fn try_decode_expr(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn PhysicalExpr>],
        _ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        internal_err!("RefusingCodec::try_decode_expr must not be reached")
    }
}

/// A codec that fails while decoding, with an error kind of its own. Stands in
/// for every codec failure whose cause is *not* a missing registration.
#[derive(Debug)]
struct ExhaustedCodec;

impl PhysicalExtensionCodec for ExhaustedCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("ExhaustedCodec::try_decode must not be reached")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("ExhaustedCodec::try_encode must not be reached")
    }

    fn try_decode_expr(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn PhysicalExpr>],
        _ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        resources_err!("the decode buffer pool is empty")
    }
}

/// A codec that can decode a `TagExpr`, standing in for the pre-registry way
/// of doing this.
#[derive(Debug)]
struct TagCodec;

impl PhysicalExtensionCodec for TagCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("not used")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("not used")
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        _ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let tag = decode_tag_payload(buf)?;
        let [child] = inputs else {
            return internal_err!("TagExpr expects exactly one child");
        };
        // No `decoded_batch_size`: that is how the tests tell this path from
        // the registry one.
        Ok(Arc::new(TagExpr::new(&tag, Arc::clone(child))))
    }

    fn try_encode_expr(
        &self,
        _node: &Arc<dyn PhysicalExpr>,
        _buf: &mut Vec<u8>,
        _ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        // `TagExpr` serializes itself through `try_to_proto`, so the encode
        // half of the codec must never be consulted — not even on the paths
        // where its decode half is.
        internal_err!("TagCodec::try_encode_expr must not be reached")
    }
}

/// A registry populated by `register`, ready to attach with
/// `SessionConfig::with_extension`.
fn registry(
    register: impl FnOnce(&mut ProtoDecoderRegistry) -> Result<()>,
) -> Result<Arc<ProtoDecoderRegistry>> {
    let mut registry = ProtoDecoderRegistry::new();
    register(&mut registry)?;
    Ok(Arc::new(registry))
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
}

/// Serialize `expr`, push it through prost bytes to mimic the wire, and hand
/// back the node the decoder will see.
fn to_wire(
    expr: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<PhysicalExprNode> {
    let converter = DefaultPhysicalProtoConverter {};
    let proto = converter.physical_expr_to_proto(expr, codec)?;
    let bytes = proto.encode_to_vec();
    PhysicalExprNode::decode(bytes.as_slice())
        .map_err(|e| internal_datafusion_err!("decode PhysicalExprNode: {e}"))
}

fn from_wire(
    node: &PhysicalExprNode,
    session: &SessionContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    let converter = DefaultPhysicalProtoConverter {};
    let task_ctx = session.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), codec);
    converter.proto_to_physical_expr(node, &test_schema(), &decode_ctx)
}

fn tag_expr() -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(TagExpr::new("hello", col("a", &test_schema())?)))
}

/// The whole point: an extension expression names itself on the wire and the
/// session's registry decodes it, with no codec involved on either side.
#[test]
fn registry_decodes_named_extension_expr() -> Result<()> {
    let expr = tag_expr()?;
    let node = to_wire(&expr, &RefusingCodec)?;

    // The name is what makes registry decoding possible.
    let extension = match node.expr_type.as_ref().unwrap() {
        physical_expr_node::ExprType::Extension(extension) => extension,
        other => panic!("expected an Extension node, got {other:?}"),
    };
    assert_eq!(extension.expr_name.as_deref(), Some(TagExpr::NAME));

    let config = SessionConfig::new()
        .with_batch_size(1234)
        .with_extension(registry(register_physical_expr::<TagExpr>)?);
    let session = SessionContext::new_with_config(config);

    let decoded = from_wire(&node, &session, &RefusingCodec)?;
    let decoded = decoded
        .downcast_ref::<TagExpr>()
        .expect("must decode back to TagExpr");

    assert_eq!(decoded.tag, "hello");
    assert_eq!(decoded.child.to_string(), "a@0");
    // Set only by the registry decoder, and only reachable through
    // `PhysicalExprDecodeCtx::task_ctx`.
    assert_eq!(decoded.decoded_batch_size, Some(1234));
    Ok(())
}

/// A name this session has no decoder for falls back to the codec, unchanged.
#[test]
fn unregistered_name_falls_back_to_codec() -> Result<()> {
    let expr = tag_expr()?;
    let node = to_wire(&expr, &TagCodec)?;

    let session = SessionContext::new();
    let decoded = from_wire(&node, &session, &TagCodec)?;
    let decoded = decoded
        .downcast_ref::<TagExpr>()
        .expect("must decode back to TagExpr");

    assert_eq!(decoded.tag, "hello");
    assert_eq!(decoded.decoded_batch_size, None, "must come from the codec");
    Ok(())
}

/// A node written without a name — every node written before `expr_name`
/// existed — takes the codec path even when a registry is present.
#[test]
fn unnamed_extension_expr_falls_back_to_codec() -> Result<()> {
    let expr = tag_expr()?;
    let mut node = to_wire(&expr, &TagCodec)?;
    match node.expr_type.as_mut().unwrap() {
        physical_expr_node::ExprType::Extension(extension) => {
            extension.expr_name = None;
        }
        other => panic!("expected an Extension node, got {other:?}"),
    }

    let config =
        SessionConfig::new().with_extension(registry(register_physical_expr::<TagExpr>)?);
    let session = SessionContext::new_with_config(config);

    let decoded = from_wire(&node, &session, &TagCodec)?;
    let decoded = decoded
        .downcast_ref::<TagExpr>()
        .expect("must decode back to TagExpr");
    assert_eq!(decoded.decoded_batch_size, None, "must come from the codec");
    Ok(())
}

/// An unnamed node that no codec can decode reports the codec's own error,
/// untouched: there was no name, so there is no registration to point at.
#[test]
fn an_unnamed_node_the_codec_rejects_reports_the_codec_error() -> Result<()> {
    let expr = tag_expr()?;
    let mut node = to_wire(&expr, &RefusingCodec)?;
    match node.expr_type.as_mut().unwrap() {
        physical_expr_node::ExprType::Extension(extension) => {
            extension.expr_name = None;
        }
        other => panic!("expected an Extension node, got {other:?}"),
    }

    let session = SessionContext::new();
    let err = from_wire(&node, &session, &RefusingCodec)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("RefusingCodec::try_decode_expr must not be reached"),
        "unexpected error: {err}"
    );
    assert!(
        !err.contains("No decoder is registered"),
        "an unnamed node must not be blamed on a missing registration: {err}"
    );
    Ok(())
}

/// The failure a name typo actually produces: the node names itself, nothing
/// is registered under that name, and the codec cannot decode it either. The
/// codec's own error can't mention the name — it never saw one — so the decode
/// site has to say what went wrong and how to fix it.
#[test]
fn an_unregistered_name_that_the_codec_rejects_says_so() -> Result<()> {
    let expr = tag_expr()?;
    let node = to_wire(&expr, &RefusingCodec)?;

    // A session that registered a different expression, so the registry
    // exists but does not know this name.
    let config = SessionConfig::new()
        .with_extension(registry(register_physical_expr::<OtherExpr>)?);
    let session = SessionContext::new_with_config(config);

    let err = from_wire(&node, &session, &RefusingCodec)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(TagExpr::NAME),
        "error must name the unregistered expression: {err}"
    );
    assert!(
        err.contains("No decoder is registered"),
        "error must say the name was unregistered: {err}"
    );
    assert!(
        err.contains("ProtoDecoderRegistry"),
        "error must say how to fix it: {err}"
    );
    assert!(
        err.contains(OtherExpr::NAME),
        "error must list what the session does know: {err}"
    );
    // The underlying codec failure is still reported, not swallowed.
    assert!(
        err.contains("RefusingCodec::try_decode_expr must not be reached"),
        "error must keep the codec's own cause: {err}"
    );
    Ok(())
}

/// Sessions are often configured by more than one layer of an application,
/// so registering a type a second time must be harmless.
#[test]
fn registering_the_same_type_twice_is_a_noop() -> Result<()> {
    let mut registry = ProtoDecoderRegistry::new();
    register_physical_expr::<TagExpr>(&mut registry)?;
    register_physical_expr::<TagExpr>(&mut registry)?;
    assert_eq!(physical_expr_names(&registry).count(), 1);
    assert!(physical_expr_names(&registry).any(|name| name == TagExpr::NAME));
    Ok(())
}

/// A distinct type declaring the same `NAME`: the collision two
/// independent crates could hit, caught at registration.
#[test]
fn a_name_collision_between_types_is_an_error() -> Result<()> {
    let mut registry = ProtoDecoderRegistry::new();
    register_physical_expr::<TagExpr>(&mut registry)?;
    let err = register_physical_expr::<TagExprClone>(&mut registry)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(TagExpr::NAME) && err.contains("already registered"),
        "unexpected error: {err}"
    );
    // The failed registration left the existing entry untouched.
    assert_eq!(physical_expr_names(&registry).count(), 1);
    Ok(())
}

#[test]
fn an_empty_name_is_rejected() {
    let mut registry = ProtoDecoderRegistry::new();
    let err = register_physical_expr::<UnnamedExpr>(&mut registry).unwrap_err();
    assert!(
        err.to_string().contains("empty name"),
        "unexpected error: {err}"
    );
    assert!(physical_expr_names(&registry).next().is_none());
}

/// A registry-decoded expression stamps and recovers its `expression_id`, so
/// two references to one expression still deduplicate to a single `Arc` under
/// [`DeduplicatingProtoConverter`].
#[test]
fn registry_decoded_expr_participates_in_deduplication() -> Result<()> {
    let schema = test_schema();
    let shared: Arc<dyn PhysicalExpr> =
        Arc::new(TagExpr::new("shared", col("a", &schema)?).with_expression_id(7));
    // One expression referenced twice.
    let composite: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&shared),
        Operator::Eq,
        Arc::clone(&shared),
    ));

    let converter = DeduplicatingProtoConverter {};
    let proto = converter.physical_expr_to_proto(&composite, &RefusingCodec)?;
    let bytes = proto.encode_to_vec();
    let node = PhysicalExprNode::decode(bytes.as_slice())
        .map_err(|e| internal_datafusion_err!("decode PhysicalExprNode: {e}"))?;

    let config =
        SessionConfig::new().with_extension(registry(register_physical_expr::<TagExpr>)?);
    let session = SessionContext::new_with_config(config);
    let task_ctx = session.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), &RefusingCodec);
    let decoded = converter.proto_to_physical_expr(&node, &schema, &decode_ctx)?;

    let binary = decoded
        .downcast_ref::<BinaryExpr>()
        .expect("must decode back to BinaryExpr");
    let left = binary
        .left()
        .downcast_ref::<TagExpr>()
        .expect("must decode back to TagExpr");
    let right = binary
        .right()
        .downcast_ref::<TagExpr>()
        .expect("must decode back to TagExpr");

    // The second occurrence is derived from the first through
    // `with_new_children`, so it carries the same shared state rather than a
    // freshly decoded copy. Without the stamped `expr_id` the deserializer
    // could not tell they were the same expression.
    assert!(
        Arc::ptr_eq(&left.shared_state, &right.shared_state),
        "both occurrences must resolve to one deduplicated expression"
    );
    assert_eq!(left.expression_id, Some(7));
    assert_eq!(
        left.decoded_batch_size,
        Some(SessionConfig::new().batch_size())
    );
    Ok(())
}

/// Nesting re-enters the registry: the outer expression's children are decoded
/// through the context, so an inner extension expression resolves by name too.
#[test]
fn nested_extension_exprs_both_decode_through_the_registry() -> Result<()> {
    let schema = test_schema();
    let inner: Arc<dyn PhysicalExpr> =
        Arc::new(TagExpr::new("inner", col("a", &schema)?));
    let outer: Arc<dyn PhysicalExpr> = Arc::new(TagExpr::new("outer", inner));

    let node = to_wire(&outer, &RefusingCodec)?;
    let config =
        SessionConfig::new().with_extension(registry(register_physical_expr::<TagExpr>)?);
    let session = SessionContext::new_with_config(config);

    let decoded = from_wire(&node, &session, &RefusingCodec)?;
    let outer = decoded
        .downcast_ref::<TagExpr>()
        .expect("must decode back to TagExpr");
    assert_eq!(outer.tag, "outer");
    assert!(outer.decoded_batch_size.is_some());

    let inner = outer
        .child
        .downcast_ref::<TagExpr>()
        .expect("the child must decode back to TagExpr");
    assert_eq!(inner.tag, "inner");
    assert!(
        inner.decoded_batch_size.is_some(),
        "the nested expression must come from the registry, not the codec"
    );
    Ok(())
}

/// A registered decoder that fails is the answer — there is no second attempt
/// through the codec, which would decode a payload the codec never wrote.
#[test]
fn a_failing_registry_decoder_does_not_fall_through_to_the_codec() -> Result<()> {
    let expr = tag_expr()?;
    let node = to_wire(&expr, &TagCodec)?;

    // `FailingTagExpr` is registered under `TagExpr`'s wire name, so the
    // node routes to a decoder that fails.
    let config = SessionConfig::new()
        .with_extension(registry(register_physical_expr::<FailingTagExpr>)?);
    let session = SessionContext::new_with_config(config);

    // `TagCodec` could decode this node, so reaching it would mask the failure.
    let err = from_wire(&node, &session, &TagCodec).unwrap_err();
    assert!(
        err.to_string()
            .contains("registry decoder failed on purpose"),
        "unexpected error: {err}"
    );
    Ok(())
}

/// The path a user actually takes: a whole plan through
/// `physical_plan_to_bytes` / `physical_plan_from_bytes`.
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

    let bytes =
        physical_plan_to_bytes_with_extension_codec(Arc::clone(&plan), &RefusingCodec)?;

    let config = SessionConfig::new()
        .with_batch_size(4096)
        .with_extension(registry(register_physical_expr::<TagExpr>)?);
    let session = SessionContext::new_with_config(config);
    let task_ctx = session.task_ctx();
    let decoded = physical_plan_from_bytes_with_extension_codec(
        &bytes,
        task_ctx.as_ref(),
        &RefusingCodec,
    )?;

    let filter = decoded
        .downcast_ref::<FilterExec>()
        .expect("must decode back to FilterExec");
    let binary = filter
        .predicate()
        .downcast_ref::<BinaryExpr>()
        .expect("predicate must decode back to BinaryExpr");
    let tag = binary
        .left()
        .downcast_ref::<TagExpr>()
        .expect("must decode back to TagExpr");
    assert_eq!(tag.tag, "in_plan");
    assert_eq!(tag.decoded_batch_size, Some(4096));
    Ok(())
}

/// The missing-registration hint is added to whatever the codec returned, never
/// substituted for it: a codec can fail for a reason that has nothing to do
/// with a missing registration, and a caller matching on the error kind must
/// still see the kind the codec chose.
#[test]
fn the_codecs_own_error_survives_the_missing_registration_hint() -> Result<()> {
    let expr = tag_expr()?;
    let node = to_wire(&expr, &RefusingCodec)?;

    let config = SessionConfig::new()
        .with_extension(registry(register_physical_expr::<OtherExpr>)?);
    let session = SessionContext::new_with_config(config);

    let err = from_wire(&node, &session, &ExhaustedCodec)
        .expect_err("the codec must fail for this payload");
    assert!(
        matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
        "the codec's error kind must survive, got: {err:?}"
    );
    let text = err.to_string();
    assert!(
        text.contains("the decode buffer pool is empty"),
        "the codec's own message must survive: {text}"
    );
    assert!(
        text.contains("No decoder is registered") && text.contains(TagExpr::NAME),
        "the missing-registration hint must be added: {text}"
    );
    Ok(())
}
