# ExecutionPlan `try_to_proto` / `try_from_proto` migration guide (#22419)

The general part is DONE and green. You are migrating ONE plan's serialization
onto the new hook. Read the reference implementation first:

- **Reference plan:** `datafusion/physical-plan/src/projection.rs` — search for
  `try_to_proto` and `impl ProjectionExec` (the `try_from_proto` block). Copy
  its shape exactly.
- **The ctx + helpers:** `datafusion/physical-plan/src/proto.rs`
  (`ExecutionPlanEncodeCtx`, `ExecutionPlanDecodeCtx`, `expect_plan_variant!`).

## What to add (in the plan's OWN source file only)

1. **`try_to_proto`** — a `#[cfg(feature = "proto")]` override INSIDE the
   `impl ExecutionPlan for FooExec` block (NOT an inherent impl — a same-named
   inherent method is silently never called through `&dyn ExecutionPlan`):

   ```rust
   #[cfg(feature = "proto")]
   fn try_to_proto(
       &self,
       ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
   ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
       use datafusion_proto_models::protobuf;
       let input = ctx.encode_child(self.input())?;          // single child
       // let inputs = ctx.encode_children(self.inputs())?;  // N children
       // let expr = ctx.encode_expr(self.predicate())?;     // one expr
       // let exprs = ctx.encode_expressions(iter_of_&Arc<dyn PhysicalExpr>)?;
       Ok(Some(protobuf::PhysicalPlanNode {
           physical_plan_type: Some(
               protobuf::physical_plan_node::PhysicalPlanType::Foo(Box::new(
                   protobuf::FooExecNode { /* … */ },
               )),
           ),
       }))
   }
   ```

2. **`try_from_proto`** — a `#[cfg(feature = "proto")] impl FooExec { pub fn … }`
   associated fn taking the WHOLE node + the decode ctx:

   ```rust
   #[cfg(feature = "proto")]
   impl FooExec {
       pub fn try_from_proto(
           node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
           ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
       ) -> Result<Arc<dyn ExecutionPlan>> {
           use datafusion_proto_models::protobuf;
           let foo = crate::expect_plan_variant!(
               node,
               protobuf::physical_plan_node::PhysicalPlanType::Foo,
               "FooExec",
           );
           let input = ctx.decode_required_child(foo.input.as_deref(), "FooExec", "input")?;
           // exprs decode against a schema you choose (usually input.schema()):
           // let e = ctx.decode_required_expr(foo.expr.as_deref(), input.schema().as_ref(), "FooExec", "expr")?;
           Ok(Arc::new(FooExec::try_new(/* … */)?))
       }
   }
   ```

## Where the existing logic is (copy it, don't reinvent)

The current serialization for your plan lives in
`datafusion/proto/src/physical_plan/mod.rs`:
- Encode: `fn try_from_<foo>_exec(...)` — the body that builds `FooExecNode`.
- Decode: `fn try_into_<foo>_physical_plan(...)` — the body that rebuilds the plan.

Relocate that logic into the two functions above. **Keep the wire format
byte-for-byte identical.** For expr decoding, the old code calls
`proto_converter.proto_to_physical_expr(expr, some_schema, ctx)` — the direct
replacement is `ctx.decode_expr(expr, some_schema)` (it accepts ANY `&Schema`,
so join filter/left/right schemas all work).

## DO NOT

- Do NOT edit `datafusion/proto/src/physical_plan/mod.rs` — the lead wires the
  central dispatch (deleting the old encode arm + repointing the decode arm) in
  one coordinated pass. You only touch your plan's file.
- Do NOT try to migrate anything that needs the `PhysicalExtensionCodec` or the
  function registry (aggregate/window UDFs, extension nodes, scans, sinks,
  DataSourceExec, ScalarSubqueryExec). The decode ctx deliberately does NOT
  expose the codec. Those stay as typed arms. If your plan needs it — STOP and
  report it as a blocker.

## KNOWN BLOCKER — enum conversions

Conversions like `JoinType::from_proto`, `NullEquality::from_proto`,
`PartitionMode`, `JoinSide` live in `datafusion-proto` (`crate::convert`) and are
NOT reachable from `datafusion-physical-plan`. The proto enums themselves
(`datafusion_proto_models::protobuf::JoinType`, etc.) ARE reachable. So write the
conversion as an inline exhaustive `match` from the proto enum to the
`datafusion_common` enum. Example:

```rust
let join_type = match protobuf::JoinType::try_from(node.join_type)
    .map_err(|_| internal_datafusion_err!("unknown JoinType {}", node.join_type))?
{
    protobuf::JoinType::Inner => JoinType::Inner,
    // … all variants …
};
```

## Verify

Run `cargo build -p datafusion-physical-plan --features proto` and make sure it
compiles. You cannot run the roundtrip tests (they live in datafusion-proto,
which the lead wires up) — compilation of your file is your success bar.

## Report back

Return: (1) the file you edited, (2) a short summary of the two functions,
(3) ANY blocker you hit (a field needing the codec/registry, a private accessor
you needed, an enum with no reachable conversion, a wire-format subtlety like
the `[u32::MAX]` empty-projection sentinel). Do NOT spin your wheels — if
something needs the codec or a design decision, STOP and report it.
