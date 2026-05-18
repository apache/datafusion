---
name: datafusion-ffi
description: Patterns and review checklist for the `datafusion-ffi` crate. Use whenever the user adds, edits, or reviews code under `datafusion/ffi/` — new `FFI_X` wrappers, `Foreign<X>` impls, codec changes, or expanding an existing wrapper to cover more of a trait's surface. Also use when reviewing PRs that touch this crate.
---

# DataFusion FFI Skill

This crate exposes a stable C ABI for DataFusion traits so that independently-compiled libraries (different Rust versions, plugins, `datafusion-python`, etc.) can interoperate at runtime. Stability and correctness here are load-bearing: a missed pattern can cause segfaults, leaks, or silently dropped trait behavior on the consumer side.

Read the crate's `README.md` first if you have not — it establishes the vocabulary (`FFI_X` / `ForeignX`, `library_marker_id`, `release`, `TaskContextProvider`, stabby vs `#[repr(C)]`).

## When to use

Trigger this skill any time the work touches `datafusion/ffi/`:

- Adding a new `FFI_<Trait>` + `Foreign<Trait>` pair
- Adding a method to an existing `FFI_X` struct
- Reviewing a PR that touches this crate
- Changing the codec / proto serialization layer
- Bumping the wrapped DataFusion trait surface (e.g. a new default method appeared upstream)

## Hard rules

1. **No `datafusion` dependency.** `datafusion-ffi` must not depend on the umbrella `datafusion` crate. Use the leaf crates (`datafusion-common`, `datafusion-expr`, `datafusion-catalog`, `datafusion-physical-plan`, etc.). `datafusion` is fine in `[dev-dependencies]`.
2. **`#[repr(C)]` on every `FFI_X` struct**, not `#[stabby::stabby]`. Stabby is used for `SString`/`SVec` only. Reasons documented in the README (build time, Arrow types lack `IStable`).
3. **`unsafe extern "C"` on every function pointer.** Plain `extern "C"` is reserved for `version` and `library_marker_id`.
4. **`unsafe impl Send` + `unsafe impl Sync`** for every `FFI_X` and every `ForeignX` — the raw pointer makes the struct `!Send`/`!Sync` by default.
5. **`#![deny(clippy::clone_on_ref_ptr)]`** is on at the crate root. Use `Arc::clone(&x)`, never `x.clone()` on `Arc`.
6. **Run before pushing:** `cargo fmt --all`, `cargo clippy -p datafusion-ffi --all-targets --all-features -- -D warnings`, `cargo test -p datafusion-ffi`.
7. **`api-change` label required.** Any PR that modifies an `FFI_X` struct layout (adds/removes/reorders fields, changes a function-pointer signature, adds a variant to an FFI enum, or changes the `version` extern) must carry the `api-change` GitHub label. Layout changes break ABI for already-compiled consumer libraries. The label drives release-note flagging so downstream users (e.g. `datafusion-python`, plugin authors) know they must recompile against the new DataFusion major. The `version()` extern in `src/lib.rs` returns the major of workspace `CARGO_PKG_VERSION`; consumers compare it at load time and can refuse mismatched producers. Apply via `gh pr edit <num> --add-label api-change`. When reviewing such a PR, block merge until label present.
8. **No FFI struct changes in patch releases.** Patch releases ship from branches named `branch-<major>` (e.g. `branch-53`, `branch-52`). FFI struct layout changes (anything that would earn rule 7's `api-change` label) **must not** target a `branch-<major>` branch and must not be back-ported. Patch releases are ABI-stable by contract — a consumer compiled against `53.1.0` must keep working against `53.1.1`. Before reviewing/approving an FFI PR, check the PR's base branch: if it matches `branch-*` or the PR description / labels indicate patch / back-port, reject the FFI struct change and ask the author to retarget `main`. Bugfixes that do not alter struct layout (e.g. fixing a function-pointer body) are fine to back-port. Quick check: `gh pr view <num> --json baseRefName,labels`.

## The standard wrapper shape

A new `FFI_X` for trait `X` must follow this template. Use `FFI_TableProvider` (src/table_provider.rs) and `FFI_CatalogProvider` (src/catalog_provider.rs) as canonical references.

### 1. The `FFI_X` struct

```rust
#[repr(C)]
#[derive(Debug)]
pub struct FFI_X {
    // One unsafe extern "C" fn per trait method (see § "Method coverage" below).
    some_method: unsafe extern "C" fn(this: &Self, ...) -> FFI_Result<...>,

    // Optional capability — None when the producer does not implement it.
    optional_method: Option<unsafe extern "C" fn(&Self, ...) -> FFI_Result<...>>,

    // Codec only if the trait moves Exprs/Plans across the boundary.
    pub logical_codec: FFI_LogicalExtensionCodec,

    clone: unsafe extern "C" fn(&Self) -> Self,
    release: unsafe extern "C" fn(&mut Self),
    pub version: unsafe extern "C" fn() -> u64,

    private_data: *mut c_void,
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_X {}
unsafe impl Sync for FFI_X {}
```

Notes:

- Method function pointers are private by default. Mark `pub` only if a downstream library needs to invoke them directly (rare — usually only `version`, `library_marker_id`, embedded codecs are public).
- `version: super::version` is mandatory. Consumers gate compatibility on it.
- `library_marker_id: crate::get_library_marker_id` is mandatory. It powers local-bypass.

### 2. `PrivateData` shape

Default — for read-only, shareable traits — use `Arc`:

```rust
struct XPrivateData {
    inner: Arc<dyn X>,
    runtime: Option<Handle>, // include when any async method exists
}
```

For traits that require `&mut self` (e.g. `Accumulator`), use `Box<dyn X>`. A `Box`-backed `FFI_X` **cannot implement `Clone`**; document this and skip the `clone` function pointer or hand-write a release path that distinguishes producer vs consumer side (see `FFI_Accumulator` in `src/udaf/accumulator.rs`).

### 3. Function-pointer wrappers

Naming convention: `<method>_fn_wrapper`.

```rust
unsafe extern "C" fn some_method_fn_wrapper(this: &FFI_X, ...) -> FFI_Result<T> {
    // 1. Recover inner via this.inner()
    // 2. Translate FFI types → native types
    // 3. Call native method
    // 4. Translate native Result → FFI_Result via sresult_return! or .into()
}
```

### 4. `clone` / `release` / `Drop`

```rust
unsafe extern "C" fn clone_fn_wrapper(this: &FFI_X) -> FFI_X { /* re-Box new private_data, copy fn ptrs */ }

unsafe extern "C" fn release_fn_wrapper(this: &mut FFI_X) {
    unsafe {
        debug_assert!(!this.private_data.is_null());
        drop(Box::from_raw(this.private_data as *mut XPrivateData));
        this.private_data = std::ptr::null_mut();
    }
}

impl Drop for FFI_X { fn drop(&mut self) { unsafe { (self.release)(self) } } }
impl Clone for FFI_X { fn clone(&self) -> Self { unsafe { (self.clone)(self) } } }
```

`release` must null `private_data` so a double-free debug-asserts loudly.

### 5. Constructor split

```rust
impl FFI_X {
    pub fn new(inner: Arc<dyn X>, runtime: Option<Handle>,
               task_ctx_provider: impl Into<FFI_TaskContextProvider>,
               logical_codec: Option<Arc<dyn LogicalExtensionCodec>>) -> Self {
        // build FFI_LogicalExtensionCodec from defaults, then forward
        Self::new_with_ffi_codec(inner, runtime, ffi_codec)
    }

    pub fn new_with_ffi_codec(inner: Arc<dyn X>, runtime: Option<Handle>,
                              logical_codec: FFI_LogicalExtensionCodec) -> Self {
        // Round-trip downcast: if inner is already a ForeignX, return its FFI directly.
        if let Some(foreign) = inner.downcast_ref::<ForeignX>() {
            return foreign.0.clone();
        }
        // …allocate XPrivateData and populate fn ptrs…
    }
}
```

The round-trip downcast is **mandatory** — without it, repeated FFI hops nest `ForeignX(FFI_X(ForeignX(...)))` and you pay the boundary cost every layer.

### 6. The `Foreign<X>` consumer

```rust
#[derive(Debug)]
pub struct ForeignX(pub FFI_X);
unsafe impl Send for ForeignX {}
unsafe impl Sync for ForeignX {}

impl From<&FFI_X> for Arc<dyn X> {
    fn from(p: &FFI_X) -> Self {
        if (p.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(unsafe { p.inner() })
        } else {
            Arc::new(ForeignX(p.clone()))
        }
    }
}

impl X for ForeignX { /* call each fn pointer, translate types back */ }
```

The marker-id check is **mandatory** for every `From<&FFI_X> for Arc<dyn X>`. Skipping it breaks the local-bypass optimization and forces the producer's data through serialization.

### 7. Tests

Every wrapper must include both:

- A **local-bypass test**: build `FFI_X` from a concrete native type, convert to `Arc<dyn X>`, and `downcast_ref::<ConcreteType>()` must succeed.
- A **forced-foreign test**: set `ffi_x.library_marker_id = crate::mock_foreign_marker_id`, convert to `Arc<dyn X>`, and `downcast_ref::<ForeignX>()` must succeed. Then exercise every method end-to-end.

See `test_ffi_table_provider_local_bypass` and `test_round_trip_ffi_table_provider_scan` in `src/table_provider.rs` for the template.

## Method coverage — the silent-default gap

**This is the area where the crate currently has real holes.** When the wrapped trait has methods with *default implementations*, those defaults are typically the trait's "no-op / unsupported" answer (`None`, `false`, `Unsupported`, `not_impl_err!()`). If the producer overrides a default but the FFI struct does not carry a function pointer for it, the consumer's `Foreign<X>` falls back to the trait default — **silently losing the override**. The producer thinks it implemented `delete_from`; the consumer behaves as if it never did.

### Rule

When adding or auditing an `FFI_X`, **enumerate every method on the wrapped trait, including defaulted ones**, and for each one decide:

| Category                                                                              | Action                                                          |
| ------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| Required method (no default)                                                          | Mandatory `unsafe extern "C" fn` field.                         |
| Defaulted, plausible override (statistics, distribution, ordering, simplify, DML, …)  | Add it. Use `Option<fn>` so older producers can ship `None`.    |
| Defaulted, deprecated or vestigial                                                    | Document the skip in a `// FFI omitted: …` comment.             |
| Defaulted, derived purely from other methods already plumbed                          | Skip — but call out the derivation in a comment.                |

For `Option<fn>` fields, the producer constructor takes a `bool` (or a typed enum) that decides whether to populate the slot; the consumer impl checks `match self.0.opt_fn { Some(f) => ..., None => trait_default }`. `FFI_TableProvider::supports_filters_pushdown` is the reference.

### Known gaps to close

A current audit (2026-05) flags these missing-default forwardings. Treat new PRs in these areas as opportunities to fix them; treat new wrappers as required to avoid creating more:

- **`FFI_TableProvider`** — missing: `constraints`, `get_table_definition`, `get_logical_plan`, `get_column_default`, `statistics`, `scan_with_args`, `delete_from`, `update`, `truncate`. DML omissions are the worst — they silently demote a writable table to read-only on the foreign side.
- **`FFI_ExecutionPlan`** — missing: `required_input_distribution`, `required_input_ordering`, `maintains_input_order`, `benefits_from_input_partitioning`, `check_invariants`, `apply_expressions`, `reset_state`, `with_fetch`, `fetch`, `supports_limit_pushdown`, `cardinality_effect`, `partition_statistics`. These break optimizer decisions on the consumer side.
- **`FFI_ScalarUDF` / `FFI_AggregateUDF` / `FFI_WindowUDF`** — missing: `aliases`, `display_name`, `schema_name`, `with_updated_config`, `simplify`, `preimage`, `short_circuits`, `evaluate_bounds` (scalar); `ordering_fields`, `group_ordering`, `order_sensitivity` (aggregate). Aliases in particular are a frequent silent-loss surprise.
- **`FFI_CatalogProvider`** — `register_schema` / `deregister_schema` are exposed, good; verify any future `CatalogProvider` defaults get the same treatment.
- **`FFI_SchemaProvider`** — missing: `owner_name`, `table_type` (cheap-path), `register_table`, `deregister_table`.
- **`FFI_Accumulator`** — missing: `retract_batch` + `supports_retract_batch` exposure, which breaks window aggregates that opt in to retraction.

When *closing* a gap, prefer `Option<fn>` over an unconditional fn pointer. Even an `Option<fn>` addition reorders the struct layout — still an ABI break. Mark `api-change`; consumers detect via the `version()` extern at load time and must recompile against the new DataFusion major.

### How to enumerate defaults

For trait `X`:

```bash
# Find the trait definition
grep -rn "pub trait X" datafusion/ --include='*.rs'

# Inspect for `fn method(...) { default_body }` (the body marks it as a default)
```

If a method's body is non-trivial, the consumer-side default is non-trivial too. Decide explicitly whether the FFI should let the consumer recompute the same default, or whether the producer's override is what should travel.

## Type-bridging conventions

- **Strings / vecs** crossing the boundary: `stabby::string::String as SString`, `stabby::vec::Vec as SVec`. Native conversion is `Vec::into_iter().collect::<SVec<_>>()` and back.
- **Optional / fallible** values: use this crate's `FFI_Option<T>` and `FFI_Result<T>` (`src/ffi_option.rs`), *not* stabby's, because ours do not require `T: IStable`.
- **Schema / arrays**: `WrappedSchema` (`src/arrow_wrappers.rs`) wraps `FFI_ArrowSchema`. Never expose `FFI_ArrowSchema` directly.
- **Logical `Expr` / `LogicalPlan`**: serialize via `datafusion-proto` using the embedded `FFI_LogicalExtensionCodec`. Same for physical plans → `FFI_PhysicalExtensionCodec`.
- **Enums** (`Volatility`, `TableType`, `InsertOp`, `TableProviderFilterPushDown`): `#[repr(u8)]`, with `From<Native> for FFI_X` and `From<&FFI_X> for Native`. Always write a round-trip unit test that exercises every variant.
- **Errors**: every `FFI_X` method that can fail returns `FFI_Result<T>`. Use the `sresult!`, `sresult_return!`, `df_result!` macros from `src/util.rs` — do not roll your own.

## Async, sessions, and task context

- Any async method becomes `unsafe extern "C" fn(...) -> FfiFuture<FFI_Result<T>>`. The wrapper body uses `async move { ... }.into_ffi()`. Store `Option<tokio::runtime::Handle>` in `PrivateData` so the producer side can re-enter its own runtime if needed.
- Methods taking `&dyn Session` cross the boundary as `FFI_SessionRef`. On the consumer side, try `session.as_local()` first; only construct a `ForeignSession::try_from(&session)` if that returns `None`. See `scan_fn_wrapper` in `src/table_provider.rs`.
- Anything that needs to deserialize an `Expr` / `LogicalPlan` needs a `TaskContext`. Threading a fresh `TaskContext` per call is wrong because new UDFs may have been registered since construction. Use `FFI_TaskContextProvider` (`src/execution/task_ctx_provider.rs`), which holds a `Weak` ref to a `TaskContextProvider`. If the weak ref is dead at call time, return a clear error — do not panic.

## Memory model checklist for every new `FFI_X`

- [ ] `private_data` is `Box::into_raw`-ed exactly once at construction.
- [ ] Every constructor path (including `clone_fn_wrapper`) allocates a fresh `Box` for its own `private_data`.
- [ ] `release_fn_wrapper` `Box::from_raw`s it and nulls the pointer.
- [ ] `Drop` calls `release`.
- [ ] No method touches `private_data` directly outside the producer side. Consumer-side methods on `ForeignX` use only the function pointers.
- [ ] `library_marker_id` and `version` are populated in **every** constructor (including `clone`).
- [ ] No method dereferences a pointer it did not check for nullness (debug-assert at minimum).

## Quick PR-review checklist

When reviewing a PR that touches `datafusion/ffi/`:

1. **Trait coverage.** Pull up the underlying trait. List its methods. Confirm every non-defaulted method has a function pointer. For each *defaulted* method, ask whether a real-world producer would override it — if yes, the PR must either plumb it through (preferably as `Option<fn>`) or explicitly justify the omission in a comment.
2. **Layout fields.** `clone`, `release`, `version`, `private_data`, `library_marker_id` all present?
3. **Marker-id bypass** in `From<&FFI_X>`?
4. **Round-trip downcast** in the constructor?
5. **`Drop` calling `release`**, and `release` nulling the pointer?
6. **`Send` + `Sync` unsafe impls** on both `FFI_X` and `ForeignX`?
7. **Stabby types** for strings/vecs; crate-local `FFI_Option`/`FFI_Result` for optional/fallible payloads?
8. **Async** uses `FfiFuture` + `.into_ffi()`, never blocking?
9. **Codec** present on any method that ships an `Expr` / plan?
10. **Tests** include both local-bypass and `mock_foreign_marker_id` forced-foreign cases?
11. **No `datafusion` runtime dep** crept into `Cargo.toml`?
12. **`Arc::clone(&x)`** everywhere — no implicit `x.clone()` on `Arc` (the lint will reject it but worth pre-flagging).
13. **`cargo clippy --all-targets --all-features -- -D warnings`** clean on the crate?
14. **`api-change` label** on the PR if any `FFI_X` struct layout / fn-ptr signature / FFI enum / `version` extern changed? Block merge until applied.
15. **Base branch check.** If FFI struct layout changed, base branch must be `main`, never `branch-<major>`. Reject back-ports of ABI-breaking changes to patch-release branches. `gh pr view <num> --json baseRefName,labels` to verify.

## References

- Crate README: `datafusion/ffi/README.md` — vocabulary + memory-model rationale.
- Canonical wrappers to model after: `src/table_provider.rs`, `src/catalog_provider.rs`.
- Mutable-trait variant: `src/udaf/accumulator.rs` (`Box<dyn Accumulator>`).
- Optional-method pattern: `FFI_TableProvider::supports_filters_pushdown`.
- Codec wiring: `src/proto/logical_extension_codec.rs`, `src/proto/physical_extension_codec.rs`.
- Examples crate: `datafusion-examples/examples/ffi` (end-to-end producer + consumer).
