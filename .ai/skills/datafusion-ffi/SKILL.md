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
    // Always populate this — calling through `Arc<dyn Trait>` dispatches to the
    // override if there is one, else to the trait default. The producer side
    // gets the right answer either way without the consumer needing to know.
    some_method: unsafe extern "C" fn(this: &Self, ...) -> FFI_Result<...>,

    // `Option<fn>` exists exactly once in this crate today:
    // `FFI_TableProvider::supports_filters_pushdown`, gated by the
    // `can_support_pushdown_filters: bool` argument to `FFI_TableProvider::new`.
    // Treat it as an exception, not a pattern. Default to a plain
    // `unsafe extern "C" fn` and let dynamic dispatch handle override-or-default.
    // Reach for `Option<fn>` only if you can name an explicit constructor
    // argument that toggles the slot AND there is a real reason to suppress
    // the FFI hop (e.g. consumer-side short-circuiting on a hot path).
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

The crate has **two distinct test surfaces** and a new wrapper usually needs entries in both. They are not interchangeable; they catch different classes of bug.

#### a. In-process unit tests (`#[cfg(test)] mod tests` inside `src/<mod>.rs`)

Run on every `cargo test -p datafusion-ffi`. Producer and consumer live in the same compilation unit, so `library_marker_id` returns the same value on both sides. To force the foreign path you must override the marker:

```rust
let mut ffi_x = FFI_X::new(provider, …);
ffi_x.library_marker_id = crate::mock_foreign_marker_id; // forces the ForeignX branch
let arc: Arc<dyn X> = (&ffi_x).into();
assert!(arc.downcast_ref::<ForeignX>().is_some());
```

Every wrapper must include at minimum:

- A **local-bypass test** — build `FFI_X` from a concrete native type, convert to `Arc<dyn X>`, `downcast_ref::<ConcreteType>()` must succeed.
- A **forced-foreign test** — set `library_marker_id = crate::mock_foreign_marker_id`, convert, `downcast_ref::<ForeignX>()` must succeed, then exercise every method end-to-end.

Templates: `test_ffi_table_provider_local_bypass` and `test_round_trip_ffi_table_provider_scan` in `src/table_provider.rs`.

What unit tests catch: Rust-level correctness (translation logic, lifetime bugs, leaks under valgrind/miri, Send/Sync, error propagation, codec round-trips). What they **cannot** catch: real ABI bugs. Both producer and consumer share `#[repr(C)]` layout because they are the exact same struct definition in memory.

#### b. Cross-library integration tests (`tests/ffi_*.rs`, gated by the `integration-tests` feature)

The crate is published as `crate-type = ["cdylib", "rlib"]`. The integration tests in `datafusion/ffi/tests/` use `libloading` to `dlopen` the crate's own `cdylib` and call `datafusion_ffi_get_module` — a `#[unsafe(no_mangle)] extern "C"` entry point defined in `src/tests/mod.rs` and gated by `#[cfg(feature = "integration-tests")]`. The test executable links against the rlib (consumer side); the dlopen'd cdylib is the producer side. Even though both are built from the same source, they are independent compilation outputs going through the actual FFI symbol path.

Run with:

```bash
cargo test -p datafusion-ffi --features integration-tests
```

To add coverage for a new wrapper:

1. **Add a constructor** in `src/tests/<area>.rs` (or a new file there). Return a populated `FFI_X` from a known-good native type.
2. **Wire it into `ForeignLibraryModule`** in `src/tests/mod.rs`: add a field of type `extern "C" fn(...) -> FFI_X` and populate it in `datafusion_ffi_get_module`. This struct is the cross-library contract — adding a field is itself an ABI change for the test module; integration tests will rebuild the cdylib automatically.
3. **Add the test** in `tests/ffi_<area>.rs` under `#[cfg(feature = "integration-tests")] mod tests { … }`. Call `datafusion_ffi::tests::utils::get_module()` to load the cdylib, invoke your constructor through the returned `ForeignLibraryModule`, convert into `Arc<dyn X>`, and exercise every method.

What integration tests catch that unit tests cannot:

- **Real ABI layout bugs.** Two builds means the consumer's view of `FFI_X` is reconstructed from declaration, not aliased to the producer's memory. Mismatched alignment, padding, niche optimization, or accidentally non-`#[repr(C)]` types surface here.
- **Symbol visibility / `no_mangle`** issues.
- **`library_marker_id` correctness without mocking** — the two libraries genuinely have different statics, so the foreign branch is taken for real.
- **Drop / leak ordering** when the producer side is in a `dlopen`'d image.

#### Which tests does my change need?

| Change                                                                | Unit | Integration |
| --------------------------------------------------------------------- | ---- | ----------- |
| New `FFI_X` wrapper                                                   | Yes  | Yes         |
| New method on existing `FFI_X`                                        | Yes  | Yes if the method takes/returns a non-trivial FFI type (anything other than primitives + enums). Pure-stabby-string getters can skip. |
| Bugfix to a wrapper body, no signature change                         | Yes  | Only if reproducing the bug requires cross-library symbol lookup or `dlopen` semantics |
| Layout change (`#[repr(C)]` field add/remove/reorder, fn-ptr sig)     | Yes  | **Mandatory** — this is exactly the bug class integration tests exist for |
| New `From<X> for FFI_X` or codec change                               | Yes  | Yes if the codec is exercised by the cross-library round-trip |

If you skip the integration test for a layout change, you have effectively shipped untested ABI.

## Method coverage — the silent-default gap

**This is the area where the crate currently has real holes.** When the wrapped trait has methods with *default implementations*, those defaults are typically the trait's "no-op / unsupported" answer (`None`, `false`, `Unsupported`, `not_impl_err!()`). If the producer overrides a default but the FFI struct does not carry a function pointer for it, the consumer's `Foreign<X>` falls back to the trait default — **silently losing the override**. The producer thinks it implemented `delete_from`; the consumer behaves as if it never did.

### Rule

When adding or auditing an `FFI_X`, **enumerate every method on the wrapped trait, including defaulted ones**, and for each one decide:

| Category                                                                              | Action                                                                                                                                |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Required method (no default)                                                          | Mandatory `unsafe extern "C" fn` field.                                                                                               |
| Defaulted, plausible override (statistics, distribution, ordering, simplify, DML, …)  | Mandatory `unsafe extern "C" fn` field — same as a required method. The wrapper body calls `inner.method(...)` and `Arc<dyn Trait>` dispatch picks override-or-default for free. |
| Defaulted, deprecated or vestigial                                                    | Document the skip in a `// FFI omitted: …` comment.                                                                                   |
| Defaulted, derived purely from other methods already plumbed                          | Skip — but call out the derivation in a comment.                                                                                      |

**Do not use `Option<fn>` just because the underlying trait has a default.** `Arc<dyn Trait>` erases override-vs-default info, so the producer side cannot know whether to populate the slot. Always plumb the fn pointer; the wrapper body invokes the trait method and dynamic dispatch does the right thing.

`Option<fn>` is used exactly once in the crate today: `FFI_TableProvider::supports_filters_pushdown`, gated by the `can_support_pushdown_filters: bool` argument to `FFI_TableProvider::new`. It is an exception, not a template. Reach for it only when (a) the producer's constructor takes an explicit capability flag and (b) skipping the FFI call is meaningfully cheaper than letting the trait default run on the producer side. Otherwise plumb the fn pointer unconditionally.

### Known gaps to close

A re-audit (2026-05) opened tracking issues for each wrapper with real gaps. Treat new PRs in these areas as opportunities to fix them; treat new wrappers as required to avoid creating more. Each linked issue lists the specific missing methods and severity.

- **`FFI_TableProvider`** — [#22328](https://github.com/apache/datafusion/issues/22328). Missing: `constraints`, `get_table_definition`, `get_logical_plan`, `get_column_default`, `scan_with_args`, `statistics`, `delete_from`, `update`, `truncate`. DML omissions are the worst — they silently demote a writable table to read-only on the foreign side.
- **`FFI_ExecutionPlan`** — [#22329](https://github.com/apache/datafusion/issues/22329). Largest single gap by method count: 18 defaults (distribution / ordering, filter pushdown, projection swapping, limit pushdown, cardinality, partition stats, state / fetch handling). Silently breaks optimizer decisions on the consumer side.
- **`FFI_ScalarUDF`** — [#22330](https://github.com/apache/datafusion/issues/22330). Missing: `display_name`, `schema_name`, `with_updated_config`, `simplify`, `preimage`, `conditional_arguments`, `evaluate_bounds`, `propagate_constraints`, `struct_field_mapping`, `output_ordering`, `preserves_lex_ordering`, `placement`, `documentation`. Optimizer hooks + SQL-output naming silently disabled.
- **`FFI_AggregateUDF`** — [#22331](https://github.com/apache/datafusion/issues/22331). Missing: `display_name`, `schema_name`, `human_display`, `window_function_schema_name`, `window_function_display_name`, `simplify`, `simplify_expr_op_literal`, `reverse_udf` / `reverse_expr`, `is_descending`, `value_from_stats`, `default_value`, `supports_null_handling_clause`, `supports_within_group_clause`, `set_monotonicity`, `documentation`. Statistics-driven shortcuts (`value_from_stats`) and SQL surface area (null-handling / within-group clauses) silently absent.
- **`FFI_WindowUDF`** — [#22332](https://github.com/apache/datafusion/issues/22332). Missing: `simplify`, `expressions`, `reverse_expr`, `documentation`. `expressions()` selects which physical args reach the evaluator — silent fallback can change semantics.
- **`FFI_SchemaProvider`** — [#22333](https://github.com/apache/datafusion/issues/22333). Missing: `table_type` async cheap-path. Forces full `table()` materialization for `information_schema.tables` probes. (`owner_name` is exposed as a static `FFI_Option<SString>` snapshot field — note for future review, not a gap today.)
- **`FFI_PhysicalOptimizerRule`** — [#22334](https://github.com/apache/datafusion/issues/22334). Missing: `optimize_with_context`. Rules whose output depends on per-invocation statistics / options are silently downgraded to the context-free path.
- **`FFI_GroupsAccumulator`** — [#22335](https://github.com/apache/datafusion/issues/22335). Missing: `size`. Consumer-side memory accounting reports trait default rather than producer's real footprint.
- **`FFI_PartitionEvaluator`** — [#22336](https://github.com/apache/datafusion/issues/22336). Missing: `memoize`. Performance regression for memoizing evaluators; correctness unaffected.
- **`FFI_RecordBatchStream`** — [#22337](https://github.com/apache/datafusion/issues/22337). Open question: no `library_marker_id` field, unlike most other `FFI_X`. Confirm intentional vs gap; if intentional, document the rationale and update this skill.

Wrappers not listed above are not certified complete — they are merely not currently tracked. Re-enumerate the trait surface (see "How to enumerate defaults" below) whenever you audit one; upstream trait drift can introduce new defaulted methods at any time and silently re-open the silent-override-loss bug class.

When *closing* a gap, add the fn pointer unconditionally — the wrapper body calls the trait method on the inner `Arc<dyn Trait>` and Rust's dynamic dispatch picks the producer's override or falls back to the default. Use `Option<fn>` only when the new method also gains a corresponding capability flag on the producer's `new()`. Either way the layout changes, so the PR is an ABI break: mark `api-change`, do not back-port to `branch-<major>`, and the workspace major bump in the next release makes the `version()` extern surface the change to consumers at load time.

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
10. **Unit tests** include both local-bypass and `mock_foreign_marker_id` forced-foreign cases? **Integration tests** in `tests/ffi_*.rs` exist for any wrapper that takes/returns non-trivial FFI types, and for *every* layout change? `cargo test -p datafusion-ffi --features integration-tests` must pass before merging an ABI-affecting PR.
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
