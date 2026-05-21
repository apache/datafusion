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
3. **`unsafe extern "C"` on every function-pointer field — including `version`.** The one exception is the `library_marker_id` field, which is plain `extern "C" fn() -> usize`. Plain (safe) `extern "C"` also applies to the standalone function defs in `src/lib.rs` — `pub extern "C" fn version()` and `pub extern "C" fn get_library_marker_id()` — which coerce into the `unsafe extern "C"` `version` field slot at construction.
4. **Match `Send`/`Sync` to the wrapped trait.** Raw `*mut c_void` makes every `FFI_X` `!Send + !Sync` by default — `unsafe impl` whichever bounds the consumer-facing trait requires. Most DataFusion traits (`TableProvider`, `ExecutionPlan`, all UDFs, codecs) need both. `Send`-only: `RecordBatchStream`, `Accumulator`, `GroupsAccumulator`, `PartitionEvaluator` (mutable / stream APIs). The matching `ForeignX` always carries the same bounds — pick consistently.
5. **`#![deny(clippy::clone_on_ref_ptr)]`** is on at the crate root. Use `Arc::clone(&x)`, never `x.clone()` on `Arc`.
6. **Run before pushing:** `cargo fmt --all`, `cargo clippy -p datafusion-ffi --all-targets --all-features -- -D warnings`, `cargo test -p datafusion-ffi`.
7. **`api change` label required.** Any PR that modifies an `FFI_X` struct layout (adds/removes/reorders fields, changes a function-pointer signature, adds a variant to an FFI enum, or changes the `version` extern) must carry the `api change` GitHub label. Layout changes break ABI for already-compiled consumer libraries. The label is the project-wide convention for highlighting breaking public-API changes in release notes — see `docs/source/contributor-guide/api-health.md` §"What to do when making breaking API changes?" (step 1 names the label explicitly). Downstream users (e.g. `datafusion-python`, plugin authors) read the labelled notes to know they must recompile against the new DataFusion major. The `version()` extern in `src/lib.rs` returns the major of workspace `CARGO_PKG_VERSION`; consumers compare it at load time and can refuse mismatched producers. Apply via `gh pr edit <num> --add-label "api change"` (label name contains a space — must be quoted). When reviewing such a PR, block merge until label present.
8. **No FFI struct changes in patch releases.** Patch releases ship from branches matching `^branch-\d+$` (e.g. `branch-53`, `branch-52`). FFI struct layout changes (anything that would earn rule 7's `api change` label) **must not** target a release branch and must not be back-ported. Patch releases are ABI-stable by contract — a consumer compiled against `53.1.0` must keep working against `53.1.1`. Before reviewing/approving an FFI PR, check the PR's base branch: if it matches the regex above, or the PR description / labels indicate patch / back-port, reject the FFI struct change and ask the author to retarget `main`. Bugfixes that do not alter struct layout (e.g. fixing a function-pointer body) are fine to back-port. Quick check: `gh pr view <num> --json baseRefName,labels --jq '.baseRefName'` then match against `^branch-\d+$`. Do **not** glob-match `branch-*` — back-port / cherry-pick working branches (e.g. `branch-53-cherry-pick-1`) also share that prefix but are not release branches; only the strict `branch-<digits>` form is the freeze target.

## The standard wrapper shape

A new `FFI_X` for trait `X` must follow this template. Use `FFI_CatalogProvider` (`src/catalog_provider.rs`) as the canonical reference — it shows the full shape (codec field, nested FFI types, `FFI_Option`/`FFI_Result` returns, Arc-backed `PrivateData`) without async or capability-flag noise. `FFI_TableProvider` (`src/table_provider.rs`) covers async (`scan`, `FFI_SessionRef`, `FfiFuture`) and the one `Option<fn>` capability flag (`supports_filters_pushdown`).

### 1. The `FFI_X` struct

```rust
#[repr(C)]
#[derive(Debug)]
pub struct FFI_X {
    some_method: unsafe extern "C" fn(this: &Self, ...) -> FFI_Result<...>,
    optional_method: Option<unsafe extern "C" fn(&Self, ...) -> FFI_Result<...>>,
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

Field rules:

- **One `unsafe extern "C" fn` per trait method.** Always populate — `Arc<dyn Trait>` dispatch picks override-or-default at call time, so the producer side gets the right answer without the consumer needing to know. See § "Method coverage".
- **`Option<fn>` is the capability-flag exception**, not a template. Crate uses it exactly once: `FFI_TableProvider::supports_filters_pushdown`. See § "Method coverage".
- **Codec field** (`FFI_LogicalExtensionCodec` / `FFI_PhysicalExtensionCodec`) only if the trait moves `Expr`s / `LogicalPlan`s / `ExecutionPlan`s across the boundary.
- **Method function pointers are private by default.** Mark `pub` only if a downstream library needs to invoke them directly (rare — typically only `version`, `library_marker_id`, embedded codecs are `pub`).
- **`version: super::version` is mandatory.** Consumers gate compatibility on it.
- **`library_marker_id: crate::get_library_marker_id` is mandatory *when the wrapper uses the standard `ForeignX` adapter pattern*.** Two flavors exist:
  - **Arc-backed (immutable / shareable traits — `TableProvider`, `ExecutionPlan`, all UDFs, codecs):** consumer-side `From<&FFI_X> for Arc<dyn X>` consults the marker to choose `Arc::clone(inner)` vs `Arc::new(ForeignX(...))`.
  - **Box-backed (mutable / move-only traits — `Accumulator`, `GroupsAccumulator`, `PartitionEvaluator`):** consumer-side `From<FFI_X> for Box<dyn X>` consults the marker to take the inner `Box` directly vs `Box::new(ForeignX(...))`. Producer-side `From<Box<dyn X>> for FFI_X` *also* uses an `is::<ForeignX>()` downcast as an additional re-wrap bypass; the marker check covers the reverse direction.

  The field is dead ABI surface — and must be omitted with a one-line module-doc rationale — only when **neither** flavor applies: no `ForeignX` adapter, no reverse `From<FFI_X> for {Arc,Box}<dyn X>`, and the trait is impl'd directly on `FFI_X`. Canonical example: `FFI_RecordBatchStream` (`impl RecordBatchStream for FFI_RecordBatchStream` at `record_batch_stream.rs:149`, no `ForeignRecordBatchStream`, no reverse `From`).

  Before flagging a missing `library_marker_id` as a gap, run all three greps on the wrapper file: `Foreign<wrapper-name>`, `From<&?FFI_X> for Arc<`, `From<FFI_X> for Box<`. Hit on any → marker is required and its absence is a real gap. Zero hits on all three → marker is intentionally not needed, not a gap.
- **`Send`/`Sync` bounds match the wrapped trait** (rule 4). Most wrappers want both; `Send`-only for streams / mutable traits.

### 2. `PrivateData` shape

Default — for read-only, shareable traits — use `Arc`:

```rust
struct XPrivateData {
    inner: Arc<dyn X>,
    runtime: Option<Handle>, // include when any async method exists
}
```

For traits that require `&mut self` (e.g. `Accumulator`, `GroupsAccumulator`, `PartitionEvaluator`), use `Box<dyn X>`:

```rust
struct XPrivateData {
    inner: Box<dyn X>,
    runtime: Option<Handle>, // include when any async method exists
}
```

A `Box`-backed `FFI_X` **cannot implement `Clone`**; document this and skip the `clone` function pointer, or hand-write a release path that distinguishes producer vs consumer side. Canonical example: `FFI_Accumulator` in `src/udaf/accumulator.rs`. See also `FFI_GroupsAccumulator` (`src/udaf/groups_accumulator.rs`) and `FFI_PartitionEvaluator` (`src/udwf/partition_evaluator.rs`).

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
| New method on existing `FFI_X`                                        | Yes  | Yes if the method takes/returns a non-trivial FFI type. See note below. |
| Bugfix to a wrapper body, no signature change                         | Yes  | Only if reproducing the bug requires cross-library symbol lookup or `dlopen` semantics |
| Layout change (`#[repr(C)]` field add/remove/reorder, fn-ptr sig)     | Yes  | **Mandatory** — this is exactly the bug class integration tests exist for |
| New `From<X> for FFI_X` or codec change                               | Yes  | Yes if the codec is exercised by the cross-library round-trip |

**"Non-trivial FFI type" for the table above** — anything other than:

- Primitives (`u8`/`u64`/`bool`/`usize`, etc.) and `#[repr(u8)]` FFI enums (`FFI_TableType`, `Volatility`, `InsertOp`, `TableProviderFilterPushDown`).
- A `stabby::string::String` (`SString`) returned by value, with no other args or returns.

Concrete skippable example: `fn name(&self) -> SString` reading a field already validated by another method. Concrete *non*-skippable examples: anything returning `SVec<T>`, `FFI_Option<T>`, `FFI_Result<T>`, `WrappedSchema`, `WrappedArray`, an `FfiFuture`, an `FFI_*` sub-struct, or any `*mut`/`*const` pointer — those exercise alignment / padding / niche-opt across the ABI boundary and need the two-build coverage. When unsure, write the integration test; the cost is one constructor + ~20 lines.

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

Tracked gaps live on GitHub under the [`ffi`](https://github.com/apache/datafusion/issues?q=is%3Aissue+is%3Aopen+label%3Affi) label — that query is the source of truth and stays current as issues are filed or closed. Treat new PRs in those areas as opportunities to fix the listed methods; treat new wrappers as required to avoid creating more. Each open issue names the specific wrapper, the missing methods, and the severity.

Quick CLI list:

```bash
gh issue list --repo apache/datafusion --label ffi --state open --limit 50
```

Common severity classes seen on the label today:

- **DML / optimizer-relevant defaults silently lost** — e.g. `delete_from`/`update`/`truncate` on table providers, distribution / ordering / pushdown on execution plans, `value_from_stats` on aggregates. These demote producer capability on the foreign side.
- **SQL surface area silently absent** — naming hooks (`display_name`, `schema_name`, `documentation`), null-handling and within-group clauses on UDAFs, etc.
- **Performance regressions, not correctness** — e.g. `memoize` on partition evaluators.
- **Open design questions** — none currently tracked. (Historical entry: whether `FFI_RecordBatchStream` needs `library_marker_id` — resolved no, it impls the trait directly on `FFI_X` with no `Foreign` adapter and no reverse `From`, so the marker has no consultation site. See `library_marker_id` rule in §"Method coverage".)

When in doubt, open the label query; do not assume the list above is exhaustive. Wrappers without an open issue are **not** certified complete — re-enumerate the trait surface (see "How to audit a wrapper's coverage" below) whenever you audit one; upstream trait drift can introduce new defaulted methods at any time and silently re-open the silent-override-loss bug class.

Conversely, an open issue under the `ffi` label is a **claim of a gap, not proof of one.** Past audits have filed false positives by enumerating gaps from memory rather than from current source (e.g. #22335 claimed `size` missing on `FFI_GroupsAccumulator` when it had been plumbed since PR #14775). Before acting on a listed gap — opening a fix PR, re-citing it in a new audit, or extending the list — run the dual-grep audit below and confirm the field is actually absent. If the issue is a false positive, close it as `not planned`, link the `file:line` of the existing plumbing, and remove the corresponding bullet from this skill.

When *closing* a gap, add the fn pointer unconditionally — the wrapper body calls the trait method on the inner `Arc<dyn Trait>` and Rust's dynamic dispatch picks the producer's override or falls back to the default. Use `Option<fn>` only when the new method also gains a corresponding capability flag on the producer's `new()`. Either way the layout changes, so the PR is an ABI break: mark `api change`, do not back-port to `branch-<major>`, and the workspace major bump in the next release makes the `version()` extern surface the change to consumers at load time.

### How to audit a wrapper's coverage

Every audit — opening an issue, filing a fix PR, or re-confirming a listed gap — must compare two sides drawn from current source. Never enumerate either side from memory or from a prior audit; trait surface and FFI struct both drift.

**Side A — trait defaults (what could go missing):**

```bash
# Find the trait definition
grep -rn "pub trait X" datafusion/ --include='*.rs'

# Inspect for `fn method(...) { default_body }` — the body marks it as a default
```

**Side B — FFI wrapper coverage (what is already plumbed):**

```bash
# List every fn-pointer field on the FFI struct
grep -nE 'pub [a-z_]+: (unsafe )?extern "C" fn' datafusion/ffi/src/.../X.rs
```

Diff Side A against Side B. Any claim of a gap — in an issue body, audit summary, or PR description — must cite `file:line` for **both** the trait default and the FFI struct line where the field is (or is not). An issue body with only one side cited is incomplete and likely a false positive; reject it pending a re-grep.

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

1. **Trait coverage.** Pull up the underlying trait. List its methods. Confirm every non-defaulted method has a function pointer. For each *defaulted* method, ask whether a real-world producer would override it — if yes, the PR must either plumb it through as a plain `unsafe extern "C" fn` (let dynamic dispatch on `Arc<dyn Trait>` pick override-or-default) or explicitly justify the omission in a comment. Reserve `Option<fn>` for the capability-flag pattern described in §"Method coverage" — do not use it just because the trait has a default.
2. **Layout fields.** `clone`, `release`, `version`, `private_data` all present? `library_marker_id` present **iff** wrapper has a `ForeignX` adapter and a reverse `From<&?FFI_X> for {Arc,Box}<dyn X>` consultation site (Arc-backed for shareable traits, Box-backed for `&mut self` traits); if the trait is impl'd directly on `FFI_X` (no `ForeignX`, no reverse `From`), `library_marker_id` is dead surface and must be omitted with a one-line rationale.
3. **Marker-id bypass** in `From<&FFI_X>` (where applicable per rule 2)?
4. **Round-trip downcast** in the constructor?
5. **`Drop` calling `release`**, and `release` nulling the pointer?
6. **`Send`/`Sync` unsafe impls** match the wrapped trait's bounds (rule 4), and `FFI_X` + `ForeignX` agree?
7. **Stabby types** for strings/vecs; crate-local `FFI_Option`/`FFI_Result` for optional/fallible payloads?
8. **Async** uses `FfiFuture` + `.into_ffi()`, never blocking?
9. **Codec** present on any method that ships an `Expr` / plan?
10. **Unit tests** include both local-bypass and `mock_foreign_marker_id` forced-foreign cases? **Integration tests** in `tests/ffi_*.rs` exist for any wrapper that takes/returns non-trivial FFI types, and for *every* layout change? `cargo test -p datafusion-ffi --features integration-tests` must pass before merging an ABI-affecting PR.
11. **No `datafusion` runtime dep** crept into `Cargo.toml`?
12. **`Arc::clone(&x)`** everywhere — no implicit `x.clone()` on `Arc` (the lint will reject it but worth pre-flagging).
13. **`cargo clippy --all-targets --all-features -- -D warnings`** clean on the crate?
14. **`api change` label** on the PR if any `FFI_X` struct layout / fn-ptr signature / FFI enum / `version` extern changed? Block merge until applied.
15. **Base branch check.** If FFI struct layout changed, base branch must be `main`, never a release branch matching `^branch-\d+$` (e.g. `branch-53`). Reject back-ports of ABI-breaking changes to patch-release branches. Verify with `gh pr view <num> --json baseRefName,labels --jq '.baseRefName'`; match strictly against `^branch-\d+$` — cherry-pick working branches like `branch-53-cherry-pick-1` also start with `branch-` and must not false-positive.

## References

- Crate README: `datafusion/ffi/README.md` — vocabulary + memory-model rationale.
- Canonical wrapper to model after: `src/catalog_provider.rs`. Async + capability-flag variants: `src/table_provider.rs`.
- Mutable-trait variant: `src/udaf/accumulator.rs` (`Box<dyn Accumulator>`).
- Optional-method pattern: `FFI_TableProvider::supports_filters_pushdown`.
- Codec wiring: `src/proto/logical_extension_codec.rs`, `src/proto/physical_extension_codec.rs`.
- Examples crate: `datafusion-examples/examples/ffi` (end-to-end producer + consumer).
