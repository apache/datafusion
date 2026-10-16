---
name: physical-plan-check
description: Implement or extend a check in the `physical_plan` module of the datafusion-property-tests crate (datafusion/property-tests), from its entry in CHECKS.md to generated test cases, snapshots and documented findings. Use when asked to implement a check code such as B5, finish a Partial check, or add a new check to the catalog.
---

# Implementing a physical plan check

The `physical_plan` module of `datafusion/property-tests` checks that
`ExecutionPlan` implementations keep the promises they make (statistics,
orderings, constants, cardinality, ...). Each check has an entry in
`CHECKS.md`, a row in `IMPLEMENTATION_STATUS.md` (both in
`docs/physical_plan/`), and runs on every plan of the audit in
`tests/physical_plan/builtin_plans/`, whose findings are recorded in insta
snapshots.

Read `DESIGN.md`, the check's entry in `CHECKS.md` and its row in
`IMPLEMENTATION_STATUS.md` before writing code.

## Two rules

### 1. Document findings, do not fix them

We are building the test suite, not fixing DataFusion. When a check reports
a built-in plan:

- **Do not change the node** (in `physical-plan`, `physical-expr`, ...),
  even when the fix looks small. Record the finding instead (see
  [Findings](#findings)).
- The only exception is a bug that blocks the suite itself, for example a
  panic or error that stops other checks from running. Say so explicitly
  when you make such a fix.
- If you are unsure whether a finding is a real bug or a false positive of
  the check, find out first: reproduce it with a small `MockSourceExec`
  input, and read the node's code. A false positive is fixed in the check,
  a real bug is documented.

### 2. Cases are generated automatically wherever possible

A check must run on every plan the `PlanHarness` checks, not only on plans
someone built to exercise it. The harness builds the inputs of a
`PlanFactory` from generated `SourceSpec`s, one case per `Profile`. If the
property a check verifies never appears in those inputs, the check passes
without checking anything.

- Prefer making the generated inputs (`SourceSpec`, `MockSourceExec`) carry
  the property, and adding a `Profile` that turns it on for every factory,
  over adding hand-written factories to `tests/physical_plan/builtin_plans/`.
- **Bring attention to every case that cannot be generated automatically.**
  Say in the final reply which situations the generated cases do not reach
  and why, what would reach them, and record them in
  `IMPLEMENTATION_STATUS.md` (the check's notes or "Checks allowed in the
  audit, and check limitations"). Do not leave this implicit.

## Workflow

### 1. Understand the catalog entry

- Pick the `CheckKind`: `Static` (no execution), `Execution` (compares
  claims with the output of a normal run), `Stream` (stream experiments,
  default case only) or `Variant` (reruns with another fetch, batch size,
  batch layout or limited inputs). The check goes in
  `src/physical_plan/checks/<kind>_checks.rs`.
- Decide the severity: `Finding::invariant` for a false claim that can give
  wrong results, `Finding::lint` for a missed optimization or a heuristic.
- If the catalog entry is ambiguous or wrong, update `CHECKS.md` in the same
  change, and say so.

### 2. Put reference computations in `oracle/`

A check that compares a claim with data computes the truth with a function
in `src/physical_plan/oracle/` (pure, over `&[RecordBatch]` or one set of
batches per partition, simple rather than fast). Examples: `exact_statistics`,
`first_unsorted_row`, `constant_violations`, `first_unequal_row`.

If the inputs should also declare the property (rule 2), `MockSourceExec`
validates its declaration with the same oracle function, so the check and
the source agree on what the property means.

### 3. Write the check

- A function named after the check, with a doc comment that starts with
  its code (`/// B5: ...`), registered in `all_checks()` in
  `src/physical_plan/checks/mod.rs` in catalog order.
- Use `CheckContext` for outputs (`output`, `child_outputs`,
  `variant_runs`, ...). A node that was not executed or failed has no
  output: return no findings, `execution_succeeds` reports it.
- Do not report what another check reports. Skip, with a comment naming the
  other check:
  - expressions that refer to a column by a wrong index or name
    (`stale_columns`, reported by `expression_column_refs`);
  - batches that do not match the schema, and expressions that cannot be
    evaluated on the output (`batch_schema`);
  - errors computing statistics (`statistics_shape`);
  - claims another check covers (for example literals in an equivalence
    class are constants, checked by `constants_hold`).
- Attribution: a node that inherits a false claim from its child should be
  reported on the child, or checked against corrected inputs, as
  `exact_statistics_hold` does with `statistics_from_inputs`.
- Plain output: one finding per problem (per expression, partition, ...),
  with the claimed and actual values. Do not group, summarize or shorten
  findings; long snapshots are fine.

### 4. Generate cases for it

Ask: **do the generated inputs ever have the property this check verifies,
and does it reach the nodes above them?**

- If `MockSourceExec` does not report the property, extend it: a
  `try_with_...` method that validates the declaration against the data
  with the oracle and rejects false ones, and a `SourceSpec::with_...`
  method that generates data with the property. Examples:
  `with_ordering`, `with_hash_partitioning`, `with_constant`,
  `with_copy` (columns equal to another column).
- Add a `Profile` in `Profile::defaults()` that applies it to every column
  of every input, as `uniform constants`, `constants per partition` and
  `copied columns` do (`src/physical_plan/harness/requirements.rs`,
  `derive_case`). Use `Profile::extended()` only for expensive variations.
- Verify coverage with a temporary test in
  `tests/physical_plan/builtin_plans/` that walks the plan of each case and
  prints, per node, how much of the
  property it reports (for example the number of constants or equivalence
  classes). Remove the probe afterwards, and report which nodes the property
  does and does not reach, and why.
- Degenerate data can make heuristic checks report false positives (for
  example, sorting a constant column never reorders it, so
  `maintains_input_order_missed` saw a sort that keeps input order). Look
  at every finding in the new case that the `default` case does not have,
  and fix the guard of the check that misreports (see
  `reports_sorted_output`).
- When generated cases cannot reach a situation, describe it (rule 2). For
  example, `FilterExec` on `a = 3` claims `distinct_count` `Exact(1)` for a
  partition with rows but no 3, which no case showed while every generated
  partition contained every value; the `hash partitioned` profile, which
  puts every 3 in one partition, reaches it. Say what would reach such a
  situation (a profile with many small partitions, a base spec with more
  distinct values) and ask before adding it.
- Add a factory for each configuration of a node (mode, flag, option)
  without asking; only a multi-node plan built for one edge case needs
  justification. See "One factory per configuration" in `DESIGN.md`.

### 5. Test the check

- `tests/physical_plan/<kind>_checks.rs`: a broken plan that is reported,
  with the exact message, and a correct plan that is clean.
  `ConfigurableExec` (`tests/physical_plan/common.rs`) can claim properties a
  plan does not have, for example `claimed_eq_properties`. Build explicit
  batches with `MockSourceExec::try_new` when the message depends on the
  data.
- `tests/physical_plan/fixtures.rs`: new `SourceSpec` and `MockSourceExec`
  features generate what they say, declare it, and reject false
  declarations.
- Tests that count cases use `Profile::defaults().len()`, not a number.

### 6. Update the snapshots and review every change

```bash
cd datafusion/property-tests
INSTA_UPDATE=always cargo test --no-fail-fast
INSTA_UPDATE=always cargo test --features extended_tests --test physical_plan builtin_plans
rm -f tests/physical_plan/builtin_plans/snapshots/*.snap.new
git diff -- tests/physical_plan/builtin_plans/snapshots
```

Read every added and removed line. For each new finding decide: real bug in
the node (document it), false positive of the check (fix the check), or a
known limitation of the check (`PlanFactory::allow(check, reason)`, listed
in "Checks allowed in the audit"). A removed finding needs a reason too.

### Findings

Record each real bug in "Known findings in built-in plans" in
`IMPLEMENTATION_STATUS.md`: the plans, the checks, which cases report it,
the cause with `file:line` references into DataFusion, the impact, and the
fix it would need (or "Needs a decision" if the contract is ambiguous). Keep
the entry when the same cause shows up in more cases or checks; extend it
instead of adding another. A bug no snapshot shows (found by reading code
or with a throwaway test) goes in the final reply, with what would make the
audit show it.

### 7. Document

- `CHECKS.md`: the entry says what is checked, what is not and why, and how
  findings are reported.
- `IMPLEMENTATION_STATUS.md`: the check's status (`Done` or `Partial` with
  what is missing), the rows of the harness components you changed
  (`MockSourceExec`, `SourceSpec`, `oracle`, `Profile`, `Case`), known
  findings, allowed checks and limitations.
- `DESIGN.md` and `README.md` list the profiles: update them when adding
  one.
- Use ASCII only.

### 8. Verify

```bash
cargo fmt --all
cargo clippy -p datafusion-property-tests --all-targets --all-features -- -D warnings
./ci/scripts/doc_prettier_check.sh --write --allow-dirty
cd datafusion/property-tests && cargo test --features extended_tests
```

If you changed code outside the crate (only to unblock the suite), also run
that crate's tests and the sqllogictests.

### 9. Report

The final reply covers:

- what the check does and what it skips;
- how the generated cases exercise it, and how you verified that;
- **cases that cannot be generated automatically**, in their own section,
  with what would reach them;
- findings, recorded in `IMPLEMENTATION_STATUS.md`, and any false positives
  you fixed in other checks;
- what was run, and what was not.
