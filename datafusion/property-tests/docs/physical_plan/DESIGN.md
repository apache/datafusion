<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design

This describes how the `physical_plan` module is put together and where it is
heading. See [CHECKS.md](CHECKS.md) for what is checked and
[IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for progress.

## Goal

`ExecutionPlan`s have a lot of properties, both implicit and explicit, that are
important to get right for correctness and performance.  The interface exposes
enough that we should be able to create a generic test suite over that interface
to (1) catch real bugs in `ExecutionPlan`s shipped by DataFusion, and (2) let
users more easily create correct and performant `ExecutionPlan`s.

This crate implements a suite of checks to run on `ExecutionPlan`s, as well as
profiles that vary the inputs to the `ExecutionPlan` to increase test coverage
(e.g. varying statistics precision or input partitioning).

The goal is that users only need to define a "spec" that describes valid inputs
to the plan node and the cases will be generated automatically.  Some plans that
have multiple configurations (e.g. `RepartitionExec` in different partitioning
modes) will require users to define these variations explicitly.

## Layers

**Checks** (`PlanCheck`, `checks/`). A check is a name, a `CheckKind` and a
function that looks at one node and returns findings about that node only.
Checks never execute anything themselves. The kind says what the checker
gathers for the check before it runs, in the `CheckContext`:

- `Static`: nothing. The check only calls the methods that describe the node
  and its children.
- `Execution`: the output of executing each node to completion, the memory
  the execution left reserved, and what executing the partition one past the
  node's last partition did.
- `Variant`: also the output of each `Variant` of each node: the plan returned
  by `with_fetch`, the node with its children limited the way `LimitPushdown`
  limits them, the node run with another session batch size or with its
  `MockSourceExec` leaves split into other batches, or the node executed a
  second time, with or without `reset_plan_states`. Variants run after every
  node has been executed normally, so that their parameters, such as a fetch
  larger than the output, can depend on the normal outputs.
- `Stream`: also the results of each stream `Experiment` on each node, for
  checks that need to see how a node drives its inputs. An experiment rebuilds
  the node with `replace_children_if_necessary`, over copies of the
  `MockSourceExec` leaves with a different `StreamBehavior`, and wraps each
  child in a pass-through probe that reports the child's own properties.
  Observing the direct inputs, rather than the leaves, attributes what is
  observed to the node itself. Each run has a timeout.

Every run uses a fresh copy of the node's subtree made with
`reset_plan_states`, so runs do not affect each other through runtime state
such as dynamic filters, and the plan under test is never mutated. When any
enabled check needs variants, every variant runs; when any needs experiments,
every experiment runs.

Variant runs and experiments are separate because they answer different
questions. An experiment observes how a node drives its streams, often on
inputs that never end, through probes and stop conditions. A variant run is a
normal execution of a different plan or with a different setting, whose output
is compared row by row with the normal output. Both rebuild a node over
modified `MockSourceExec` leaves with `fixtures::map_mock_leaves`. Settings
such as the batch size and the leaf layout apply to the whole subtree, so a
node is only compared under a variant in which every child produced the same
rows in the same order and partitions as normally.

**Inputs** (`fixtures/`). `SourceSpec` is a plain, cloneable description of an
input: schema, partition layout, batch layout, value distribution, ordering,
constant and copied columns, statistics precision, row ids and seed. A
`SourceSpec` is built into a `MockSourceExec`, a leaf node that emits data with
all the properties specified by the `SourceSpec`. Its `StreamBehavior`
controls what its streams do after serving their batches: end, stall, fail, or
never end. An unbounded source reports `Unbounded` and unknown statistics, and
repeats its data in a way that keeps its declared ordering and partitioning
true.

**Oracles** (`oracle/`). Reference computations of the true properties of a set
of batches: exact statistics, sortedness, hash partition placement, whether an
expression is constant, whether two expressions are equal, how the rows of two
sets of batches compare (as multisets, in order, and as a prefix of a sorted
sequence apart from ties), and where the rows of an input appear in an output,
tracked by their row ids. The generated floating point values are small
multiples of 0.5, so sums of them do not depend on the order an operator adds
them in.

**Checker** (`PlanChecker`). Gathers the context the enabled checks need,
visits every node, runs every check on it, and attributes findings to nodes by
path. Plans run on a current-thread Tokio runtime, one run after another, so
reports are deterministic although some operators produce timing-dependent
output.

**Harness** (`harness/`). Builds a plan from a `PlanFactory` for every case
derived from a list of `Profile`s, and runs a `PlanChecker` on each.

## Principles

- **Inputs are the truth.** If a check needs to trust something about an
  input, the input must have verified it.
- **Report at the source.** A problem is reported on the node that causes it,
  not on every ancestor that inherits it. Checks skip nodes whose children
  already failed in the same way.
- **Determinism.** Data is seeded, and plans run on a current-thread runtime,
  so every case, and therefore every report, is the same on every run.
- **Severity reflects consequences.** An invariant violation means DataFusion
  can produce wrong results or errors by trusting the plan. A lint means a
  missed optimization.
- **One factory per configuration.** Following the goal of as little test
  code as possible, a simple plan needs one factory, and a plan with modes,
  flags or options one factory per configuration, such as `RepartitionExec`
  round robin, hash, and each preserving order.

## Harness

```rust
let factory = PlanFactory::new("FilterExec", vec![SourceSpec::new(schema)], |inputs| {
    let input = Arc::clone(&inputs[0]);
    let predicate = col("b", &input.schema())?;
    Ok(Arc::new(FilterExec::try_new(predicate, input)?) as Arc<dyn ExecutionPlan>)
});
let report = PlanHarness::new().check(&factory)?;
report.assert_no_invariant_violations();
```

**Factories.** A `PlanFactory` is a name, one base `SourceSpec` per input, and a
closure that builds the plan from input plans. A plan without inputs, such as
`EmptyExec`, has no base specs and one case. The base spec carries what only
the author of the plan knows: the schema, the value distribution (few distinct
values on join keys so that inputs match), the number of rows, and optionally an
ordering the plan should see, for example to exercise `InputOrderMode::Sorted`.
The harness sets everything else for each case: partition layout, hash
partitioning, batch layout, statistics precision, seed and row ids. The number
of rows stays with the base spec, rather than with the harness, because a
sensible size depends on the plan: a cross join produces the product of its
input sizes. `create` is called several times per case, so it must be a pure
function of its inputs; it must bind expressions to the schemas of the inputs
it receives, by name, since the harness appends a `__row_id` column; and it
must use the input plans as given, since the harness recognizes them by
identity. A factory for a leaf, such as a `DataSourceExec`, can still take an
input and build the leaf from its rows (the `MockSourceExec` it receives),
declaring what the input declares, so that the leaf is checked with every
layout of the profiles rather than in a single case; the audit's sources and
sinks do this. `allow(check, reason)` skips a check in every case and shows the
reason in the report. Write one factory per configuration of the node (see
"One factory per configuration" in the principles).

**Profiles and cases.** A `Profile` describes a layout of inputs, such as
partition weights, statistics precision and seed. A `PlanHarness` holds a list
of profiles and tests a factory once per profile. The factory's base specs say
what its inputs contain, and a profile says how any inputs are laid out, so the
same profiles apply to every factory. For each profile, the harness applies the
profile to every base spec, meets the plan's input requirements, and builds the
plan on the resulting inputs: that is a case. The input requirements come from
the plan: the harness builds the plan, reads the `required_input_ordering` and
`required_input_distribution` of each node directly above an input, changes that
input's spec to meet them, and builds the plan again until every requirement is
met, at most 5 times. The checker then runs on the plan of every case, and the
`FactoryReport` holds one report per case. A factory without inputs has one
case, for the first profile, since every profile gives the same plan.
`PlanHarness::new()` uses `Profile::defaults()`, and `with_profiles` replaces
them. `Profile::extended()` adds other seeds, partition counts and row counts,
which the audit checks with the `extended_tests` feature, in separate
snapshots. A case can be rebuilt by hand from `Case::inputs` and
`PlanFactory::create`.

**Which checks run.** The stream checks depend on how a plan drives its streams
rather than on the shape of its data, and several of them wait for the stream
timeout, so only the `default` profile runs them. Every other case runs the
static, execution and variant checks.

**Reports.** `PlanHarness::check` returns a `FactoryReport` with the plan
and the checker's report of every case. It displays every case, with the
plan and the violations of each case that has any. A violation that occurs
in several cases is listed in each, so the report is long when a plan has
problems, but each case can be read on its own.

## Open questions

- How a factory states which inputs are valid beyond the schema and the value
  distribution, such as value ranges, or schemas that depend on each other.
- How users mark findings as expected per case rather than per check. `allow`
  is per factory.
- Inputs with zero partitions, other stream behaviors given to `create`, and
  probing other hooks (`with_fetch`, `repartitioned`, `try_pushdown_sort`) to
  add cases for section D, are not generated yet.
- D8 `replace_children_consistent` can build the same node on different valid
  inputs by calling `create` on the inputs of two cases.
