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

# Apache DataFusion Property Tests

[Apache DataFusion] is an extensible query execution framework, written in
Rust, that uses [Apache Arrow] as its in-memory format.

This crate checks that implementations of DataFusion's extension points are
consistent with the contracts of those extension points. DataFusion trusts
what an implementation reports about itself, so an implementation that
reports something false can lead to wrong results or missed optimizations.
The checks run against the implementations defined in DataFusion and can be
used to test your own.

The crate currently covers `ExecutionPlan`, in the `physical_plan` module.

## ExecutionPlan

Optimizer rules trust what a plan reports about itself, such as its
cardinality effect, statistics and orderings. The checks compare these with
each other, with what the plan's inputs report, and with what the plan does
when it runs.

Describe how to build your plan from its inputs, and let the harness do the
rest:

```rust
use datafusion_property_tests::physical_plan::fixtures::SourceSpec;
use datafusion_property_tests::physical_plan::harness::{PlanFactory, PlanHarness};

// One base spec per input: its schema, value distribution and size
let factory = PlanFactory::new("MyExec", vec![SourceSpec::new(schema)], |inputs| {
    let input = Arc::clone(&inputs[0]);
    // Bind expressions to the input's schema by name: the harness adds a
    // `__row_id` column to every input
    let key = col("a", &input.schema())?;
    Ok(Arc::new(MyExec::try_new(key, input)?) as Arc<dyn ExecutionPlan>)
});

let report = PlanHarness::new().check(&factory)?;
report.assert_no_invariant_violations();
```

The harness generates the inputs, sorts, hash partitions or merges them into
one partition as the plan requires, and checks the plan on several cases:
several partitions with an empty one, a single partition, inexact and absent
statistics, empty input, constant columns, columns with an equal copy,
input sorted by its row ids, and hash partitioned input. The report lists
every case, with the plan and the findings of each case that has any.

To check a plan built by hand, use `PlanChecker` on inputs generated with
`SourceSpec`:

```rust
use datafusion_property_tests::physical_plan::PlanChecker;
use datafusion_property_tests::physical_plan::fixtures::SourceSpec;

// Generate an input with known data, statistics, ordering and partitioning
let input = SourceSpec::new(schema)
    .with_partition_rows(&[100, 0, 250])
    .build_arc()?;
let plan = build_my_plan(input)?;

let report = PlanChecker::new().check(&plan)?;
report.assert_no_invariant_violations();
```

- [CHECKS.md](docs/physical_plan/CHECKS.md) lists every check, why it
  matters and how to fix a violation.
- [IMPLEMENTATION_STATUS.md](docs/physical_plan/IMPLEMENTATION_STATUS.md)
  tracks which checks are implemented, and the violations currently found in
  DataFusion's own plans.
- [DESIGN.md](docs/physical_plan/DESIGN.md) describes how the module is
  structured, including the harness, and where it is heading.

To add a check, write a function that returns the findings for one node, in
the file of `src/physical_plan/checks/` for the check's `CheckKind`, add it
to `checks::all_checks` with its name and `CheckKind`, and describe it in
[CHECKS.md](docs/physical_plan/CHECKS.md).

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
