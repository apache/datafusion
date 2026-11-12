<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Physical plan benchmarks

## Reproducing the scalar aggregate FILTER measurements

The following optional benchmark exercises scalar `true`, `false`, `NULL`, and
batch-varying predicates in ungrouped, grouped, and skip-partial aggregation.
A shuffled 50%-selective array predicate and an unfiltered aggregate serve as
controls. It uses the existing in-memory `AggregateExec` benchmark helpers to
exclude SQL planning and file I/O.

To run it locally, paste the function below into
[`aggregate_filter.rs`](aggregate_filter.rs), immediately before
`aggregate_filter_benchmark`. In `aggregate_filter_benchmark`, add this call
immediately after `let runtime = Runtime::new().unwrap();`:

```rust
benchmark_scalar_filters(c, &runtime);
```

<details>
<summary>Benchmark code</summary>

```rust
fn benchmark_scalar_filters(c: &mut Criterion, runtime: &Runtime) {
    use datafusion_common::ScalarValue;
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion_physical_expr::ScalarFunctionExpr;

    let schema = schema();
    let batch_filter = Arc::new(create_udf(
        "batch_filter",
        vec![DataType::Boolean],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(|args| match &args[0] {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(array, 0)?,
            )),
            scalar => Ok(scalar.clone()),
        }),
    ));
    let varying: Arc<dyn PhysicalExpr> = Arc::new(
        ScalarFunctionExpr::try_new(
            batch_filter,
            vec![col("include_a", &schema).unwrap()],
            &schema,
            Arc::default(),
        )
        .unwrap(),
    );
    let filters = [
        ("true", Some(lit(true))),
        ("false", Some(lit(false))),
        ("null", Some(lit(ScalarValue::Boolean(None)))),
        ("varying", Some(varying)),
        ("array_50_percent", Some(col("include_a", &schema).unwrap())),
        ("unfiltered", None),
    ];
    let config = SessionConfig::new()
        .set_usize(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            0,
        )
        .set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(0.0)),
        );
    let task_ctx = Arc::new(TaskContext::default().with_session_config(config));
    for (name, mode, grouped) in [
        ("ungrouped", AggregateMode::Single, false),
        ("grouped", AggregateMode::Single, true),
        ("skip_partial", AggregateMode::Partial, true),
    ] {
        let batches = make_batches(
            Some(50),
            if mode == AggregateMode::Partial {
                NUM_ROWS
            } else {
                16
            },
        );
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
        let mut group = c.benchmark_group(format!("scalar_aggregate_filter/{name}"));
        for (filter_name, filter) in &filters {
            let group_by = if grouped {
                PhysicalGroupBy::new_single(vec![(
                    col("group_key", &schema).unwrap(),
                    "group_key".to_string(),
                )])
            } else {
                PhysicalGroupBy::default()
            };
            let plan: Arc<dyn ExecutionPlan> = Arc::new(
                AggregateExec::try_new(
                    mode,
                    group_by,
                    vec![aggregate_expr(&schema, 0, ArgumentKind::Column)],
                    vec![filter.clone()],
                    Arc::clone(&input),
                    Arc::clone(&schema),
                )
                .unwrap(),
            );
            group.bench_function(*filter_name, |b| {
                b.iter(|| {
                    black_box(
                        runtime
                            .block_on(collect(Arc::clone(&plan), Arc::clone(&task_ctx)))
                            .unwrap(),
                    );
                });
            });
        }
        group.finish();
    }
}
```

</details>

Run from the repository root:

```bash
cargo bench --profile release-nonlto -p datafusion-physical-plan \
    --features test_utils --bench aggregate_filter -- \
    scalar_aggregate_filter --warm-up-time 0.5 --measurement-time 1 \
    --sample-size 30 --noplot
```

For comparisons, apply the same snippet and registration call to both revisions,
use separate `CARGO_TARGET_DIR` directories, and alternate the run order. The
benchmark uses 65,536 rows in batches of 8,192, with 16 groups for grouped
aggregation and one group per row for skip-partial aggregation. The latter sets
both skip-partial probe thresholds to zero. The varying predicate returns the
first shuffled filter value in each batch as a scalar.

### Recorded measurements

The scalar FILTER change was compared with main at `34eb6dc5b`, using the same
benchmark source and `release-nonlto` builds. Times below are medians of three
per-run medians, with 30 Criterion samples, 0.5 s warm-up, and 1 s measurement per
case. The two revisions were run in alternating order.

| Ungrouped FILTER               | main median | Scalar path median | Change |
| ------------------------------ | ----------: | -----------------: | -----: |
| Scalar true                    |    13.54 us |           12.28 us |  -9.3% |
| Scalar false                   |     5.75 us |            3.93 us | -31.7% |
| Scalar NULL                    |     7.21 us |            4.06 us | -43.7% |
| Scalar varying between batches |     7.45 us |            5.74 us | -23.0% |

An initial short run suggested a 6.2% slowdown in the skip-partial array-filter
control. Three longer runs per build, alternating the run order, did not reproduce
it. To repeat those control measurements, use:

```bash
cargo bench --profile release-nonlto -p datafusion-physical-plan \
    --features test_utils --bench aggregate_filter -- \
    'scalar_aggregate_filter/(grouped|skip_partial)/(array_50_percent|unfiltered|true)$' \
    --warm-up-time 1 --measurement-time 3 --sample-size 60 --noplot
```

| Control                   | main median | Scalar path median | Change |
| ------------------------- | ----------: | -----------------: | -----: |
| Grouped array filter      |   211.42 us |          210.88 us |  -0.3% |
| Grouped unfiltered        |   162.90 us |          162.12 us |  -0.5% |
| Grouped scalar true       |   162.36 us |          162.13 us |  -0.1% |
| Skip-partial array filter |   196.14 us |          193.73 us |  -1.2% |
| Skip-partial unfiltered   |   134.08 us |          133.83 us |  -0.2% |
| Skip-partial scalar true  |   134.70 us |          134.04 us |  -0.5% |

These local microbenchmarks ran on a shared macOS machine. They measure aggregate
execution overhead and do not establish an end-to-end query speedup.
