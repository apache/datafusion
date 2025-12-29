use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::{self, binary, lit};
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{
    ExecutionPlan, execute_stream, filter::FilterExec, test::TestMemoryExec,
};

const NUM_FIELDS: usize = 1000;

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(
        (0..NUM_FIELDS)
            .map(|i| Arc::new(Field::new(format!("x_{i}"), DataType::Int64, false)))
            .collect::<Fields>(),
    ))
});

fn partitioning() -> Partitioning {
    Partitioning::RoundRobinBatch(16)
}

fn col_name(i: usize) -> String {
    format!("x_{i}")
}

fn aggr_name(i: usize) -> String {
    format!("aggr({})", col_name(i))
}

fn col(i: usize) -> Arc<dyn PhysicalExpr> {
    expressions::col(&col_name(i), &SCHEMA).unwrap()
}

/// Returns a typical plan for the query like:
///
/// ```sql
/// SELECT aggr1(col1) as aggr1, aggr2(col2) as aggr2 FROM t
/// WHERE p1
/// HAVING p2
/// ```
///
/// A plan looks like:
///
/// ```text
/// ProjectionExec
///   FilterExec
///     AggregateExec: mode=Final
///       CoalescePartitionsExec
///         AggregateExec: mode=Partial
///           RepartitionExec
///             FilterExec
///               TestMemoryExec
/// ```
///
fn query1_plan() -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::clone(&SCHEMA);
    let input = TestMemoryExec::try_new(&[vec![]], Arc::clone(&schema), None)?;

    let plan = FilterExec::try_new(
        // Some predicate.
        binary(
            binary(col(0), Operator::Eq, col(1), &schema)?,
            Operator::And,
            binary(col(2), Operator::Eq, lit(42_i64), &schema)?,
            &schema,
        )?,
        Arc::new(input),
    )?;

    let plan = RepartitionExec::try_new(Arc::new(plan), partitioning())?;

    let plan = {
        // Partial aggregation.
        let aggr_expr = (0..NUM_FIELDS)
            .map(|i| {
                AggregateExprBuilder::new(avg_udaf(), vec![col(i)])
                    .schema(Arc::clone(&schema))
                    .alias(aggr_name(i))
                    .build()
                    .map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?;
        let filter_expr = (0..aggr_expr.len()).map(|_| None).collect();

        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new(vec![], vec![], vec![], false),
            aggr_expr,
            filter_expr,
            Arc::new(plan),
            Arc::clone(&schema),
        )?
    };

    let plan = CoalescePartitionsExec::new(Arc::new(plan));

    let schema = plan.schema();
    let plan = {
        // Final aggregation.
        let aggr_expr = (0..NUM_FIELDS)
            .map(|i| {
                AggregateExprBuilder::new(
                    avg_udaf(),
                    vec![Arc::new(expressions::Column::new(&aggr_name(i), i))],
                )
                .schema(Arc::clone(&schema))
                .alias(aggr_name(i))
                .build()
                .map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?;
        let filter_expr = (0..aggr_expr.len()).map(|_| None).collect();

        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new(vec![], vec![], vec![], false),
            aggr_expr,
            filter_expr,
            Arc::new(plan),
            Arc::clone(&schema),
        )?
    };

    let schema = plan.schema();
    let plan = {
        let predicate = (0..schema.fields.len()).fold(lit(true), |expr, i| {
            binary(
                expr,
                Operator::And,
                binary(
                    Arc::new(expressions::Column::new(schema.field(i).name(), i)),
                    Operator::Gt,
                    lit(i as i64),
                    &schema,
                )
                .unwrap(),
                &schema,
            )
            .unwrap()
        });

        FilterExec::try_new(predicate, Arc::new(plan))?
    };

    Ok(Arc::new(plan))
}

#[cfg(not(feature = "stateless_plan"))]
fn reset_plan_states(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    plan.transform_up(|plan| {
        let new_plan = Arc::clone(&plan).reset_state()?;
        Ok(Transformed::yes(new_plan))
    })
    .unwrap()
    .data
}

fn bench_plan_execute(c: &mut Criterion) {
    let task_ctx = Arc::new(TaskContext::default());
    let plan = query1_plan().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("execute", |b| {
        b.iter(|| {
            #[cfg(not(feature = "stateless_plan"))]
            let plan = reset_plan_states(Arc::clone(&plan));

            #[cfg(feature = "stateless_plan")]
            let plan = Arc::clone(&plan);

            let _stream =
                rt.block_on(async { execute_stream(plan, Arc::clone(&task_ctx)) });
        });
    });
}

criterion_group!(benches, bench_plan_execute);
criterion_main!(benches);
