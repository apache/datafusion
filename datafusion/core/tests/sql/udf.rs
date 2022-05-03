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

use super::*;
use arrow::compute::add;
use datafusion::{
    logical_plan::{create_udaf, FunctionRegistry, LogicalPlanBuilder},
    physical_plan::{expressions::AvgAccumulator, functions::make_scalar_function},
};

/// test that casting happens on udfs.
/// c11 is f32, but `custom_sqrt` requires f64. Casting happens but the logical plan and
/// physical plan have the same schema.
#[tokio::test]
async fn csv_query_custom_udf_with_cast() -> Result<()> {
    let ctx = create_ctx()?;
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT avg(custom_sqrt(c11)) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["0.6584408483418833"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn scalar_udf() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from_slice(&[1, 10, 10, 100])),
            Arc::new(Int32Array::from_slice(&[2, 12, 12, 120])),
        ],
    )?;

    let mut ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let myfunc = |args: &[ArrayRef]| {
        let l = &args[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("cast failed");
        let r = &args[1]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("cast failed");
        Ok(Arc::new(add(l, r)?) as ArrayRef)
    };
    let myfunc = make_scalar_function(myfunc);

    ctx.register_udf(create_udf(
        "my_add",
        vec![DataType::Int32, DataType::Int32],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        myfunc,
    ));

    // from here on, we may be in a different scope. We would still like to be able
    // to call UDFs.

    let t = ctx.table("t")?;

    let plan = LogicalPlanBuilder::from(t.to_logical_plan()?)
        .project(vec![
            col("a"),
            col("b"),
            ctx.udf("my_add")?.call(vec![col("a"), col("b")]),
        ])?
        .build()?;

    assert_eq!(
        format!("{:?}", plan),
        "Projection: #t.a, #t.b, my_add(#t.a, #t.b)\n  TableScan: t projection=Some([0, 1])"
    );

    let plan = ctx.optimize(&plan)?;
    let plan = ctx.create_physical_plan(&plan).await?;
    let task_ctx = ctx.task_ctx();
    let result = collect(plan, task_ctx).await?;

    let expected = vec![
        "+-----+-----+-----------------+",
        "| a   | b   | my_add(t.a,t.b) |",
        "+-----+-----+-----------------+",
        "| 1   | 2   | 3               |",
        "| 10  | 12  | 22              |",
        "| 10  | 12  | 22              |",
        "| 100 | 120 | 220             |",
        "+-----+-----+-----------------+",
    ];
    assert_batches_eq!(expected, &result);

    let batch = &result[0];
    let a = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("failed to cast a");
    let b = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("failed to cast b");
    let sum = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("failed to cast sum");

    assert_eq!(4, a.len());
    assert_eq!(4, b.len());
    assert_eq!(4, sum.len());
    for i in 0..sum.len() {
        assert_eq!(a.value(i) + b.value(i), sum.value(i));
    }

    ctx.deregister_table("t")?;

    Ok(())
}

/// tests the creation, registration and usage of a UDAF
#[tokio::test]
async fn simple_udaf() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[4, 5]))],
    )?;

    let mut ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    // define a udaf, using a DataFusion's accumulator
    let my_avg = create_udaf(
        "my_avg",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|| Ok(Box::new(AvgAccumulator::try_new(&DataType::Float64)?))),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    );

    ctx.register_udaf(my_avg);

    let result = plan_and_collect(&ctx, "SELECT MY_AVG(a) FROM t").await?;

    let expected = vec![
        "+-------------+",
        "| my_avg(t.a) |",
        "+-------------+",
        "| 3           |",
        "+-------------+",
    ];
    assert_batches_eq!(expected, &result);

    Ok(())
}
