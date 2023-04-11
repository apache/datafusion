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
    execution::registry::FunctionRegistry,
    physical_plan::{expressions::AvgAccumulator, functions::make_scalar_function},
};
use datafusion_common::{cast::as_int32_array, ScalarValue};
use datafusion_expr::{create_udaf, Accumulator, LogicalPlanBuilder};

/// test that casting happens on udfs.
/// c11 is f32, but `custom_sqrt` requires f64. Casting happens but the logical plan and
/// physical plan have the same schema.
#[tokio::test]
async fn csv_query_custom_udf_with_cast() -> Result<()> {
    let ctx = create_ctx();
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
            Arc::new(Int32Array::from_slice([1, 10, 10, 100])),
            Arc::new(Int32Array::from_slice([2, 12, 12, 120])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t", batch)?;

    let myfunc = |args: &[ArrayRef]| {
        let l = as_int32_array(&args[0])?;
        let r = as_int32_array(&args[1])?;
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

    let t = ctx.table("t").await?;

    let plan = LogicalPlanBuilder::from(t.into_optimized_plan()?)
        .project(vec![
            col("a"),
            col("b"),
            ctx.udf("my_add")?.call(vec![col("a"), col("b")]),
        ])?
        .build()?;

    assert_eq!(
        format!("{plan:?}"),
        "Projection: t.a, t.b, my_add(t.a, t.b)\n  TableScan: t projection=[a, b]"
    );

    let result = DataFrame::new(ctx.state(), plan).collect().await?;

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
    let a = as_int32_array(batch.column(0))?;
    let b = as_int32_array(batch.column(1))?;
    let sum = as_int32_array(batch.column(2))?;

    assert_eq!(4, a.len());
    assert_eq!(4, b.len());
    assert_eq!(4, sum.len());
    for i in 0..sum.len() {
        assert_eq!(a.value(i) + b.value(i), sum.value(i));
    }

    ctx.deregister_table("t")?;

    Ok(())
}

#[tokio::test]
async fn scalar_udf_zero_params() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([1, 10, 10, 100]))],
    )?;
    let ctx = SessionContext::new();

    ctx.register_batch("t", batch)?;
    // create function just returns 100 regardless of inp
    let myfunc = |args: &[ArrayRef]| {
        let num_rows = args[0].len();
        Ok(Arc::new((0..num_rows).map(|_| 100).collect::<Int32Array>()) as ArrayRef)
    };
    let myfunc = make_scalar_function(myfunc);

    ctx.register_udf(create_udf(
        "get_100",
        vec![],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        myfunc,
    ));

    let result = plan_and_collect(&ctx, "select get_100() a from t").await?;
    let expected = vec![
        "+-----+", //
        "| a   |", //
        "+-----+", //
        "| 100 |", //
        "| 100 |", //
        "| 100 |", //
        "| 100 |", //
        "+-----+", //
    ];
    assert_batches_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "select get_100() a").await?;
    let expected = vec![
        "+-----+", //
        "| a   |", //
        "+-----+", //
        "| 100 |", //
        "+-----+", //
    ];
    assert_batches_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "select get_100() from t where a=999").await?;
    let expected = vec![
        "++", //
        "++",
    ];
    assert_batches_eq!(expected, &result);
    Ok(())
}

/// tests the creation, registration and usage of a UDAF
#[tokio::test]
async fn simple_udaf() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    // define a udaf, using a DataFusion's accumulator
    let my_avg = create_udaf(
        "my_avg",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(Box::new(AvgAccumulator::try_new(
                &DataType::Float64,
                &DataType::Float64,
            )?))
        }),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    );

    ctx.register_udaf(my_avg);

    let result = plan_and_collect(&ctx, "SELECT MY_AVG(a) FROM t").await?;

    let expected = vec![
        "+-------------+",
        "| my_avg(t.a) |",
        "+-------------+",
        "| 3.0         |",
        "+-------------+",
    ];
    assert_batches_eq!(expected, &result);

    Ok(())
}

#[tokio::test]
async fn udaf_as_window_func() -> Result<()> {
    #[derive(Debug)]
    struct MyAccumulator;

    impl Accumulator for MyAccumulator {
        fn state(&self) -> Result<Vec<ScalarValue>> {
            unimplemented!()
        }

        fn update_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
            unimplemented!()
        }

        fn merge_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
            unimplemented!()
        }

        fn evaluate(&self) -> Result<ScalarValue> {
            unimplemented!()
        }

        fn size(&self) -> usize {
            unimplemented!()
        }
    }

    let my_acc = create_udaf(
        "my_acc",
        DataType::Int32,
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(MyAccumulator))),
        Arc::new(vec![DataType::Int32]),
    );

    let context = SessionContext::new();
    context.register_table(
        "my_table",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
            Schema::new(vec![
                Field::new("a", DataType::UInt32, false),
                Field::new("b", DataType::Int32, false),
            ]),
        ))),
    )?;
    context.register_udaf(my_acc);

    let sql = "SELECT a, MY_ACC(b) OVER(PARTITION BY a) FROM my_table";
    let expected = r#"Projection: my_table.a, AggregateUDF { name: "my_acc", signature: Signature { type_signature: Exact([Int32]), volatility: Immutable }, fun: "<FUNC>" }(my_table.b) PARTITION BY [my_table.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[AggregateUDF { name: "my_acc", signature: Signature { type_signature: Exact([Int32]), volatility: Immutable }, fun: "<FUNC>" }(my_table.b) PARTITION BY [my_table.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: my_table"#;

    let dataframe = context.sql(sql).await.unwrap();
    assert_eq!(format!("{:?}", dataframe.logical_plan()), expected);
    Ok(())
}
