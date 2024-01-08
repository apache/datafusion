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

use arrow::compute::kernels::numeric::add;
use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion::{
    execution::registry::FunctionRegistry,
    physical_plan::functions::make_scalar_function, test_util,
};
use datafusion_common::cast::as_float64_array;
use datafusion_common::{assert_batches_eq, cast::as_int32_array, Result, ScalarValue};
use datafusion_expr::{
    create_udaf, create_udf, Accumulator, ColumnarValue, LogicalPlanBuilder, Volatility,
};
use std::sync::Arc;

/// test that casting happens on udfs.
/// c11 is f32, but `custom_sqrt` requires f64. Casting happens but the logical plan and
/// physical plan have the same schema.
#[tokio::test]
async fn csv_query_custom_udf_with_cast() -> Result<()> {
    let ctx = create_udf_context();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT avg(custom_sqrt(c11)) FROM aggregate_test_100";
    let actual = plan_and_collect(&ctx, sql).await.unwrap();
    let expected = [
        "+------------------------------------------+",
        "| AVG(custom_sqrt(aggregate_test_100.c11)) |",
        "+------------------------------------------+",
        "| 0.6584408483418835                       |",
        "+------------------------------------------+",
    ];
    assert_batches_eq!(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_avg_sqrt() -> Result<()> {
    let ctx = create_udf_context();
    register_aggregate_csv(&ctx).await?;
    // Note it is a different column (c12) than above (c11)
    let sql = "SELECT avg(custom_sqrt(c12)) FROM aggregate_test_100";
    let actual = plan_and_collect(&ctx, sql).await.unwrap();
    let expected = [
        "+------------------------------------------+",
        "| AVG(custom_sqrt(aggregate_test_100.c12)) |",
        "+------------------------------------------+",
        "| 0.6706002946036459                       |",
        "+------------------------------------------+",
    ];
    assert_batches_eq!(&expected, &actual);
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
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
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

    let expected = [
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
        vec![Arc::new(Int32Array::from(vec![1, 10, 10, 100]))],
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
    let expected = [
        "+-----+", //
        "| a   |", //
        "+-----+", //
        "| 100 |", //
        "| 100 |", //
        "| 100 |", //
        "| 100 |", //
        "+-----+",
    ];
    assert_batches_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "select get_100() a").await?;
    let expected = [
        "+-----+", //
        "| a   |", //
        "+-----+", //
        "| 100 |", //
        "+-----+",
    ];
    assert_batches_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "select get_100() from t where a=999").await?;
    let expected = [
        "++", //
        "++",
    ];
    assert_batches_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn scalar_udf_override_built_in_scalar_function() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![-100]))],
    )?;
    let ctx = SessionContext::new();

    ctx.register_batch("t", batch)?;
    // register a UDF that has the same name as a builtin function (abs) and just returns 1 regardless of input
    ctx.register_udf(create_udf(
        "abs",
        vec![DataType::Int32],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))))),
    ));

    // Make sure that the UDF is used instead of the built-in function
    let result = plan_and_collect(&ctx, "select abs(a) a from t").await?;
    let expected = [
        "+---+", //
        "| a |", //
        "+---+", //
        "| 1 |", //
        "+---+",
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
        vec![DataType::Int32],
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

#[tokio::test]
async fn case_sensitive_identifiers_user_defined_functions() -> Result<()> {
    let ctx = SessionContext::new();
    let arr = Int32Array::from(vec![1]);
    let batch = RecordBatch::try_from_iter(vec![("i", Arc::new(arr) as _)])?;
    ctx.register_batch("t", batch).unwrap();

    let myfunc = |args: &[ArrayRef]| Ok(Arc::clone(&args[0]));
    let myfunc = make_scalar_function(myfunc);

    ctx.register_udf(create_udf(
        "MY_FUNC",
        vec![DataType::Int32],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        myfunc,
    ));

    // doesn't work as it was registered with non lowercase
    let err = plan_and_collect(&ctx, "SELECT MY_FUNC(i) FROM t")
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Error during planning: Invalid function \'my_func\'"));

    // Can call it if you put quotes
    let result = plan_and_collect(&ctx, "SELECT \"MY_FUNC\"(i) FROM t").await?;

    let expected = [
        "+--------------+",
        "| MY_FUNC(t.i) |",
        "+--------------+",
        "| 1            |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &result);

    Ok(())
}

#[tokio::test]
async fn test_user_defined_functions_with_alias() -> Result<()> {
    let ctx = SessionContext::new();
    let arr = Int32Array::from(vec![1]);
    let batch = RecordBatch::try_from_iter(vec![("i", Arc::new(arr) as _)])?;
    ctx.register_batch("t", batch).unwrap();

    let myfunc = |args: &[ArrayRef]| Ok(Arc::clone(&args[0]));
    let myfunc = make_scalar_function(myfunc);

    let udf = create_udf(
        "dummy",
        vec![DataType::Int32],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        myfunc,
    )
    .with_aliases(vec!["dummy_alias"]);

    ctx.register_udf(udf);

    let expected = [
        "+------------+",
        "| dummy(t.i) |",
        "+------------+",
        "| 1          |",
        "+------------+",
    ];
    let result = plan_and_collect(&ctx, "SELECT dummy(i) FROM t").await?;
    assert_batches_eq!(expected, &result);

    let alias_result = plan_and_collect(&ctx, "SELECT dummy_alias(i) FROM t").await?;
    assert_batches_eq!(expected, &alias_result);

    Ok(())
}

fn create_udf_context() -> SessionContext {
    let ctx = SessionContext::new();
    // register a custom UDF
    ctx.register_udf(create_udf(
        "custom_sqrt",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(custom_sqrt),
    ));

    ctx
}

fn custom_sqrt(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arg = &args[0];
    if let ColumnarValue::Array(v) = arg {
        let input = as_float64_array(v).expect("cast failed");
        let array: Float64Array = input.iter().map(|v| v.map(|x| x.sqrt())).collect();
        Ok(ColumnarValue::Array(Arc::new(array)))
    } else {
        unimplemented!()
    }
}

async fn register_aggregate_csv(ctx: &SessionContext) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    let schema = test_util::aggr_test_schema();
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

/// Execute SQL and return results as a RecordBatch
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}
