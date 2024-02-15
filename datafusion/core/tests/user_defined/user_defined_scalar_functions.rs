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
use arrow_array::{Array, ArrayRef, Float64Array, Int32Array, RecordBatch, UInt8Array};
use arrow_schema::DataType::Float64;
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion::{execution::registry::FunctionRegistry, test_util};
use datafusion_common::cast::as_float64_array;
use datafusion_common::{
    assert_batches_eq, assert_batches_sorted_eq, cast::as_int32_array, not_impl_err,
    plan_err, DataFusionError, ExprSchema, Result, ScalarValue,
};
use datafusion_expr::{
    create_udaf, create_udf, Accumulator, ColumnarValue, ExprSchemable,
    LogicalPlanBuilder, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rand::{thread_rng, Rng};
use std::any::Any;
use std::iter;
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

    let myfunc = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(l) = &args[0] else {
            panic!("should be array")
        };
        let ColumnarValue::Array(r) = &args[1] else {
            panic!("should be array")
        };

        let l = as_int32_array(l)?;
        let r = as_int32_array(r)?;
        Ok(ColumnarValue::from(Arc::new(add(l, r)?) as ArrayRef))
    });

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
    let myfunc = Arc::new(|_args: &[ColumnarValue]| {
        Ok(ColumnarValue::Array(
            Arc::new((0..1).map(|_| 100).collect::<Int32Array>()) as ArrayRef,
        ))
    });

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
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            unimplemented!()
        }

        fn update_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
            unimplemented!()
        }

        fn merge_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
            unimplemented!()
        }

        fn evaluate(&mut self) -> Result<ScalarValue> {
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
    let expected = r#"Projection: my_table.a, AggregateUDF { inner: AggregateUDF { name: "my_acc", signature: Signature { type_signature: Exact([Int32]), volatility: Immutable }, fun: "<FUNC>" } }(my_table.b) PARTITION BY [my_table.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[AggregateUDF { inner: AggregateUDF { name: "my_acc", signature: Signature { type_signature: Exact([Int32]), volatility: Immutable }, fun: "<FUNC>" } }(my_table.b) PARTITION BY [my_table.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
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

    let myfunc = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(array) = &args[0] else {
            panic!("should be array")
        };
        Ok(ColumnarValue::from(Arc::clone(array)))
    });

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

    let myfunc = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(array) = &args[0] else {
            panic!("should be array")
        };
        Ok(ColumnarValue::from(Arc::clone(array)))
    });

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

#[derive(Debug)]
pub struct RandomUDF {
    signature: Signature,
}

impl RandomUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(0, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for RandomUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "random_udf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let len: usize = match &args[0] {
            // This udf is always invoked with zero argument so its argument
            // is a null array indicating the batch size.
            ColumnarValue::Array(array) if array.data_type().is_null() => array.len(),
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid argument type".to_string(),
                ))
            }
        };
        let mut rng = thread_rng();
        let values = iter::repeat_with(|| rng.gen_range(0.1..1.0)).take(len);
        let array = Float64Array::from_iter_values(values);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

/// Ensure that a user defined function with zero argument will be invoked
/// with a null array indicating the batch size.
#[tokio::test]
async fn test_user_defined_functions_zero_argument() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "index",
        DataType::UInt8,
        false,
    )]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt8Array::from_iter_values([1, 2, 3]))],
    )?;

    ctx.register_batch("data_table", batch)?;

    let random_normal_udf = ScalarUDF::from(RandomUDF::new());
    ctx.register_udf(random_normal_udf);

    let result = plan_and_collect(
        &ctx,
        "SELECT random_udf() AS random_udf, random() AS native_random FROM data_table",
    )
    .await?;

    assert_eq!(result.len(), 1);
    let batch = &result[0];
    let random_udf = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let native_random = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(random_udf.len(), native_random.len());

    let mut previous = -1.0;
    for i in 0..random_udf.len() {
        assert!(random_udf.value(i) >= 0.0 && random_udf.value(i) < 1.0);
        assert!(random_udf.value(i) != previous);
        previous = random_udf.value(i);
    }

    Ok(())
}

#[derive(Debug)]
struct TakeUDF {
    signature: Signature,
}

impl TakeUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

/// Implement a ScalarUDFImpl whose return type is a function of the input values
impl ScalarUDFImpl for TakeUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "take"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        not_impl_err!("Not called because the return_type_from_exprs is implemented")
    }

    /// This function returns the type of the first or second argument based on
    /// the third argument:
    ///
    /// 1. If the third argument is '0', return the type of the first argument
    /// 2. If the third argument is '1', return the type of the second argument
    fn return_type_from_exprs(
        &self,
        arg_exprs: &[Expr],
        schema: &dyn ExprSchema,
    ) -> Result<DataType> {
        if arg_exprs.len() != 3 {
            return plan_err!("Expected 3 arguments, got {}.", arg_exprs.len());
        }

        let take_idx = if let Some(Expr::Literal(ScalarValue::Int64(Some(idx)))) =
            arg_exprs.get(2)
        {
            if *idx == 0 || *idx == 1 {
                *idx as usize
            } else {
                return plan_err!("The third argument must be 0 or 1, got: {idx}");
            }
        } else {
            return plan_err!(
                "The third argument must be a literal of type int64, but got {:?}",
                arg_exprs.get(2)
            );
        };

        arg_exprs.get(take_idx).unwrap().get_type(schema)
    }

    // The actual implementation
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let take_idx = match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) if v < &2 => *v as usize,
            _ => unreachable!(),
        };
        match &args[take_idx] {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(array.clone())),
            ColumnarValue::Scalar(_) => unimplemented!(),
        }
    }
}

#[tokio::test]
async fn verify_udf_return_type() -> Result<()> {
    // Create a new ScalarUDF from the implementation
    let take = ScalarUDF::from(TakeUDF::new());

    // SELECT
    //   take(smallint_col, double_col, 0) as take0,
    //   take(smallint_col, double_col, 1) as take1
    // FROM alltypes_plain;
    let exprs = vec![
        take.call(vec![col("smallint_col"), col("double_col"), lit(0_i64)])
            .alias("take0"),
        take.call(vec![col("smallint_col"), col("double_col"), lit(1_i64)])
            .alias("take1"),
    ];

    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await?;

    let df = ctx.table("alltypes_plain").await?.select(exprs)?;

    let schema = df.schema();

    // The output schema should be
    // * type of column smallint_col (int32)
    // * type of column double_col (float64)
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(1).data_type(), &DataType::Float64);

    let expected = [
        "+-------+-------+",
        "| take0 | take1 |",
        "+-------+-------+",
        "| 0     | 0.0   |",
        "| 0     | 0.0   |",
        "| 0     | 0.0   |",
        "| 0     | 0.0   |",
        "| 1     | 10.1  |",
        "| 1     | 10.1  |",
        "| 1     | 10.1  |",
        "| 1     | 10.1  |",
        "+-------+-------+",
    ];
    assert_batches_sorted_eq!(&expected, &df.collect().await?);

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

async fn register_alltypes_parquet(ctx: &SessionContext) -> Result<()> {
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(())
}

/// Execute SQL and return results as a RecordBatch
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}
