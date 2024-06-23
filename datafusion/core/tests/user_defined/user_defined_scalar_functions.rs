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

use std::any::Any;
use std::sync::Arc;

use arrow::compute::kernels::numeric::add;
use arrow_array::{ArrayRef, Float32Array, Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::context::{FunctionFactory, RegisterFunction, SessionState};
use datafusion::prelude::*;
use datafusion::{execution::registry::FunctionRegistry, test_util};
use datafusion_common::cast::{as_float64_array, as_int32_array};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    assert_batches_eq, assert_batches_sorted_eq, assert_contains, exec_err, internal_err,
    not_impl_err, plan_err, DFSchema, DataFusionError, ExprSchema, Result, ScalarValue,
};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    Accumulator, ColumnarValue, CreateFunction, CreateFunctionBody, ExprSchemable,
    LogicalPlanBuilder, OperateFunctionArg, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions_array::range::range_udf;
use parking_lot::Mutex;
use sqlparser::ast::Ident;

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
        "| avg(custom_sqrt(aggregate_test_100.c11)) |",
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
        "| avg(custom_sqrt(aggregate_test_100.c12)) |",
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

struct Simple0ArgsScalarUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
}

impl std::fmt::Debug for Simple0ArgsScalarUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl ScalarUDFImpl for Simple0ArgsScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        not_impl_err!("{} function does not accept arguments", self.name())
    }

    fn invoke_no_args(&self, _number_rows: usize) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(100))))
    }
}

#[tokio::test]
async fn test_row_mismatch_error_in_scalar_udf() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t", batch)?;

    // udf that always return 1 row
    let buggy_udf = Arc::new(|_: &[ColumnarValue]| {
        Ok(ColumnarValue::Array(Arc::new(Int32Array::from(vec![0]))))
    });

    ctx.register_udf(create_udf(
        "buggy_func",
        vec![DataType::Int32],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        buggy_udf,
    ));
    assert_contains!(
        ctx.sql("select buggy_func(a) from t")
            .await?
            .show()
            .await
            .err()
            .unwrap()
            .to_string(),
        "UDF returned a different number of rows than expected"
    );
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

    let get_100_udf = Simple0ArgsScalarUDF {
        name: "get_100".to_string(),
        signature: Signature::exact(vec![], Volatility::Immutable),
        return_type: DataType::Int32,
    };

    ctx.register_udf(ScalarUDF::from(get_100_udf));

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
    let expected = r#"Projection: my_table.a, my_acc(my_table.b) PARTITION BY [my_table.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[my_acc(my_table.b) PARTITION BY [my_table.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
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
struct CastToI64UDF {
    signature: Signature,
}

impl CastToI64UDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CastToI64UDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "cast_to_i64"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    // Demonstrate simplifying a UDF
    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // DataFusion should have ensured the function is called with just a
        // single argument
        assert_eq!(args.len(), 1);
        let arg = args.pop().unwrap();

        // Note that Expr::cast_to requires an ExprSchema but simplify gets a
        // SimplifyInfo so we have to replicate some of the casting logic here.

        let source_type = info.get_data_type(&arg)?;
        let new_expr = if source_type == DataType::Int64 {
            // the argument's data type is already the correct type
            arg
        } else {
            // need to use an actual cast to get the correct type
            Expr::Cast(datafusion_expr::Cast {
                expr: Box::new(arg),
                data_type: DataType::Int64,
            })
        };
        // return the newly written argument to DataFusion
        Ok(ExprSimplifyResult::Simplified(new_expr))
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        unimplemented!("Function should have been simplified prior to evaluation")
    }
}

#[tokio::test]
async fn test_user_defined_functions_cast_to_i64() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, false)]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0]))],
    )?;

    ctx.register_batch("t", batch)?;

    let cast_to_i64_udf = ScalarUDF::from(CastToI64UDF::new());
    ctx.register_udf(cast_to_i64_udf);

    let result = plan_and_collect(&ctx, "SELECT cast_to_i64(x) FROM t").await?;

    assert_batches_eq!(
        &[
            "+------------------+",
            "| cast_to_i64(t.x) |",
            "+------------------+",
            "| 1                |",
            "| 2                |",
            "| 3                |",
            "+------------------+"
        ],
        &result
    );

    Ok(())
}

#[tokio::test]
async fn deregister_udf() -> Result<()> {
    let cast2i64 = ScalarUDF::from(CastToI64UDF::new());
    let ctx = SessionContext::new();

    ctx.register_udf(cast2i64.clone());

    assert!(ctx.udfs().contains("cast_to_i64"));

    ctx.deregister_udf("cast_to_i64");

    assert!(!ctx.udfs().contains("cast_to_i64"));

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
        _arg_data_types: &[DataType],
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

// create_scalar_function_from_sql_statement helper
// structures and methods.

#[derive(Debug, Default)]
struct CustomFunctionFactory {}

#[async_trait::async_trait]
impl FunctionFactory for CustomFunctionFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        let f: ScalarFunctionWrapper = statement.try_into()?;

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(f))))
    }
}
// a wrapper type to be used to register
// custom function to datafusion context
//
// it also defines custom [ScalarUDFImpl::simplify()]
// to replace ScalarUDF expression with one instance contains.
#[derive(Debug)]
struct ScalarFunctionWrapper {
    name: String,
    expr: Expr,
    signature: Signature,
    return_type: arrow_schema::DataType,
}

impl ScalarUDFImpl for ScalarFunctionWrapper {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow_schema::DataType],
    ) -> Result<arrow_schema::DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(
        &self,
        _args: &[datafusion_expr::ColumnarValue],
    ) -> Result<datafusion_expr::ColumnarValue> {
        internal_err!("This function should not get invoked!")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let replacement = Self::replacement(&self.expr, &args)?;

        Ok(ExprSimplifyResult::Simplified(replacement))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

impl ScalarFunctionWrapper {
    // replaces placeholders with actual arguments
    fn replacement(expr: &Expr, args: &[Expr]) -> Result<Expr> {
        let result = expr.clone().transform(|e| {
            let r = match e {
                Expr::Placeholder(placeholder) => {
                    let placeholder_position =
                        Self::parse_placeholder_identifier(&placeholder.id)?;
                    if placeholder_position < args.len() {
                        Transformed::yes(args[placeholder_position].clone())
                    } else {
                        exec_err!(
                            "Function argument {} not provided, argument missing!",
                            placeholder.id
                        )?
                    }
                }
                _ => Transformed::no(e),
            };

            Ok(r)
        })?;

        Ok(result.data)
    }
    // Finds placeholder identifier.
    // placeholders are in `$X` format where X >= 1
    fn parse_placeholder_identifier(placeholder: &str) -> Result<usize> {
        if let Some(value) = placeholder.strip_prefix('$') {
            Ok(value.parse().map(|v: usize| v - 1).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Placeholder `{}` parsing error: {}!",
                    placeholder, e
                ))
            })?)
        } else {
            exec_err!("Placeholder should start with `$`!")
        }
    }
}

impl TryFrom<CreateFunction> for ScalarFunctionWrapper {
    type Error = DataFusionError;

    fn try_from(definition: CreateFunction) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            name: definition.name,
            expr: definition
                .params
                .function_body
                .expect("Expression has to be defined!"),
            return_type: definition
                .return_type
                .expect("Return type has to be defined!"),
            signature: Signature::exact(
                definition
                    .args
                    .unwrap_or_default()
                    .into_iter()
                    .map(|a| a.data_type)
                    .collect(),
                definition
                    .params
                    .behavior
                    .unwrap_or(datafusion_expr::Volatility::Volatile),
            ),
        })
    }
}

#[tokio::test]
async fn create_scalar_function_from_sql_statement() -> Result<()> {
    let function_factory = Arc::new(CustomFunctionFactory::default());
    let ctx = SessionContext::new().with_function_factory(function_factory.clone());
    let options = SQLOptions::new().with_allow_ddl(false);

    let sql = r#"
    CREATE FUNCTION better_add(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        RETURN $1 + $2
    "#;

    // try to `create function` when sql options have allow ddl disabled
    assert!(ctx.sql_with_options(sql, options).await.is_err());

    // Create the `better_add` function dynamically via CREATE FUNCTION statement
    assert!(ctx.sql(sql).await.is_ok());
    // try to `drop function` when sql options have allow ddl disabled
    assert!(ctx
        .sql_with_options("drop function better_add", options)
        .await
        .is_err());

    let result = ctx
        .sql("select better_add(2.0, 2.0)")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-----------------------------------+",
            "| better_add(Float64(2),Float64(2)) |",
            "+-----------------------------------+",
            "| 4.0                               |",
            "+-----------------------------------+",
        ],
        &result
    );

    // statement drops  function
    assert!(ctx.sql("drop function better_add").await.is_ok());
    // no function, it panics
    assert!(ctx.sql("drop function better_add").await.is_err());
    // no function, it dies not care
    assert!(ctx.sql("drop function if exists better_add").await.is_ok());
    // query should fail as there is no function
    assert!(ctx.sql("select better_add(2.0, 2.0)").await.is_err());

    // tests expression parsing
    // if expression is not correct
    let bad_expression_sql = r#"
    CREATE FUNCTION bad_expression_fun(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        RETURN $1 $3
    "#;
    assert!(ctx.sql(bad_expression_sql).await.is_err());

    // tests bad function definition
    let bad_definition_sql = r#"
    CREATE FUNCTION bad_definition_fun(DOUBLE, DOUBLE)
        RET BAD_TYPE
        RETURN $1 + $3
    "#;
    assert!(ctx.sql(bad_definition_sql).await.is_err());

    Ok(())
}

/// Saves whatever is passed to it as a scalar function
#[derive(Debug, Default)]
struct RecordingFunctonFactory {
    calls: Mutex<Vec<CreateFunction>>,
}

impl RecordingFunctonFactory {
    fn new() -> Self {
        Self::default()
    }

    /// return all the calls made to the factory
    fn calls(&self) -> Vec<CreateFunction> {
        self.calls.lock().clone()
    }
}

#[async_trait::async_trait]
impl FunctionFactory for RecordingFunctonFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        self.calls.lock().push(statement);

        let udf = range_udf();
        Ok(RegisterFunction::Scalar(udf))
    }
}

#[tokio::test]
async fn create_scalar_function_from_sql_statement_postgres_syntax() -> Result<()> {
    let function_factory = Arc::new(RecordingFunctonFactory::new());
    let ctx = SessionContext::new().with_function_factory(function_factory.clone());

    let sql = r#"
      CREATE FUNCTION strlen(name TEXT)
      RETURNS int LANGUAGE plrust AS
      $$
        Ok(Some(name.unwrap().len() as i32))
      $$;
    "#;

    let body = "
        Ok(Some(name.unwrap().len() as i32))
      ";

    match ctx.sql(sql).await {
        Ok(_) => {}
        Err(e) => {
            panic!("Error creating function: {}", e);
        }
    }

    // verify that the call was passed through
    let calls = function_factory.calls();
    let schema = DFSchema::try_from(Schema::empty())?;
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    let expected = CreateFunction {
        or_replace: false,
        temporary: false,
        name: "strlen".into(),
        args: Some(vec![OperateFunctionArg {
            name: Some(Ident {
                value: "name".into(),
                quote_style: None,
            }),
            data_type: DataType::Utf8,
            default_expr: None,
        }]),
        return_type: Some(DataType::Int32),
        params: CreateFunctionBody {
            language: Some(Ident {
                value: "plrust".into(),
                quote_style: None,
            }),
            behavior: None,
            function_body: Some(lit(body)),
        },
        schema: Arc::new(schema),
    };

    assert_eq!(call, &expected);

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
