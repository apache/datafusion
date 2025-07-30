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
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{as_string_array, create_array, record_batch, Int8Array, UInt64Array};
use arrow::array::{
    builder::BooleanBuilder, cast::AsArray, Array, ArrayRef, Float32Array, Float64Array,
    Int32Array, RecordBatch, StringArray,
};
use arrow::compute::kernels::numeric::add;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_schema::extension::{Bool8, CanonicalExtensionType, ExtensionType};
use arrow_schema::{ArrowError, FieldRef};
use datafusion::common::test_util::batches_to_string;
use datafusion::execution::context::{FunctionFactory, RegisterFunction, SessionState};
use datafusion::prelude::*;
use datafusion::{execution::registry::FunctionRegistry, test_util};
use datafusion_common::cast::{as_float64_array, as_int32_array};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    assert_batches_eq, assert_batches_sorted_eq, assert_contains, exec_err, not_impl_err,
    plan_err, DFSchema, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    lit_with_metadata, udf_equals_hash, Accumulator, ColumnarValue, CreateFunction,
    CreateFunctionBody, LogicalPlanBuilder, OperateFunctionArg, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::range::range_udf;
use parking_lot::Mutex;
use regex::Regex;
use sqlparser::ast::Ident;
use sqlparser::tokenizer::Span;

/// test that casting happens on udfs.
/// c11 is f32, but `custom_sqrt` requires f64. Casting happens but the logical plan and
/// physical plan have the same schema.
#[tokio::test]
async fn csv_query_custom_udf_with_cast() -> Result<()> {
    let ctx = create_udf_context();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT avg(custom_sqrt(c11)) FROM aggregate_test_100";
    let actual = plan_and_collect(&ctx, sql).await?;

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
    +------------------------------------------+
    | avg(custom_sqrt(aggregate_test_100.c11)) |
    +------------------------------------------+
    | 0.6584408483418835                       |
    +------------------------------------------+
    "###);

    Ok(())
}

#[tokio::test]
async fn csv_query_avg_sqrt() -> Result<()> {
    let ctx = create_udf_context();
    register_aggregate_csv(&ctx).await?;
    // Note it is a different column (c12) than above (c11)
    let sql = "SELECT avg(custom_sqrt(c12)) FROM aggregate_test_100";
    let actual = plan_and_collect(&ctx, sql).await?;

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
    +------------------------------------------+
    | avg(custom_sqrt(aggregate_test_100.c12)) |
    +------------------------------------------+
    | 0.6706002946036459                       |
    +------------------------------------------+
    "###);

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
        DataType::Int32,
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
        format!("{plan}"),
        "Projection: t.a, t.b, my_add(t.a, t.b)\n  TableScan: t projection=[a, b]"
    );

    let result = DataFrame::new(ctx.state(), plan).collect().await?;

    insta::assert_snapshot!(batches_to_string(&result), @r###"
    +-----+-----+-----------------+
    | a   | b   | my_add(t.a,t.b) |
    +-----+-----+-----------------+
    | 1   | 2   | 3               |
    | 10  | 12  | 22              |
    | 10  | 12  | 22              |
    | 100 | 120 | 220             |
    +-----+-----+-----------------+
    "###);

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

#[derive(PartialEq, Hash)]
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

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(100))))
    }

    udf_equals_hash!(ScalarUDFImpl);
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
        DataType::Int32,
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
        "Internal error: UDF buggy_func returned a different number of rows than expected. Expected: 2, Got: 1"
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
    insta::assert_snapshot!(batches_to_string(&result), @r###"
    +-----+
    | a   |
    +-----+
    | 100 |
    | 100 |
    | 100 |
    | 100 |
    +-----+
    "###);

    let result = plan_and_collect(&ctx, "select get_100() a").await?;
    insta::assert_snapshot!(batches_to_string(&result), @r###"
    +-----+
    | a   |
    +-----+
    | 100 |
    +-----+
    "###);

    let result = plan_and_collect(&ctx, "select get_100() from t where a=999").await?;
    insta::assert_snapshot!(batches_to_string(&result), @r###"
    ++
    ++
    "###);

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
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))))),
    ));

    // Make sure that the UDF is used instead of the built-in function
    let result = plan_and_collect(&ctx, "select abs(a) a from t").await?;
    insta::assert_snapshot!(batches_to_string(&result), @r###"
    +---+
    | a |
    +---+
    | 1 |
    +---+
    "###);

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

    let dataframe = context.sql(sql).await?;
    assert_eq!(format!("{}", dataframe.logical_plan()), expected);
    Ok(())
}

#[tokio::test]
async fn case_sensitive_identifiers_user_defined_functions() -> Result<()> {
    let ctx = SessionContext::new();
    let arr = Int32Array::from(vec![1]);
    let batch = RecordBatch::try_from_iter(vec![("i", Arc::new(arr) as _)])?;
    ctx.register_batch("t", batch)?;

    let myfunc = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(array) = &args[0] else {
            panic!("should be array")
        };
        Ok(ColumnarValue::from(Arc::clone(array)))
    });

    ctx.register_udf(create_udf(
        "MY_FUNC",
        vec![DataType::Int32],
        DataType::Int32,
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

    insta::assert_snapshot!(batches_to_string(&result), @r###"
    +--------------+
    | MY_FUNC(t.i) |
    +--------------+
    | 1            |
    +--------------+
    "###);

    Ok(())
}

#[tokio::test]
async fn test_user_defined_functions_with_alias() -> Result<()> {
    let ctx = SessionContext::new();
    let arr = Int32Array::from(vec![1]);
    let batch = RecordBatch::try_from_iter(vec![("i", Arc::new(arr) as _)])?;
    ctx.register_batch("t", batch)?;

    let myfunc = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(array) = &args[0] else {
            panic!("should be array")
        };
        Ok(ColumnarValue::from(Arc::clone(array)))
    });

    let udf = create_udf(
        "dummy",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        myfunc,
    )
    .with_aliases(vec!["dummy_alias"]);

    ctx.register_udf(udf);

    let result = plan_and_collect(&ctx, "SELECT dummy(i) FROM t").await?;
    insta::assert_snapshot!(batches_to_string(&result), @r###"
    +------------+
    | dummy(t.i) |
    +------------+
    | 1          |
    +------------+
    "###);

    let alias_result = plan_and_collect(&ctx, "SELECT dummy_alias(i) FROM t").await?;
    insta::assert_snapshot!(batches_to_string(&alias_result), @r###"
    +------------+
    | dummy(t.i) |
    +------------+
    | 1          |
    +------------+
    "###);

    Ok(())
}

/// Volatile UDF that should append a different value to each row
#[derive(Debug, PartialEq, Hash)]
struct AddIndexToStringVolatileScalarUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
}

impl AddIndexToStringVolatileScalarUDF {
    fn new() -> Self {
        Self {
            name: "add_index_to_string".to_string(),
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
            return_type: DataType::Utf8,
        }
    }
}

impl ScalarUDFImpl for AddIndexToStringVolatileScalarUDF {
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), &args.args)?;
        let answer = match arg {
            // When called with static arguments, the result is returned as an array.
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => {
                let mut answer = vec![];
                for index in 1..=args.number_rows {
                    // When calling a function with immutable arguments, the result is returned with ")".
                    // Example: SELECT add_index_to_string('const_value') FROM table;
                    answer.push(index.to_string() + ") " + value);
                }
                answer
            }
            // The result is returned as an array when called with dynamic arguments.
            ColumnarValue::Array(array) => {
                let string_array = as_string_array(array);
                let mut counter = HashMap::<&str, u64>::new();
                string_array
                    .iter()
                    .map(|value| {
                        let value = value.expect("Unexpected null");
                        let index = counter.get(value).unwrap_or(&0) + 1;
                        counter.insert(value, index);

                        // When calling a function with mutable arguments, the result is returned with ".".
                        // Example: SELECT add_index_to_string(table.value) FROM table;
                        index.to_string() + ". " + value
                    })
                    .collect()
            }
            _ => unimplemented!(),
        };
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(answer))))
    }

    udf_equals_hash!(ScalarUDFImpl);
}

#[tokio::test]
async fn volatile_scalar_udf_with_params() -> Result<()> {
    {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec![
                "test_1", "test_1", "test_1", "test_2", "test_2", "test_1", "test_2",
            ]))],
        )?;
        let ctx = SessionContext::new();

        ctx.register_batch("t", batch)?;

        let get_new_str_udf = AddIndexToStringVolatileScalarUDF::new();

        ctx.register_udf(ScalarUDF::from(get_new_str_udf));

        let result =
            plan_and_collect(&ctx, "select add_index_to_string(t.a) AS str from t") // with dynamic function parameters
                .await?;
        let expected = [
            "+-----------+",
            "| str       |",
            "+-----------+",
            "| 1. test_1 |",
            "| 2. test_1 |",
            "| 3. test_1 |",
            "| 1. test_2 |",
            "| 2. test_2 |",
            "| 4. test_1 |",
            "| 3. test_2 |",
            "+-----------+",
        ];
        assert_batches_eq!(expected, &result);

        let result =
            plan_and_collect(&ctx, "select add_index_to_string('test') AS str from t") // with fixed function parameters
                .await?;
        let expected = [
            "+---------+",
            "| str     |",
            "+---------+",
            "| 1) test |",
            "| 2) test |",
            "| 3) test |",
            "| 4) test |",
            "| 5) test |",
            "| 6) test |",
            "| 7) test |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &result);

        let result =
            plan_and_collect(&ctx, "select add_index_to_string('test_value') as str") // with fixed function parameters
                .await?;
        let expected = [
            "+---------------+",
            "| str           |",
            "+---------------+",
            "| 1) test_value |",
            "+---------------+",
        ];
        assert_batches_eq!(expected, &result);
    }
    {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec![
                "test_1", "test_1", "test_1",
            ]))],
        )?;
        let ctx = SessionContext::new();

        ctx.register_batch("t", batch)?;

        let get_new_str_udf = AddIndexToStringVolatileScalarUDF::new();

        ctx.register_udf(ScalarUDF::from(get_new_str_udf));

        let result =
            plan_and_collect(&ctx, "select add_index_to_string(t.a) AS str from t")
                .await?;
        let expected = [
            "+-----------+", //
            "| str       |", //
            "+-----------+", //
            "| 1. test_1 |", //
            "| 2. test_1 |", //
            "| 3. test_1 |", //
            "+-----------+",
        ];
        assert_batches_eq!(expected, &result);
    }
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

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("dummy - not implemented")
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
async fn test_user_defined_sql_functions() -> Result<()> {
    let ctx = SessionContext::new();

    let expr_planners = ctx.expr_planners();

    assert!(!expr_planners.is_empty());

    Ok(())
}

#[tokio::test]
async fn deregister_udf() -> Result<()> {
    let cast2i64 = ScalarUDF::from(CastToI64UDF::new());
    let ctx = SessionContext::new();

    ctx.register_udf(cast2i64);

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
        not_impl_err!("Not called because the return_field_from_args is implemented")
    }

    /// This function returns the type of the first or second argument based on
    /// the third argument:
    ///
    /// 1. If the third argument is '0', return the type of the first argument
    /// 2. If the third argument is '1', return the type of the second argument
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 3 {
            return plan_err!("Expected 3 arguments, got {}.", args.arg_fields.len());
        }

        let take_idx = if let Some(take_idx) = args.scalar_arguments.get(2) {
            // This is for test only, safe to unwrap
            let take_idx = take_idx
                .unwrap()
                .try_as_str()
                .unwrap()
                .unwrap()
                .parse::<usize>()
                .unwrap();

            if take_idx == 0 || take_idx == 1 {
                take_idx
            } else {
                return plan_err!("The third argument must be 0 or 1, got: {take_idx}");
            }
        } else {
            return plan_err!(
                "The third argument must be a literal of type int64, but got {:?}",
                args.scalar_arguments.get(2)
            );
        };

        Ok(Field::new(
            self.name(),
            args.arg_fields[take_idx].data_type().to_owned(),
            true,
        )
        .into())
    }

    // The actual implementation
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [_arg0, _arg1, arg2] = take_function_args(self.name(), &args.args)?;
        let take_idx = match arg2 {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) if v == "0" => 0,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) if v == "1" => 1,
            _ => unreachable!(),
        };
        match &args.args[take_idx] {
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
        take.call(vec![col("smallint_col"), col("double_col"), lit("0")])
            .alias("take0"),
        take.call(vec![col("smallint_col"), col("double_col"), lit("1")])
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
#[derive(Debug, PartialEq, Hash)]
struct ScalarFunctionWrapper {
    name: String,
    expr: Expr,
    signature: Signature,
    return_type: DataType,
}

impl ScalarUDFImpl for ScalarFunctionWrapper {
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

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("dummy - not implemented")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let replacement = Self::replacement(&self.expr, &args)?;

        Ok(ExprSimplifyResult::Simplified(replacement))
    }

    udf_equals_hash!(ScalarUDFImpl);
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
                    "Placeholder `{placeholder}` parsing error: {e}!"
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
                definition.params.behavior.unwrap_or(Volatility::Volatile),
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
struct RecordingFunctionFactory {
    calls: Mutex<Vec<CreateFunction>>,
}

impl RecordingFunctionFactory {
    fn new() -> Self {
        Self::default()
    }

    /// return all the calls made to the factory
    fn calls(&self) -> Vec<CreateFunction> {
        self.calls.lock().clone()
    }
}

#[async_trait::async_trait]
impl FunctionFactory for RecordingFunctionFactory {
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
    let function_factory = Arc::new(RecordingFunctionFactory::new());
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
            panic!("Error creating function: {e}");
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
                span: Span::empty(),
            }),
            data_type: DataType::Utf8View,
            default_expr: None,
        }]),
        return_type: Some(DataType::Int32),
        params: CreateFunctionBody {
            language: Some(Ident {
                value: "plrust".into(),
                quote_style: None,
                span: Span::empty(),
            }),
            behavior: None,
            function_body: Some(lit(body)),
        },
        schema: Arc::new(schema),
    };

    assert_eq!(call, &expected);

    Ok(())
}

#[derive(Debug)]
struct MyRegexUdf {
    signature: Signature,
    regex: Regex,
}

impl PartialEq for MyRegexUdf {
    fn eq(&self, other: &Self) -> bool {
        let Self { signature, regex } = self;
        signature == &other.signature && regex.as_str() == other.regex.as_str()
    }
}

impl Hash for MyRegexUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self { signature, regex } = self;
        signature.hash(state);
        regex.as_str().hash(state);
    }
}

impl MyRegexUdf {
    fn new(pattern: &str) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
            regex: Regex::new(pattern).expect("regex"),
        }
    }

    fn matches(&self, value: Option<&str>) -> Option<bool> {
        Some(self.regex.is_match(value?))
    }
}

impl ScalarUDFImpl for MyRegexUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regex_udf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if matches!(args, [DataType::Utf8]) {
            Ok(DataType::Boolean)
        } else {
            plan_err!("regex_udf only accepts a Utf8 argument")
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.as_slice() {
            [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    self.matches(value.as_deref()),
                )))
            }
            [ColumnarValue::Array(values)] => {
                let mut builder = BooleanBuilder::with_capacity(values.len());
                for value in values.as_string::<i32>() {
                    builder.append_option(self.matches(value))
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            _ => exec_err!("regex_udf only accepts a Utf8 arguments"),
        }
    }

    udf_equals_hash!(ScalarUDFImpl);
}

#[tokio::test]
async fn test_parameterized_scalar_udf() -> Result<()> {
    let batch = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec!["foo", "bar", "foobar", "barfoo"])) as ArrayRef,
    )])?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let t = ctx.table("t").await?;
    let foo_udf = ScalarUDF::from(MyRegexUdf::new("fo{2}"));
    let bar_udf = ScalarUDF::from(MyRegexUdf::new("[Bb]ar"));

    let plan = LogicalPlanBuilder::from(t.into_optimized_plan()?)
        .filter(
            foo_udf
                .call(vec![col("text")])
                .and(bar_udf.call(vec![col("text")])),
        )?
        .filter(col("text").is_not_null())?
        .build()?;

    assert_eq!(
        format!("{plan}"),
        "Filter: t.text IS NOT NULL\n  Filter: regex_udf(t.text) AND regex_udf(t.text)\n    TableScan: t projection=[text]"
    );

    let actual = DataFrame::new(ctx.state(), plan).collect().await?;
    let expected = [
        "+--------+",
        "| text   |",
        "+--------+",
        "| foobar |",
        "| barfoo |",
        "+--------+",
    ];
    assert_batches_eq!(expected, &actual);

    ctx.deregister_table("t")?;
    Ok(())
}

fn create_udf_context() -> SessionContext {
    let ctx = SessionContext::new();
    // register a custom UDF
    ctx.register_udf(create_udf(
        "custom_sqrt",
        vec![DataType::Float64],
        DataType::Float64,
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
    let testdata = test_util::arrow_test_data();
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
    let testdata = test_util::parquet_test_data();
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

#[derive(Debug, PartialEq)]
struct MetadataBasedUdf {
    name: String,
    signature: Signature,
    metadata: HashMap<String, String>,
}

impl Hash for MetadataBasedUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            name,
            signature,
            metadata: _, // unhashable
        } = self;
        name.hash(state);
        signature.hash(state);
    }
}

impl MetadataBasedUdf {
    fn new(metadata: HashMap<String, String>) -> Self {
        // The name we return must be unique. Otherwise we will not call distinct
        // instances of this UDF. This is a small hack for the unit tests to get unique
        // names, but you could do something more elegant with the metadata.
        let name = format!("metadata_based_udf_{}", metadata.len());
        Self {
            name,
            signature: Signature::exact(vec![DataType::UInt64], Volatility::Immutable),
            metadata,
        }
    }
}

impl ScalarUDFImpl for MetadataBasedUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        unimplemented!(
            "this should never be called since return_field_from_args is implemented"
        );
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(self.name(), DataType::UInt64, true)
            .with_metadata(self.metadata.clone())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        assert_eq!(args.arg_fields.len(), 1);
        let should_double = args.arg_fields[0]
            .metadata()
            .get("modify_values")
            .map(|v| v == "double_output")
            .unwrap_or(false);
        let mulitplier = if should_double { 2 } else { 1 };

        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let array_values: Vec<_> = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|x| x * mulitplier))
                    .collect();
                let array_ref = Arc::new(UInt64Array::from(array_values)) as ArrayRef;
                Ok(ColumnarValue::Array(array_ref))
            }
            ColumnarValue::Scalar(value) => {
                let ScalarValue::UInt64(value) = value else {
                    return exec_err!("incorrect data type");
                };

                Ok(ColumnarValue::Scalar(ScalarValue::UInt64(
                    value.map(|v| v * mulitplier),
                )))
            }
        }
    }

    udf_equals_hash!(ScalarUDFImpl);
}

#[tokio::test]
async fn test_metadata_based_udf() -> Result<()> {
    let data_array = Arc::new(UInt64Array::from(vec![0, 5, 10, 15, 20])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("no_metadata", DataType::UInt64, true),
        Field::new("with_metadata", DataType::UInt64, true).with_metadata(
            [("modify_values".to_string(), "double_output".to_string())]
                .into_iter()
                .collect(),
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::clone(&data_array), Arc::clone(&data_array)],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let t = ctx.table("t").await?;
    let no_output_meta_udf = ScalarUDF::from(MetadataBasedUdf::new(HashMap::new()));
    let with_output_meta_udf = ScalarUDF::from(MetadataBasedUdf::new(
        [("output_metatype".to_string(), "custom_value".to_string())]
            .into_iter()
            .collect(),
    ));

    let plan = LogicalPlanBuilder::from(t.into_optimized_plan()?)
        .project(vec![
            no_output_meta_udf
                .call(vec![col("no_metadata")])
                .alias("meta_no_in_no_out"),
            no_output_meta_udf
                .call(vec![col("with_metadata")])
                .alias("meta_with_in_no_out"),
            with_output_meta_udf
                .call(vec![col("no_metadata")])
                .alias("meta_no_in_with_out"),
            with_output_meta_udf
                .call(vec![col("with_metadata")])
                .alias("meta_with_in_with_out"),
        ])?
        .build()?;

    let actual = DataFrame::new(ctx.state(), plan).collect().await?;

    // To test for output metadata handling, we set the expected values on the result
    // To test for input metadata handling, we check the numbers returned
    let mut output_meta = HashMap::new();
    let _ = output_meta.insert("output_metatype".to_string(), "custom_value".to_string());
    let expected_schema = Schema::new(vec![
        Field::new("meta_no_in_no_out", DataType::UInt64, true),
        Field::new("meta_with_in_no_out", DataType::UInt64, true),
        Field::new("meta_no_in_with_out", DataType::UInt64, true)
            .with_metadata(output_meta.clone()),
        Field::new("meta_with_in_with_out", DataType::UInt64, true)
            .with_metadata(output_meta.clone()),
    ]);

    let expected = record_batch!(
        ("meta_no_in_no_out", UInt64, [0, 5, 10, 15, 20]),
        ("meta_with_in_no_out", UInt64, [0, 10, 20, 30, 40]),
        ("meta_no_in_with_out", UInt64, [0, 5, 10, 15, 20]),
        ("meta_with_in_with_out", UInt64, [0, 10, 20, 30, 40])
    )?
    .with_schema(Arc::new(expected_schema))?;

    assert_eq!(expected, actual[0]);

    ctx.deregister_table("t")?;
    Ok(())
}

#[tokio::test]
async fn test_metadata_based_udf_with_literal() -> Result<()> {
    let ctx = SessionContext::new();
    let input_metadata: HashMap<String, String> =
        [("modify_values".to_string(), "double_output".to_string())]
            .into_iter()
            .collect();
    let input_metadata = FieldMetadata::from(input_metadata);
    let df = ctx.sql("select 0;").await?.select(vec![
        lit(5u64).alias_with_metadata("lit_with_doubling", Some(input_metadata.clone())),
        lit(5u64).alias("lit_no_doubling"),
        lit_with_metadata(5u64, Some(input_metadata))
            .alias("lit_with_double_no_alias_metadata"),
    ])?;

    let output_metadata: HashMap<String, String> =
        [("output_metatype".to_string(), "custom_value".to_string())]
            .into_iter()
            .collect();
    let custom_udf = ScalarUDF::from(MetadataBasedUdf::new(output_metadata.clone()));

    let plan = LogicalPlanBuilder::from(df.into_optimized_plan()?)
        .project(vec![
            custom_udf
                .call(vec![col("lit_with_doubling")])
                .alias("doubled_output"),
            custom_udf
                .call(vec![col("lit_no_doubling")])
                .alias("not_doubled_output"),
            custom_udf
                .call(vec![col("lit_with_double_no_alias_metadata")])
                .alias("double_without_alias_metadata"),
        ])?
        .build()?;

    let actual = DataFrame::new(ctx.state(), plan).collect().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("doubled_output", DataType::UInt64, false)
            .with_metadata(output_metadata.clone()),
        Field::new("not_doubled_output", DataType::UInt64, false)
            .with_metadata(output_metadata.clone()),
        Field::new("double_without_alias_metadata", DataType::UInt64, false)
            .with_metadata(output_metadata.clone()),
    ]));

    let expected = RecordBatch::try_new(
        schema,
        vec![
            create_array!(UInt64, [10]),
            create_array!(UInt64, [5]),
            create_array!(UInt64, [10]),
        ],
    )?;

    assert_eq!(expected, actual[0]);

    Ok(())
}

/// This UDF is to test extension handling, both on the input and output
/// sides. For the input, we will handle the data differently if there is
/// the canonical extension type Bool8. For the output we will add a
/// user defined extension type.
#[derive(Debug)]
struct ExtensionBasedUdf {
    name: String,
    signature: Signature,
}

impl Default for ExtensionBasedUdf {
    fn default() -> Self {
        Self {
            name: "canonical_extension_udf".to_string(),
            signature: Signature::exact(vec![DataType::Int8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ExtensionBasedUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Field::new("canonical_extension_udf", DataType::Utf8, true)
            .with_extension_type(MyUserExtentionType {})
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        assert_eq!(args.arg_fields.len(), 1);
        let input_field = args.arg_fields[0].as_ref();

        let output_as_bool = matches!(
            CanonicalExtensionType::try_from(input_field),
            Ok(CanonicalExtensionType::Bool8(_))
        );

        // If we have the extension type set, we are outputting a boolean value.
        // Otherwise we output a string representation of the numeric value.
        fn print_value(v: Option<i8>, as_bool: bool) -> Option<String> {
            v.map(|x| match as_bool {
                true => format!("{}", x != 0),
                false => format!("{x}"),
            })
        }

        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let array_values: Vec<_> = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .iter()
                    .map(|v| print_value(v, output_as_bool))
                    .collect();
                let array_ref = Arc::new(StringArray::from(array_values)) as ArrayRef;
                Ok(ColumnarValue::Array(array_ref))
            }
            ColumnarValue::Scalar(value) => {
                let ScalarValue::Int8(value) = value else {
                    return exec_err!("incorrect data type");
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(print_value(
                    *value,
                    output_as_bool,
                ))))
            }
        }
    }
}

struct MyUserExtentionType {}

impl ExtensionType for MyUserExtentionType {
    const NAME: &'static str = "my_user_extention_type";
    type Metadata = ();

    fn metadata(&self) -> &Self::Metadata {
        &()
    }

    fn serialize_metadata(&self) -> Option<String> {
        None
    }

    fn deserialize_metadata(
        _metadata: Option<&str>,
    ) -> std::result::Result<Self::Metadata, ArrowError> {
        Ok(())
    }

    fn supports_data_type(
        &self,
        data_type: &DataType,
    ) -> std::result::Result<(), ArrowError> {
        if let DataType::Utf8 = data_type {
            Ok(())
        } else {
            Err(ArrowError::InvalidArgumentError(
                "only utf8 supported".to_string(),
            ))
        }
    }

    fn try_new(
        _data_type: &DataType,
        _metadata: Self::Metadata,
    ) -> std::result::Result<Self, ArrowError> {
        Ok(Self {})
    }
}

#[tokio::test]
async fn test_extension_based_udf() -> Result<()> {
    let data_array = Arc::new(Int8Array::from(vec![0, 0, 10, 20])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("no_extension", DataType::Int8, true),
        Field::new("with_extension", DataType::Int8, true).with_extension_type(Bool8),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::clone(&data_array), Arc::clone(&data_array)],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let t = ctx.table("t").await?;
    let extension_based_udf = ScalarUDF::from(ExtensionBasedUdf::default());

    let plan = LogicalPlanBuilder::from(t.into_optimized_plan()?)
        .project(vec![
            extension_based_udf
                .call(vec![col("no_extension")])
                .alias("without_bool8_extension"),
            extension_based_udf
                .call(vec![col("with_extension")])
                .alias("with_bool8_extension"),
        ])?
        .build()?;

    let actual = DataFrame::new(ctx.state(), plan).collect().await?;

    // To test for output extension handling, we set the expected values on the result
    // To test for input extensions handling, we check the strings returned
    let expected_schema = Schema::new(vec![
        Field::new("without_bool8_extension", DataType::Utf8, true)
            .with_extension_type(MyUserExtentionType {}),
        Field::new("with_bool8_extension", DataType::Utf8, true)
            .with_extension_type(MyUserExtentionType {}),
    ]);

    let expected = record_batch!(
        ("without_bool8_extension", Utf8, ["0", "0", "10", "20"]),
        (
            "with_bool8_extension",
            Utf8,
            ["false", "false", "true", "true"]
        )
    )?
    .with_schema(Arc::new(expected_schema))?;

    assert_eq!(expected, actual[0]);

    ctx.deregister_table("t")?;
    Ok(())
}
