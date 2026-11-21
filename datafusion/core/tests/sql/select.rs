// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::collections::HashMap;

use super::*;
use arrow::array::AsArray;
use arrow::datatypes::Metadata;
use datafusion_common::{
    DataFusionError, ParamValues, ScalarValue, metadata::ScalarAndMetadata,
};
use insta::assert_snapshot;

#[tokio::test]
async fn test_list_query_parameters() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;

    let results = ctx
        .sql("SELECT * FROM test WHERE c1 = $1")
        .await?
        .with_param_values(vec![ScalarValue::from(3i32)])?
        .collect()
        .await?;
    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----+----+-------+
    | c1 | c2 | c3    |
    +----+----+-------+
    | 3  | 1  | false |
    | 3  | 10 | true  |
    | 3  | 2  | true  |
    | 3  | 3  | false |
    | 3  | 4  | true  |
    | 3  | 5  | false |
    | 3  | 6  | true  |
    | 3  | 7  | false |
    | 3  | 8  | true  |
    | 3  | 9  | false |
    +----+----+-------+
    ");
    Ok(())
}

#[tokio::test]
async fn test_named_query_parameters() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;

    // sql to statement then to logical plan with parameters
    let results = ctx
        .sql("SELECT c1, c2 FROM test WHERE c1 > $coo AND c1 < $foo")
        .await?
        .with_param_values(vec![
            ("foo", ScalarValue::UInt32(Some(3))),
            ("coo", ScalarValue::UInt32(Some(0))),
        ])?
        .collect()
        .await?;
    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----+----+
    | c1 | c2 |
    +----+----+
    | 1  | 1  |
    | 1  | 10 |
    | 1  | 2  |
    | 1  | 3  |
    | 1  | 4  |
    | 1  | 5  |
    | 1  | 6  |
    | 1  | 7  |
    | 1  | 8  |
    | 1  | 9  |
    | 2  | 1  |
    | 2  | 10 |
    | 2  | 2  |
    | 2  | 3  |
    | 2  | 4  |
    | 2  | 5  |
    | 2  | 6  |
    | 2  | 7  |
    | 2  | 8  |
    | 2  | 9  |
    +----+----+
    ");
    Ok(())
}

// Test prepare statement from sql to final result
// This test is equivalent with the test parallel_query_with_filter below but using prepare statement
#[tokio::test]
async fn test_prepare_statement() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;

    // sql to statement then to prepare logical plan with parameters
    let dataframe = ctx
        .sql("SELECT c1, c2 FROM test WHERE c1 > $2 AND c1 < $1")
        .await?;

    // prepare logical plan to logical plan without parameters
    let param_values = vec![ScalarValue::Int32(Some(3)), ScalarValue::Float64(Some(0.0))];
    let dataframe = dataframe.with_param_values(param_values)?;
    let results = dataframe.collect().await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----+----+
    | c1 | c2 |
    +----+----+
    | 1  | 1  |
    | 1  | 10 |
    | 1  | 2  |
    | 1  | 3  |
    | 1  | 4  |
    | 1  | 5  |
    | 1  | 6  |
    | 1  | 7  |
    | 1  | 8  |
    | 1  | 9  |
    | 2  | 1  |
    | 2  | 10 |
    | 2  | 2  |
    | 2  | 3  |
    | 2  | 4  |
    | 2  | 5  |
    | 2  | 6  |
    | 2  | 7  |
    | 2  | 8  |
    | 2  | 9  |
    +----+----+
    ");

    Ok(())
}

#[tokio::test]
async fn prepared_statement_type_coercion() -> Result<()> {
    let ctx = SessionContext::new();
    let signed_ints: Int32Array = vec![-1, 0, 1].into();
    let unsigned_ints: UInt64Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_from_iter(vec![
        ("signed", Arc::new(signed_ints) as ArrayRef),
        ("unsigned", Arc::new(unsigned_ints) as ArrayRef),
    ])?;
    ctx.register_batch("test", batch)?;
    let results = ctx.sql("SELECT signed, unsigned FROM test WHERE $1 >= signed AND signed <= $2 AND unsigned = $3")
        .await?
        .with_param_values(vec![
            ScalarValue::from(1_i64),
            ScalarValue::from(-1_i32),
            ScalarValue::from("1"),
        ])?
        .collect()
        .await?;
    assert_snapshot!(batches_to_sort_string(&results), @r"
    +--------+----------+
    | signed | unsigned |
    +--------+----------+
    | -1     | 1        |
    +--------+----------+
    ");
    Ok(())
}

#[tokio::test]
async fn test_parameter_type_coercion() -> Result<()> {
    let ctx = SessionContext::new();
    let signed_ints: Int32Array = vec![-1, 0, 1].into();
    let unsigned_ints: UInt64Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_from_iter(vec![
        ("signed", Arc::new(signed_ints) as ArrayRef),
        ("unsigned", Arc::new(unsigned_ints) as ArrayRef),
    ])?;
    ctx.register_batch("test", batch)?;
    let results = ctx.sql("SELECT signed, unsigned FROM test WHERE $foo >= signed AND signed <= $bar AND unsigned <= $baz AND unsigned = $str")
        .await?
        .with_param_values(vec![
            ("foo", ScalarValue::from(1_u64)),
            ("bar", ScalarValue::from(-1_i64)),
            ("baz", ScalarValue::from(2_i32)),
            ("str", ScalarValue::from("1")),
        ])?
        .collect().await?;
    assert_snapshot!(batches_to_sort_string(&results), @r"
    +--------+----------+
    | signed | unsigned |
    +--------+----------+
    | -1     | 1        |
    +--------+----------+
    ");
    Ok(())
}

#[tokio::test]
async fn test_parameter_invalid_types() -> Result<()> {
    let ctx = SessionContext::new();
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
        Some(1),
        Some(2),
        Some(3),
    ])]);
    let batch =
        RecordBatch::try_from_iter(vec![("list", Arc::new(list_array) as ArrayRef)])?;
    ctx.register_batch("test", batch)?;
    let results = ctx
        .sql("SELECT list FROM test WHERE list = $1")
        .await?
        .with_param_values(vec![ScalarValue::from(4_i32)])?
        .collect()
        .await;
    assert_snapshot!(results.unwrap_err().strip_backtrace(),
        @r"
    type_coercion
    caused by
    Error during planning: Cannot infer common argument type for comparison operation List(Int32) = Int32
    ");
    Ok(())
}

#[tokio::test]
async fn test_positional_parameter_not_bound() -> Result<()> {
    let ctx = SessionContext::new();
    let signed_ints: Int32Array = vec![-1, 0, 1].into();
    let unsigned_ints: UInt64Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_from_iter(vec![
        ("signed", Arc::new(signed_ints) as ArrayRef),
        ("unsigned", Arc::new(unsigned_ints) as ArrayRef),
    ])?;
    ctx.register_batch("test", batch)?;

    let query = "SELECT signed, unsigned FROM test \
            WHERE $1 >= signed AND signed <= $2 \
            AND unsigned <= $3 AND unsigned = $4";

    let results = ctx.sql(query).await?.collect().await;

    assert_eq!(
        results.unwrap_err().strip_backtrace(),
        "Execution error: Placeholder '$1' was not provided a value for execution."
    );

    let results = ctx
        .sql(query)
        .await?
        .with_param_values(vec![
            ScalarValue::from(4_i32),
            ScalarValue::from(-1_i64),
            ScalarValue::from(2_i32),
            ScalarValue::from("1"),
        ])?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +--------+----------+
    | signed | unsigned |
    +--------+----------+
    | -1     | 1        |
    +--------+----------+
    ");

    Ok(())
}

#[tokio::test]
async fn test_named_parameter_not_bound() -> Result<()> {
    let ctx = SessionContext::new();
    let signed_ints: Int32Array = vec![-1, 0, 1].into();
    let unsigned_ints: UInt64Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_from_iter(vec![
        ("signed", Arc::new(signed_ints) as ArrayRef),
        ("unsigned", Arc::new(unsigned_ints) as ArrayRef),
    ])?;
    ctx.register_batch("test", batch)?;

    let query = "SELECT signed, unsigned FROM test \
            WHERE $foo >= signed AND signed <= $bar \
            AND unsigned <= $baz AND unsigned = $str";

    let results = ctx.sql(query).await?.collect().await;

    assert_eq!(
        results.unwrap_err().strip_backtrace(),
        "Execution error: Placeholder '$foo' was not provided a value for execution."
    );

    let results = ctx
        .sql(query)
        .await?
        .with_param_values(vec![
            ("foo", ScalarValue::from(4_i32)),
            ("bar", ScalarValue::from(-1_i64)),
            ("baz", ScalarValue::from(2_i32)),
            ("str", ScalarValue::from("1")),
        ])?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +--------+----------+
    | signed | unsigned |
    +--------+----------+
    | -1     | 1        |
    +--------+----------+
    ");

    Ok(())
}

#[tokio::test]
async fn test_optimize_union_of_aliased_placeholders_before_binding() -> Result<()> {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = state
        .create_logical_plan("SELECT $1 AS a UNION ALL SELECT $2 AS a")
        .await?;

    let optimized = state.optimize(&plan)?;
    let bound = optimized.replace_params_with_values(&ParamValues::List(vec![
        ScalarValue::from("x").into(),
        ScalarValue::from("y").into(),
    ]))?;
    let batches = ctx.execute_logical_plan(bound).await?.collect().await?;

    assert_snapshot!(batches_to_sort_string(&batches), @r"
    +---+
    | a |
    +---+
    | x |
    | y |
    +---+
    ");

    Ok(())
}

#[tokio::test]
async fn test_query_parameters_with_metadata() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT $1, $2").await.unwrap();

    let metadata1 = HashMap::from([("some_key".to_string(), "some_value".to_string())]);
    let metadata2 =
        HashMap::from([("some_other_key".to_string(), "some_other_value".to_string())]);

    let df_with_params_replaced = df
        .with_param_values(ParamValues::List(vec![
            ScalarAndMetadata::new(
                ScalarValue::UInt32(Some(1)),
                Some(metadata1.clone().into()),
            ),
            ScalarAndMetadata::new(
                ScalarValue::Utf8(Some("two".to_string())),
                Some(metadata2.clone().into()),
            ),
        ]))
        .unwrap();

    let schema = df_with_params_replaced.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::UInt32);
    assert_eq!(schema.field(0).metadata(), &metadata1);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).metadata(), &metadata2);

    let batches = df_with_params_replaced.collect().await.unwrap();
    assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+-----+
    | $1 | $2  |
    +----+-----+
    | 1  | two |
    +----+-----+
    ");

    Ok(())
}

#[tokio::test]
async fn test_limit_offset_parameters_named() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .sql(
            "SELECT value FROM (VALUES (10), (20), (30)) AS t(value) \
             ORDER BY value LIMIT $rows OFFSET $skip",
        )
        .await?;
    assert_eq!(
        df.logical_plan().get_parameter_types()?,
        HashMap::from([
            ("$rows".to_string(), Some(DataType::Int64)),
            ("$skip".to_string(), Some(DataType::Int64)),
        ])
    );
    let results = df
        .with_param_values(vec![
            ("rows", ScalarValue::Int64(Some(1))),
            ("skip", ScalarValue::Int64(Some(1))),
        ])?
        .collect()
        .await?;
    datafusion::assert_batches_eq!(
        [
            "+-------+",
            "| value |",
            "+-------+",
            "| 20    |",
            "+-------+"
        ],
        &results
    );
    Ok(())
}

#[tokio::test]
async fn test_limit_offset_parameters_keep_field_metadata() -> Result<()> {
    let ctx = SessionContext::new();
    let metadata = HashMap::from([("some_key".to_string(), "some_value".to_string())]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, false).with_metadata(metadata.clone()),
    ]));
    ctx.register_batch(
        "t",
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 1]))])?,
    )?;

    for clause in ["LIMIT", "OFFSET"] {
        let sql = format!("SELECT $1 AS value FROM t WHERE value = $1 {clause} $1");
        let df = ctx.sql(&sql).await?;
        let results = df
            .with_param_values(ParamValues::List(vec![ScalarAndMetadata::new(
                ScalarValue::Int32(Some(1)),
                Some(metadata.clone().into()),
            )]))?
            .collect()
            .await?;
        datafusion::assert_batches_eq!(
            [
                "+-------+",
                "| value |",
                "+-------+",
                "| 1     |",
                "+-------+"
            ],
            &results
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_version_function() {
    let expected_version = format!(
        "Apache DataFusion {}, {} on {}",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::ARCH,
        std::env::consts::OS,
    );

    let ctx = SessionContext::new();
    let results = ctx
        .sql("select version()")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // since width of columns varies between platforms, we can't compare directly
    // so we just check that the version string is present

    // expect a single string column with a single row
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_columns(), 1);
    let version = results[0].column(0).as_string::<i32>();
    assert_eq!(version.len(), 1);

    assert_eq!(version.value(0), expected_version);
}

/// Regression test for https://github.com/apache/datafusion/issues/17513
/// See https://github.com/apache/datafusion/pull/17520
#[tokio::test]
async fn test_select_no_projection() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    // `create_ctx_with_partition` creates 10 rows per partition and we chose 1 partition
    let ctx = create_ctx_with_partition(&tmp_dir, 1).await?;

    let results = ctx.sql("SELECT FROM test").await?.collect().await?;
    // We should get all of the rows, just without any columns
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);
    // Check that none of the batches have any columns
    for batch in &results {
        assert_eq!(batch.num_columns(), 0);
    }
    // Sanity check the output, should be just empty columns
    assert_snapshot!(batches_to_sort_string(&results), @r"
    ++
    ++
    ++
    ");
    Ok(())
}

#[tokio::test]
async fn test_select_cast_date_literal_to_timestamp_overflow() -> Result<()> {
    let ctx = SessionContext::new();
    let err = ctx
        .sql("SELECT CAST(DATE '9999-12-31' AS TIMESTAMP)")
        .await?
        .collect()
        .await
        .unwrap_err();

    assert_contains!(
        err.to_string(),
        "Cannot cast Date32 value 2932896 to Timestamp(ns): converted value exceeds the representable i64 range"
    );
    Ok(())
}

// Regression test: a recursive CTE whose anchor aliases a computed column
// (`upper(val) AS val`) and whose recursive term leaves the same expression
// un-aliased must still produce batches whose schema field names come from
// the anchor term — especially when the outer query uses ORDER BY + LIMIT
// (the TopK path passes batch schemas through verbatim, so any drift in
// RecursiveQueryExec's emitted batches is visible downstream).
#[tokio::test]
async fn test_recursive_cte_batch_schema_stable_with_order_by_limit() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql(
        "CREATE TABLE records (\
            id VARCHAR NOT NULL, \
            parent_id VARCHAR, \
            ts TIMESTAMP NOT NULL, \
            val VARCHAR\
         )",
    )
    .await?
    .collect()
    .await?;
    ctx.sql(
        "INSERT INTO records VALUES \
            ('a00', NULL,  TIMESTAMP '2025-01-01 00:00:00', 'v_span'), \
            ('a01', 'a00', TIMESTAMP '2025-01-01 00:00:01', 'v_log_1'), \
            ('a02', 'a01', TIMESTAMP '2025-01-01 00:00:02', 'v_log_2'), \
            ('a03', 'a02', TIMESTAMP '2025-01-01 00:00:03', 'v_log_3'), \
            ('a04', 'a03', TIMESTAMP '2025-01-01 00:00:04', 'v_log_4'), \
            ('a05', 'a04', TIMESTAMP '2025-01-01 00:00:05', 'v_log_5')",
    )
    .await?
    .collect()
    .await?;

    let results = ctx
        .sql(
            "WITH RECURSIVE descendants AS (\
                SELECT id, parent_id, ts, upper(val) AS val \
                  FROM records WHERE id = 'a00' \
                UNION ALL \
                SELECT r.id, r.parent_id, r.ts, upper(r.val) \
                  FROM records r INNER JOIN descendants d ON r.parent_id = d.id \
             ) \
             SELECT id, parent_id, ts, val FROM descendants ORDER BY ts ASC LIMIT 10",
        )
        .await?
        .collect()
        .await?;

    let expected_names = ["id", "parent_id", "ts", "val"];
    assert!(!results.is_empty(), "expected at least one batch");
    for (i, batch) in results.iter().enumerate() {
        let schema = batch.schema();
        let actual_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            actual_names, expected_names,
            "batch {i} schema field names leaked from recursive branch"
        );
    }
    Ok(())
}

fn constructor_field_metadata(origin: &str) -> Metadata {
    Metadata::new()
        .with("ARROW:extension:name", "example.id")
        .with("ARROW:extension:metadata", "{}")
        .with("origin", origin)
        .with(format!("only_{origin}"), origin)
}

fn constructor_metadata_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true)
            .with_metadata(constructor_field_metadata("a")),
        Field::new("b", DataType::Int32, true)
            .with_metadata(constructor_field_metadata("b")),
    ]));
    ctx.register_batch(
        "t",
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(Int32Array::from(vec![Some(2), Some(3)])),
            ],
        )?,
    )?;
    Ok(ctx)
}

async fn assert_list_query(
    ctx: &SessionContext,
    sql: &str,
    expected_type: &DataType,
    expected_values: &dyn Array,
    expected_offsets: &[i32],
) -> Result<()> {
    let df = ctx.sql(sql).await?;
    assert_eq!(df.schema().field(0).data_type(), expected_type, "{sql}");
    let batches = df.collect().await?;
    let array = batches[0].column(0).as_list::<i32>();
    assert_eq!(array.data_type(), expected_type, "{sql}");
    assert_eq!(array.values().as_ref(), expected_values, "{sql}");
    assert_eq!(array.value_offsets(), expected_offsets, "{sql}");
    Ok(())
}

#[tokio::test]
async fn test_named_struct_constructor_field_metadata() -> Result<()> {
    let ctx = constructor_metadata_context()?;
    let df = ctx
        .sql("SELECT named_struct('left', a, 'right', b) AS s FROM t")
        .await?;
    let expected = DataType::Struct(
        vec![
            Field::new("left", DataType::Int32, true)
                .with_metadata(constructor_field_metadata("a")),
            Field::new("right", DataType::Int32, true)
                .with_metadata(constructor_field_metadata("b")),
        ]
        .into(),
    );
    assert_eq!(df.schema().field(0).data_type(), &expected);
    let batches = df.collect().await?;
    let values = batches[0].column(0).as_struct();
    assert_eq!(values.data_type(), &expected);
    assert_eq!(
        values.column(0).as_ref(),
        &Int32Array::from(vec![Some(1), None])
    );
    assert_eq!(
        values.column(1).as_ref(),
        &Int32Array::from(vec![Some(2), Some(3)])
    );
    Ok(())
}

#[tokio::test]
async fn test_make_array_constructor_field_metadata() -> Result<()> {
    use arrow::array::{Int64Array, StructArray};
    let ctx = constructor_metadata_context()?;
    let cases: Vec<(_, _, ArrayRef)> = vec![
        (
            ["a", "b"],
            constructor_field_metadata("a").with("only_b", "b"),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(3)])),
        ),
        (
            ["b", "a"],
            constructor_field_metadata("b").with("only_a", "a"),
            Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(3), None])),
        ),
        (
            ["NULL", "a"],
            constructor_field_metadata("a"),
            Arc::new(Int32Array::from(vec![None, Some(1), None, None])),
        ),
        (
            ["a", "CAST(2 AS BIGINT)"],
            Metadata::new().with("origin", "a").with("only_a", "a"),
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(2)])),
        ),
        (
            ["CAST(2 AS BIGINT)", "a"],
            Metadata::new().with("origin", "a").with("only_a", "a"),
            Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(2), None])),
        ),
    ];
    for (args, metadata, values) in cases {
        // Apply the same value/metadata contract directly and through Struct layers.
        for depth in 0..=2 {
            let args = args.map(|arg| {
                (0..depth).fold(arg.to_string(), |arg, _| {
                    format!("named_struct('v', {arg})")
                })
            });
            let mut values = Arc::clone(&values);
            let mut field = Field::new_list_field(values.data_type().clone(), true)
                .with_metadata(metadata.clone());
            for _ in 0..depth {
                values = Arc::new(StructArray::new(
                    vec![field.with_name("v")].into(),
                    vec![values],
                    None,
                ));
                field = Field::new_list_field(values.data_type().clone(), true);
            }
            assert_list_query(
                &ctx,
                &format!("SELECT make_array({}, {}) FROM t", args[0], args[1]),
                &DataType::List(Arc::new(field)),
                values.as_ref(),
                &[0, 2, 4],
            )
            .await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_scalar_constructor_field_metadata() -> Result<()> {
    let ctx = SessionContext::new();
    let metadata = Metadata::new()
        .with("ARROW:extension:name", "example.id")
        .with("ARROW:extension:metadata", "{}")
        .with("origin", "input");
    let value = ScalarAndMetadata::new(
        ScalarValue::Int32(Some(7)),
        Some(metadata.clone().into()),
    );
    for (sql, expected) in [
        (
            "SELECT make_array($1) AS v",
            DataType::List(Arc::new(
                Field::new_list_field(DataType::Int32, true)
                    .with_metadata(metadata.clone()),
            )),
        ),
        (
            "SELECT make_array($1, CAST(8 AS BIGINT)) AS v",
            DataType::List(Arc::new(
                Field::new_list_field(DataType::Int64, true).with_metadata(
                    HashMap::from([("origin".to_string(), "input".to_string())]),
                ),
            )),
        ),
        (
            "SELECT named_struct('value', $1) AS v",
            DataType::Struct(
                vec![
                    Field::new("value", DataType::Int32, true)
                        .with_metadata(metadata.clone()),
                ]
                .into(),
            ),
        ),
    ] {
        let df = ctx
            .sql(sql)
            .await?
            .with_param_values(ParamValues::List(vec![value.clone()]))?;
        assert_eq!(df.schema().field(0).data_type(), &expected, "{sql}");
        let batches = df.collect().await?;
        assert_eq!(batches[0].column(0).data_type(), &expected, "{sql}");
        assert_eq!(batches[0].num_rows(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn test_make_array_nested_field_metadata() -> Result<()> {
    use arrow::array::StructArray;
    use arrow::buffer::NullBuffer;
    let ctx = SessionContext::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (origin, nullable, values) in [
        ("a", false, vec![Some(1), Some(2)]),
        ("b", true, vec![Some(3), None]),
    ] {
        let field = Field::new("v", DataType::Int32, nullable)
            .with_metadata(HashMap::from([("origin".to_string(), origin.to_string())]));
        columns.push(Arc::new(StructArray::new(
            vec![field].into(),
            vec![Arc::new(Int32Array::from(values))],
            None,
        )));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", columns[0].data_type().clone(), false),
        Field::new("b", columns[1].data_type().clone(), false),
    ]));
    ctx.register_batch("t", RecordBatch::try_new(schema, columns)?)?;
    for (args, origin, values, validity, offsets) in [
        (
            "a, b",
            "a",
            vec![Some(1), Some(3), Some(2), None],
            None,
            [0, 2, 4],
        ),
        (
            "b, a",
            "b",
            vec![Some(3), Some(1), None, Some(2)],
            None,
            [0, 2, 4],
        ),
        (
            "named_struct('v', a['v']), named_struct('v', b['v'])",
            "a",
            vec![Some(1), Some(3), Some(2), None],
            None,
            [0, 2, 4],
        ),
        (
            "a, b, NULL",
            "a",
            vec![Some(1), Some(3), None, Some(2), None, None],
            Some(vec![true, true, false, true, true, false]),
            [0, 3, 6],
        ),
        (
            "NULL, a, b",
            "a",
            vec![None, Some(1), Some(3), None, Some(2), None],
            Some(vec![false, true, true, false, true, true]),
            [0, 3, 6],
        ),
    ] {
        let field = Field::new("v", DataType::Int32, true)
            .with_metadata(HashMap::from([("origin".to_string(), origin.to_string())]));
        let values = StructArray::new(
            vec![field].into(),
            vec![Arc::new(Int32Array::from(values))],
            validity.map(NullBuffer::from),
        );
        let expected = DataType::new_list(values.data_type().clone(), true);
        assert_list_query(
            &ctx,
            &format!("SELECT make_array({args}) FROM t"),
            &expected,
            &values,
            &offsets,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_coalesce_struct_field_metadata() -> Result<()> {
    use arrow::array::{Int64Array, NullArray, StructArray};
    use arrow::buffer::NullBuffer;
    use datafusion::logical_expr::{Volatility, create_udf};

    let ctx = SessionContext::new();
    let first_ordinary = Metadata::new()
        .with("origin", "first")
        .with("only_first", "1");
    let first_extension = first_ordinary
        .clone()
        .with("ARROW:extension:name", "example.id")
        .with("ARROW:extension:metadata", "{}");
    let second_ordinary = Metadata::new()
        .with("origin", "second")
        .with("only_second", "2");

    let i32_ext: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("v", DataType::Int32, false)
                .with_metadata(first_extension.clone()),
        ]
        .into(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
        Some(NullBuffer::from(vec![true, false, false, false])),
    ));
    let i32_plain: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("v", DataType::Int32, true).with_metadata(second_ordinary.clone()),
        ]
        .into(),
        vec![Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(30),
            Some(40),
        ]))],
        None,
    ));
    let i64_opt: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("v", DataType::Int64, true).with_metadata(second_ordinary.clone()),
        ]
        .into(),
        vec![Arc::new(Int64Array::from(vec![
            Some(100),
            None,
            Some(300),
            Some(400),
        ]))],
        Some(NullBuffer::from(vec![false, true, true, false])),
    ));
    let null_child: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("v", DataType::Null, true).with_metadata(
                Metadata::new()
                    .with("ARROW:extension:name", "example.null")
                    .with("origin", "ignored")
                    .with("null_only", "ignored"),
            ),
        ]
        .into(),
        vec![Arc::new(NullArray::new(4))],
        Some(NullBuffer::from(vec![false, false, true, false])),
    ));

    for (udf_name, data_type) in [
        ("volatile_i32_ext", i32_ext.data_type().clone()),
        ("volatile_i64_opt", i64_opt.data_type().clone()),
        ("volatile_null_child", null_child.data_type().clone()),
    ] {
        ctx.register_udf(create_udf(
            udf_name,
            vec![data_type.clone()],
            data_type,
            Volatility::Volatile,
            Arc::new(|args| Ok(args[0].clone())),
        ));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("i32_ext", i32_ext.data_type().clone(), true),
        Field::new("i32_plain", i32_plain.data_type().clone(), false),
        Field::new("i64_opt", i64_opt.data_type().clone(), true),
        Field::new("null_child", null_child.data_type().clone(), true),
    ]));
    ctx.register_batch(
        "t",
        RecordBatch::try_new(schema, vec![i32_ext, i32_plain, i64_opt, null_child])?,
    )?;

    let cases = [
        (
            [
                "SELECT coalesce(i32_ext, i32_plain) FROM t",
                "SELECT coalesce(volatile_i32_ext(i32_ext), i32_plain) FROM t",
            ],
            false,
            StructArray::new(
                vec![
                    Field::new("v", DataType::Int32, true)
                        .with_metadata(first_extension.clone().with("only_second", "2")),
                ]
                .into(),
                vec![Arc::new(Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(30),
                    Some(40),
                ]))],
                None,
            ),
        ),
        (
            [
                "SELECT coalesce(NULL, i32_ext, i64_opt) FROM t",
                "SELECT coalesce(NULL, volatile_i32_ext(i32_ext), i64_opt) FROM t",
            ],
            true,
            StructArray::new(
                vec![
                    Field::new("v", DataType::Int64, true)
                        .with_metadata(first_ordinary.with("only_second", "2")),
                ]
                .into(),
                vec![Arc::new(Int64Array::from(vec![
                    Some(1),
                    None,
                    Some(300),
                    None,
                ]))],
                Some(NullBuffer::from(vec![true, true, true, false])),
            ),
        ),
        (
            [
                "SELECT coalesce(i64_opt, i32_ext) FROM t",
                "SELECT coalesce(volatile_i64_opt(i64_opt), i32_ext) FROM t",
            ],
            true,
            StructArray::new(
                vec![
                    Field::new("v", DataType::Int64, true)
                        .with_metadata(second_ordinary.with("only_first", "1")),
                ]
                .into(),
                vec![Arc::new(Int64Array::from(vec![
                    Some(1),
                    None,
                    Some(300),
                    None,
                ]))],
                Some(NullBuffer::from(vec![true, true, true, false])),
            ),
        ),
        (
            [
                "SELECT coalesce(null_child, i32_ext) FROM t",
                "SELECT coalesce(volatile_null_child(null_child), i32_ext) FROM t",
            ],
            true,
            StructArray::new(
                vec![
                    Field::new("v", DataType::Int32, true).with_metadata(first_extension),
                ]
                .into(),
                vec![Arc::new(Int32Array::from(vec![Some(1), None, None, None]))],
                Some(NullBuffer::from(vec![true, false, true, false])),
            ),
        ),
    ];

    for (queries, expected_nullable, expected) in cases {
        for sql in queries {
            let df = ctx.sql(sql).await?;
            assert_eq!(
                df.schema().field(0).data_type(),
                expected.data_type(),
                "{sql}"
            );
            assert_eq!(
                df.schema().field(0).is_nullable(),
                expected_nullable,
                "{sql}"
            );
            let batches = df.collect().await?;
            let actual = batches[0].column(0).as_struct();
            assert_eq!(actual.data_type(), expected.data_type(), "{sql}");
            assert_eq!(actual.nulls(), expected.nulls(), "{sql}");
            assert_eq!(actual, &expected, "{sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_reordered_struct_coercion_field_metadata() -> Result<()> {
    use arrow::array::{Int64Array, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use datafusion::logical_expr::{Volatility, create_udf};

    let ctx = SessionContext::new();
    let a_first = Metadata::new()
        .with("ARROW:extension:name", "example.id")
        .with("ARROW:extension:metadata", "{}")
        .with("origin", "first")
        .with("only_first", "1");
    let a_second = Metadata::new()
        .with("origin", "second")
        .with("only_second", "2");
    let b_left = Metadata::new()
        .with("ARROW:extension:name", "example.label")
        .with("ARROW:extension:metadata", "{}")
        .with("label", "left");
    let b_right = Metadata::new()
        .with("ARROW:extension:name", "example.label")
        .with("ARROW:extension:metadata", "{}")
        .with("label", "right");

    let inner_ab = StructArray::new(
        vec![
            Field::new("a", DataType::Int32, false).with_metadata(a_first),
            Field::new("b", DataType::Utf8, true).with_metadata(b_left.clone()),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec![None, Some("unused")])),
        ],
        None,
    );
    let s_ab: ArrayRef = Arc::new(StructArray::new(
        inner_ab.fields().clone(),
        inner_ab.columns().to_vec(),
        Some(NullBuffer::from(vec![true, false])),
    ));
    let s_ba: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("b", DataType::Utf8, false).with_metadata(b_right),
            Field::new("a", DataType::Int64, true).with_metadata(a_second),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec!["x", "y"])),
            Arc::new(Int64Array::from(vec![Some(30), None])),
        ],
        None,
    ));
    let nested_ab: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("nested", inner_ab.data_type().clone(), false)].into(),
        vec![Arc::new(inner_ab)],
        Some(NullBuffer::from(vec![true, false])),
    ));
    let nested_ba: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("nested", s_ba.data_type().clone(), true)].into(),
        vec![Arc::clone(&s_ba)],
        None,
    ));

    for (udf_name, data_type) in [
        ("volatile_s_ab", s_ab.data_type().clone()),
        ("volatile_nested_ab", nested_ab.data_type().clone()),
    ] {
        ctx.register_udf(create_udf(
            udf_name,
            vec![data_type.clone()],
            data_type,
            Volatility::Volatile,
            Arc::new(|args| Ok(args[0].clone())),
        ));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("s_ab", s_ab.data_type().clone(), true),
        Field::new("s_ba", s_ba.data_type().clone(), false),
        Field::new("nested_ab", nested_ab.data_type().clone(), true),
        Field::new("nested_ba", nested_ba.data_type().clone(), false),
    ]));
    ctx.register_batch(
        "t",
        RecordBatch::try_new(schema, vec![s_ab, s_ba, nested_ab, nested_ba])?,
    )?;

    let expected_a = Field::new("a", DataType::Int64, true).with_metadata(
        Metadata::new()
            .with("origin", "first")
            .with("only_first", "1")
            .with("only_second", "2"),
    );
    let expected_b = Field::new("b", DataType::Utf8, true).with_metadata(b_left);

    let expected_top = StructArray::new(
        vec![expected_b.clone(), expected_a.clone()].into(),
        vec![
            Arc::new(StringArray::from(vec![None, Some("y")])),
            Arc::new(Int64Array::from(vec![Some(10), None])),
        ],
        None,
    );
    for sql in [
        "SELECT coalesce(s_ab, s_ba) FROM t",
        "SELECT coalesce(volatile_s_ab(s_ab), s_ba) FROM t",
    ] {
        let df = ctx.sql(sql).await?;
        assert_eq!(
            df.schema().field(0).data_type(),
            expected_top.data_type(),
            "{sql}"
        );
        assert!(!df.schema().field(0).is_nullable(), "{sql}");
        let batches = df.collect().await?;
        let actual = batches[0].column(0).as_struct();
        assert_eq!(actual.data_type(), expected_top.data_type(), "{sql}");
        assert_eq!(actual.null_count(), 0, "{sql}");
        assert_eq!(actual, &expected_top, "{sql}");
    }

    // An enclosing ordered Struct keeps the first inner field order.
    let expected_inner = StructArray::new(
        vec![expected_a.clone(), expected_b.clone()].into(),
        vec![
            Arc::new(Int64Array::from(vec![Some(10), None])),
            Arc::new(StringArray::from(vec![None, Some("y")])),
        ],
        None,
    );
    let expected_nested = StructArray::new(
        vec![Field::new(
            "nested",
            expected_inner.data_type().clone(),
            true,
        )]
        .into(),
        vec![Arc::new(expected_inner)],
        None,
    );
    for sql in [
        "SELECT coalesce(nested_ab, nested_ba) FROM t",
        "SELECT coalesce(volatile_nested_ab(nested_ab), nested_ba) FROM t",
    ] {
        let df = ctx.sql(sql).await?;
        assert_eq!(
            df.schema().field(0).data_type(),
            expected_nested.data_type(),
            "{sql}"
        );
        assert!(!df.schema().field(0).is_nullable(), "{sql}");
        let batches = df.collect().await?;
        let actual = batches[0].column(0).as_struct();
        assert_eq!(actual.data_type(), expected_nested.data_type(), "{sql}");
        assert_eq!(actual.null_count(), 0, "{sql}");
        assert_eq!(actual, &expected_nested, "{sql}");
    }

    let expected_array_values = StructArray::new(
        vec![expected_b, expected_a].into(),
        vec![
            Arc::new(StringArray::from(vec![None, Some("x"), None, Some("y")])),
            Arc::new(Int64Array::from(vec![Some(10), Some(30), None, None])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let expected_list_type =
        DataType::new_list(expected_array_values.data_type().clone(), true);
    let df = ctx.sql("SELECT make_array(s_ab, s_ba) FROM t").await?;
    assert_eq!(df.schema().field(0).data_type(), &expected_list_type);
    let batches = df.collect().await?;
    let list = batches[0].column(0).as_list::<i32>();
    assert_eq!(list.data_type(), &expected_list_type);
    assert_eq!(list.value_offsets(), &[0, 2, 4]);
    assert_eq!(
        list.values().as_struct().data_type(),
        expected_array_values.data_type()
    );
    assert_eq!(list.values().as_ref(), &expected_array_values);
    Ok(())
}

#[tokio::test]
async fn test_struct_coercion_field_metadata_conflict() -> Result<()> {
    let ctx = SessionContext::new();
    let struct_field = |col_name: &str, ext_name: &str| {
        let child = Field::new("v", DataType::Utf8, true)
            .with_metadata(Metadata::new().with("ARROW:extension:name", ext_name));
        Field::new(col_name, DataType::Struct(vec![child].into()), true)
    };
    let s1 = struct_field("s1", "example.first");
    let s2 = struct_field("s2", "example.second");
    let n1 = Field::new(
        "n1",
        DataType::Struct(vec![Field::new("nested", s1.data_type().clone(), true)].into()),
        true,
    );
    let n2 = Field::new(
        "n2",
        DataType::Struct(vec![Field::new("nested", s2.data_type().clone(), true)].into()),
        true,
    );
    ctx.register_batch(
        "t",
        RecordBatch::new_empty(Arc::new(Schema::new(vec![s1, s2, n1, n2]))),
    )?;

    for sql in [
        "SELECT coalesce(s1, s2) FROM t",
        "SELECT coalesce(NULL, s1, s2) FROM t",
        "SELECT coalesce(n1, n2) FROM t",
        "SELECT make_array(s1, s2) FROM t",
        "SELECT make_array(n1, n2) FROM t",
    ] {
        let err = ctx.sql(sql).await.unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)), "{sql}: {err}");
        for expected in [
            "Cannot merge field metadata",
            "example.first",
            "example.second",
        ] {
            assert!(err.to_string().contains(expected), "{sql}: {err}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_make_array_constructor_field_metadata_conflict() -> Result<()> {
    let ctx = SessionContext::new();
    let fields = ["example.first", "example.second"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            Field::new(format!("c{i}"), DataType::Int32, true).with_metadata(
                HashMap::from([("ARROW:extension:name".to_string(), name.to_string())]),
            )
        })
        .collect::<Vec<_>>();
    ctx.register_batch("t", RecordBatch::new_empty(Arc::new(Schema::new(fields))))?;
    for expression in [
        "make_array(c0, c1)",
        "make_array(named_struct('v', c0), named_struct('v', c1))",
        "make_array(named_struct('v', c0), NULL, named_struct('v', c1))",
        "make_array(named_struct('v', named_struct('w', c0)), named_struct('v', named_struct('w', c1)))",
        "make_array(named_struct('v', c0, 'other', 0), named_struct('other', 0, 'v', c1))",
        "coalesce(named_struct('v', c0), named_struct('v', c1))",
    ] {
        let err = ctx
            .sql(&format!("SELECT {expression} FROM t"))
            .await
            .unwrap_err();
        for expected in [
            "Cannot merge field metadata",
            "example.first",
            "example.second",
        ] {
            assert!(err.to_string().contains(expected), "{expression}: {err}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_conditional_functions_field_metadata() -> Result<()> {
    let ctx = SessionContext::new();
    let a_metadata = HashMap::from([
        ("shared".to_string(), "a".to_string()),
        ("only_a".to_string(), "a".to_string()),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Utf8, true).with_metadata(HashMap::from([(
            "only_flag".to_string(),
            "flag".to_string(),
        )])),
        Field::new("a", DataType::Utf8, true).with_metadata(a_metadata.clone()),
        Field::new("b", DataType::Utf8, true).with_metadata(HashMap::from([
            ("shared".to_string(), "b".to_string()),
            ("only_b".to_string(), "b".to_string()),
        ])),
    ]));
    ctx.register_batch("t", RecordBatch::new_empty(schema))?;

    let merged = HashMap::from([
        ("shared".to_string(), "a".to_string()),
        ("only_a".to_string(), "a".to_string()),
        ("only_b".to_string(), "b".to_string()),
    ]);
    // `df.schema()` is the schema of the plan as first built, before these
    // functions are rewritten to CASE.
    for (sql, expected) in [
        ("SELECT coalesce(a, b) FROM t", &merged),
        ("SELECT nvl(a, b) FROM t", &merged),
        ("SELECT ifnull(a, b) FROM t", &merged),
        ("SELECT nvl2(flag, a, b) FROM t", &merged),
        ("SELECT coalesce(NULL, a) FROM t", &a_metadata),
    ] {
        let df = ctx.sql(sql).await?;
        assert_eq!(df.schema().field(0).metadata(), expected, "{sql}");
    }
    Ok(())
}
