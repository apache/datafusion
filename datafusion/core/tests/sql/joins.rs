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

use insta::assert_snapshot;

use datafusion::assert_batches_eq;
use datafusion::catalog::MemTable;
use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use datafusion::test_util::register_unbounded_file_with_ordering;
use datafusion_sql::unparser::plan_to_sql;

use super::*;

#[tokio::test]
async fn join_change_in_planner() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new().unwrap();
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone()).unwrap();
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    // Specify the ordering:
    let file_sort_order = vec![
        [col("a1")]
            .into_iter()
            .map(|e| {
                let ascending = true;
                let nulls_first = false;
                e.sort(ascending, nulls_first)
            })
            .collect::<Vec<_>>(),
    ];
    register_unbounded_file_with_ordering(
        &ctx,
        schema.clone(),
        &left_file_path,
        "left",
        file_sort_order.clone(),
    )?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone()).unwrap();
    register_unbounded_file_with_ordering(
        &ctx,
        schema,
        &right_file_path,
        "right",
        file_sort_order,
    )?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_no_order_on_filter() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new().unwrap();
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone()).unwrap();
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
        Field::new("a3", DataType::UInt32, false),
    ]));
    // Specify the ordering:
    let file_sort_order = vec![
        [col("a1")]
            .into_iter()
            .map(|e| {
                let ascending = true;
                let nulls_first = false;
                e.sort(ascending, nulls_first)
            })
            .collect::<Vec<_>>(),
    ];
    register_unbounded_file_with_ordering(
        &ctx,
        schema.clone(),
        &left_file_path,
        "left",
        file_sort_order.clone(),
    )?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone()).unwrap();
    register_unbounded_file_with_ordering(
        &ctx,
        schema,
        &right_file_path,
        "right",
        file_sort_order,
    )?;
    let sql = "SELECT * FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a3 > t2.a3 + 3 AND t1.a3 < t2.a3 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a3@0 AS Int64) > CAST(a3@1 AS Int64) + 3 AND CAST(a3@0 AS Int64) < CAST(a3@1 AS Int64) + 10
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2, a3], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2, a3], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_change_in_planner_without_sort() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new()?;
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    let left_source = FileStreamProvider::new_file(schema.clone(), left_file_path);
    let left = StreamConfig::new(Arc::new(left_source));
    ctx.register_table("left", Arc::new(StreamTable::new(Arc::new(left))))?;

    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    let right_source = FileStreamProvider::new_file(schema, right_file_path);
    let right = StreamConfig::new(Arc::new(right_source));
    ctx.register_table("right", Arc::new(StreamTable::new(Arc::new(right))))?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_change_in_planner_without_sort_not_allowed() -> Result<()> {
    let config = SessionConfig::new()
        .with_target_partitions(8)
        .with_allow_symmetric_joins_without_pruning(false);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new()?;
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    let left_source = FileStreamProvider::new_file(schema.clone(), left_file_path);
    let left = StreamConfig::new(Arc::new(left_source));
    ctx.register_table("left", Arc::new(StreamTable::new(Arc::new(left))))?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    let right_source = FileStreamProvider::new_file(schema.clone(), right_file_path);
    let right = StreamConfig::new(Arc::new(right_source));
    ctx.register_table("right", Arc::new(StreamTable::new(Arc::new(right))))?;
    let df = ctx.sql("SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10").await?;
    match df.create_physical_plan().await {
        Ok(_) => panic!("Expecting error."),
        Err(e) => {
            assert_eq!(
                e.strip_backtrace(),
                "SanityCheckPlan\ncaused by\nError during planning: Join operation cannot operate on a non-prunable stream without enabling the 'allow_symmetric_joins_without_pruning' configuration flag"
            )
        }
    }
    Ok(())
}

#[tokio::test]
async fn join_using_uppercase_column() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "UPPER",
        DataType::UInt32,
        false,
    )]));
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("uppercase-column.csv");
    let mut file = File::create(file_path.clone())?;
    file.write_all("0".as_bytes())?;
    drop(file);

    let ctx = SessionContext::new();
    ctx.register_csv(
        "test",
        file_path.to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let dataframe = ctx
        .sql(
            r#"
        SELECT test."UPPER" FROM "test"
        INNER JOIN (
            SELECT test."UPPER" FROM "test"
        ) AS selection USING ("UPPER")
        ;
        "#,
        )
        .await?;

    assert_batches_eq!(
        [
            "+-------+",
            "| UPPER |",
            "+-------+",
            "| 0     |",
            "+-------+",
        ],
        &dataframe.collect().await?
    );

    Ok(())
}

// Issue #17359: https://github.com/apache/datafusion/issues/17359
#[tokio::test]
async fn unparse_cross_join() -> Result<()> {
    let ctx = SessionContext::new();

    let j1_schema = Arc::new(Schema::new(vec![
        Field::new("j1_id", DataType::Int32, true),
        Field::new("j1_string", DataType::Utf8, true),
    ]));
    let j2_schema = Arc::new(Schema::new(vec![
        Field::new("j2_id", DataType::Int32, true),
        Field::new("j2_string", DataType::Utf8, true),
    ]));

    ctx.register_table("j1", Arc::new(MemTable::try_new(j1_schema, vec![vec![]])?))?;
    ctx.register_table("j2", Arc::new(MemTable::try_new(j2_schema, vec![vec![]])?))?;

    let df = ctx
        .sql(
            r#"
            select j1.j1_id, j2.j2_string
            from j1, j2
            where j2.j2_id = 0
            "#,
        )
        .await?;

    let unopt_sql = plan_to_sql(df.logical_plan())?;
    assert_snapshot!(unopt_sql, @"SELECT j1.j1_id, j2.j2_string FROM j1 CROSS JOIN j2 WHERE (j2.j2_id = 0)");

    let optimized_plan = df.into_optimized_plan()?;

    let opt_sql = plan_to_sql(&optimized_plan)?;
    assert_snapshot!(opt_sql, @"SELECT j1.j1_id, j2.j2_string FROM j1 CROSS JOIN j2 WHERE (j2.j2_id = 0)");

    Ok(())
}

/// Reproducer for a bug where `flatten_dictionary_array` in the hash join
/// InList pushdown code returned the dictionary value pool (unique entries)
/// instead of the expanded logical array.  When the build side spans
/// multiple batches, `concat_batches` deduplicates the dictionary, making
/// `values().len() < keys().len()`.  Combined with a second (non-dictionary)
/// join key column, this caused a length mismatch panic in
/// `StructArray::new`.
///
/// This test exercises the full SQL → physical plan → execution path.
#[tokio::test]
async fn test_hash_join_dictionary_column_inlist_pushdown() -> Result<()> {
    use arrow::compute::cast;

    let dict_type =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    // --- Build side: two batches with overlapping dictionary values ---
    // After concat_batches the dictionary deduplicates: values=["a","b","c"],
    // keys=[0,1,0,2] → values().len()=3, keys().len()=4.
    let build_schema = Arc::new(Schema::new(vec![
        Field::new("key_str", dict_type.clone(), false),
        Field::new("key_int", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        Arc::clone(&build_schema),
        vec![
            cast(
                &StringArray::from(vec!["a", "b"]),
                &dict_type,
            )?,
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::clone(&build_schema),
        vec![
            cast(
                &StringArray::from(vec!["a", "c"]),
                &dict_type,
            )?,
            Arc::new(Int32Array::from(vec![3, 4])),
        ],
    )?;

    let build_table =
        MemTable::try_new(Arc::clone(&build_schema), vec![vec![batch1, batch2]])?;

    // --- Probe side: a larger table so the optimizer picks a hash join ---
    let probe_schema = Arc::new(Schema::new(vec![
        Field::new("key_str", dict_type.clone(), false),
        Field::new("key_int", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]));

    let probe_batch = RecordBatch::try_new(
        Arc::clone(&probe_schema),
        vec![
            cast(
                &StringArray::from(vec!["a", "b", "a", "c", "d", "a"]),
                &dict_type,
            )?,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
        ],
    )?;

    let probe_table =
        MemTable::try_new(Arc::clone(&probe_schema), vec![vec![probe_batch]])?;

    // Use a single-partition, small-batch config so that the build side
    // is collected (CollectLeft) and InList pushdown is attempted.
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(2);
    let ctx = SessionContext::new_with_config(config);

    ctx.register_table("build_side", Arc::new(build_table))?;
    ctx.register_table("probe_side", Arc::new(probe_table))?;

    // Multi-column join: triggers the StructArray path in
    // build_struct_inlist_values that panicked before the fix.
    let sql = "\
        SELECT p.key_str, p.key_int, p.val \
        FROM probe_side p \
        INNER JOIN build_side b \
          ON p.key_str = b.key_str AND p.key_int = b.key_int \
        ORDER BY p.val";

    let batches = ctx.sql(sql).await?.collect().await?;

    assert_batches_eq!(
        [
            "+---------+---------+-----+",
            "| key_str | key_int | val |",
            "+---------+---------+-----+",
            "| a       | 1       | 10  |",
            "| b       | 2       | 20  |",
            "| a       | 3       | 30  |",
            "| c       | 4       | 40  |",
            "+---------+---------+-----+",
        ],
        &batches
    );

    Ok(())
}
