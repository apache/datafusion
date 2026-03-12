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

fn count_physical_plan_nodes_by_name(
    plan: &Arc<dyn ExecutionPlan>,
    node_name: &str,
) -> usize {
    let mut count = 0;
    let mut stack = vec![Arc::clone(plan)];

    while let Some(node) = stack.pop() {
        if node.name() == node_name {
            count += 1;
        }
        stack.extend(node.children().into_iter().cloned());
    }

    count
}

fn batch_slicing_ctx(batch_size: usize) -> SessionContext {
    batch_slicing_ctx_with_partitions(batch_size, 1)
}

fn batch_slicing_ctx_with_partitions(
    batch_size: usize,
    target_partitions: usize,
) -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(target_partitions),
    )
}

fn assert_total_and_max_batch_rows(
    results: &[RecordBatch],
    total_rows: usize,
    max_batch_rows: usize,
) {
    assert_eq!(
        results.iter().map(RecordBatch::num_rows).sum::<usize>(),
        total_rows
    );
    assert!(
        results
            .iter()
            .all(|batch| batch.num_rows() <= max_batch_rows)
    );
}

fn nested_int32_batch(rows: &[&[&[i32]]]) -> Result<RecordBatch> {
    let mut list_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

    for row in rows {
        for values in *row {
            for &value in *values {
                list_builder.values().values().append_value(value);
            }
            list_builder.values().append(true);
        }
        list_builder.append(true);
    }

    Ok(RecordBatch::try_from_iter(vec![(
        "nested",
        Arc::new(list_builder.finish()) as ArrayRef,
    )])?)
}

#[tokio::test]
async fn unnest_chunks_high_fanout_batches() -> Result<()> {
    let ctx = batch_slicing_ctx(4);

    let shape_ids = Arc::new(UInt32Array::from(vec![1, 2, 3])) as ArrayRef;
    let tag_ids = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(11), Some(12), Some(13)]),
        Some(vec![Some(21), Some(22), Some(23)]),
        Some(vec![Some(31), Some(32), Some(33)]),
    ])) as ArrayRef;

    let batch =
        RecordBatch::try_from_iter(vec![("shape_id", shape_ids), ("tag_id", tag_ids)])?;

    ctx.register_batch("shapes", batch)?;

    let results = ctx
        .table("shapes")
        .await?
        .unnest_columns(&["tag_id"])?
        .collect()
        .await?;

    assert_eq!(
        results
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![3, 3, 3]
    );
    assert_total_and_max_batch_rows(&results, 9, 4);

    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+--------+
    | shape_id | tag_id |
    +----------+--------+
    | 1        | 11     |
    | 1        | 12     |
    | 1        | 13     |
    | 2        | 21     |
    | 2        | 22     |
    | 2        | 23     |
    | 3        | 31     |
    | 3        | 32     |
    | 3        | 33     |
    +----------+--------+
    "
    );

    Ok(())
}

/// Tests that the batch-slicing estimation path respects `batch_size` when
/// multiple unnested columns have different lengths per row.
///
/// `estimate_row_output_rows` picks `max(len_col_a, len_col_b)` per row to
/// determine how many rows a batch-slice will expand to.  With `batch_size=4`
/// and row expansion vectors [3, 2, 3] this should yield three output batches
/// whose sizes never exceed 4.
#[tokio::test]
async fn unnest_chunks_multi_col_different_lengths() -> Result<()> {
    let ctx = batch_slicing_ctx(4);

    // id: 1, 2, 3
    // list_a: [1], [2, 3], [4, 5, 6]          (lengths 1, 2, 3)
    // list_b: [10, 11, 12], [20], [30, 31]     (lengths 3, 1, 2)
    // max lengths per row: 3, 2, 3  -> total output rows: 8
    let ids = Arc::new(UInt32Array::from(vec![1u32, 2, 3])) as ArrayRef;
    let list_a = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2), Some(3)]),
        Some(vec![Some(4), Some(5), Some(6)]),
    ])) as ArrayRef;
    let list_b = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(10), Some(11), Some(12)]),
        Some(vec![Some(20)]),
        Some(vec![Some(30), Some(31)]),
    ])) as ArrayRef;

    let batch = RecordBatch::try_from_iter(vec![
        ("id", ids),
        ("list_a", list_a),
        ("list_b", list_b),
    ])?;
    ctx.register_batch("t", batch)?;

    let results = ctx
        .table("t")
        .await?
        .unnest_columns(&["list_a", "list_b"])?
        .collect()
        .await?;

    assert_total_and_max_batch_rows(&results, 8, 4);

    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----+--------+--------+
    | id | list_a | list_b |
    +----+--------+--------+
    | 1  |        | 11     |
    | 1  |        | 12     |
    | 1  | 1      | 10     |
    | 2  | 2      | 20     |
    | 2  | 3      |        |
    | 3  | 4      | 30     |
    | 3  | 5      | 31     |
    | 3  | 6      |        |
    +----+--------+--------+
    "
    );

    Ok(())
}

/// Tests that `preserve_nulls = false` correctly drops null-list rows when the
/// batch-slicing estimation path is active.
///
/// `estimate_row_output_rows` returns 0 for null rows under
/// `preserve_nulls = false`, so those rows are folded into adjacent slices
/// without contributing output rows.
#[tokio::test]
async fn unnest_chunks_preserve_nulls_false() -> Result<()> {
    let ctx = batch_slicing_ctx(2);

    // list: [1, 2], null, [3], null
    // id:   A,      B,    C,   D
    // With preserve_nulls=false: only rows A and C produce output (3 rows total)
    let list_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
        None,
    ])) as ArrayRef;
    let id_array = Arc::new(StringArray::from(vec!["A", "B", "C", "D"])) as ArrayRef;

    let batch = RecordBatch::try_from_iter(vec![("id", id_array), ("list", list_array)])?;
    ctx.register_batch("t", batch)?;

    let results = ctx
        .table("t")
        .await?
        .unnest_columns_with_options(
            &["list"],
            UnnestOptions::new().with_preserve_nulls(false),
        )?
        .collect()
        .await?;

    assert_total_and_max_batch_rows(&results, 3, 2);

    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----+------+
    | id | list |
    +----+------+
    | A  | 1    |
    | A  | 2    |
    | C  | 3    |
    +----+------+
    "
    );

    Ok(())
}

/// Tests that recursive unnest (`depth > 1`) preserves correctness under a
/// small `batch_size`.
///
/// Recursive / staged unnest now bypasses row-slice chunking entirely because
/// changing batch boundaries can leak through downstream repartitioning and
/// reorder parent-row groups. This test verifies that the conservative
/// fallback still produces the complete result.
#[tokio::test]
async fn unnest_chunks_recursive_single_row_fallback() -> Result<()> {
    let ctx = batch_slicing_ctx(4);

    let batch = nested_int32_batch(&[&[&[1, 2], &[3]], &[&[4, 5]], &[&[6], &[7, 8, 9]]])?;
    ctx.register_batch("t", batch)?;

    let results = ctx
        .sql("SELECT unnest(unnest(nested)) AS val FROM t")
        .await?
        .collect()
        .await?;

    assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 9);

    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +-----+
    | val |
    +-----+
    | 1   |
    | 2   |
    | 3   |
    | 4   |
    | 5   |
    | 6   |
    | 7   |
    | 8   |
    | 9   |
    +-----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_chunks_stacked_unnest_preserves_order() -> Result<()> {
    let ctx = batch_slicing_ctx(2);

    let batch = nested_int32_batch(&[&[&[1, 2], &[3]], &[&[4], &[5, 6]]])?;
    ctx.register_batch("t", batch)?;

    let dataframe = ctx
        .sql(
            "SELECT unnest(nested1) AS val, original \
             FROM (SELECT unnest(nested) AS nested1, nested AS original FROM t)",
        )
        .await?;
    let physical_plan = dataframe.clone().create_physical_plan().await?;
    assert_eq!(
        count_physical_plan_nodes_by_name(&physical_plan, "UnnestExec"),
        2
    );

    let results = dataframe.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +-----+---------------+
    | val | original      |
    +-----+---------------+
    | 1   | [[1, 2], [3]] |
    | 2   | [[1, 2], [3]] |
    | 3   | [[1, 2], [3]] |
    | 4   | [[4], [5, 6]] |
    | 5   | [[4], [5, 6]] |
    | 6   | [[4], [5, 6]] |
    +-----+---------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_chunks_recursive_repeated_refs_preserve_order() -> Result<()> {
    let ctx = batch_slicing_ctx_with_partitions(2, 4);

    ctx.sql(
        "CREATE TABLE recursive_unnest_table AS VALUES \
         (struct([1], 'a'), [[[1],[2]],[[1,1]]], [struct([1],[[1,2]])]), \
         (struct([2], 'b'), [[[3,4],[5]],[[null,6],null,[7,8]]], [struct([2],[[3],[4]])])",
    )
    .await?;

    let dataframe = ctx
        .sql(
            "SELECT \
               unnest(column2), \
               unnest(unnest(column2)), \
               unnest(unnest(unnest(column2))), \
               unnest(unnest(unnest(column2))) + 1 \
             FROM recursive_unnest_table",
        )
        .await?;

    let physical_plan = dataframe.clone().create_physical_plan().await?;
    let plan_text = displayable(physical_plan.as_ref())
        .indent(false)
        .to_string();
    assert!(
        plan_text.contains("RepartitionExec: partitioning=RoundRobinBatch(4)"),
        "expected repeated-reference unnest plan to include RoundRobinBatch(4), got:\n{plan_text}"
    );
    assert!(
        count_physical_plan_nodes_by_name(&physical_plan, "UnnestExec") >= 1,
        "expected repeated-reference unnest plan to include UnnestExec, got:\n{plan_text}"
    );

    let results = dataframe.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +----------------------------------------+------------------------------------------------+--------------------------------------------------------+-------------------------------------------------------------------+
    | UNNEST(recursive_unnest_table.column2) | UNNEST(UNNEST(recursive_unnest_table.column2)) | UNNEST(UNNEST(UNNEST(recursive_unnest_table.column2))) | UNNEST(UNNEST(UNNEST(recursive_unnest_table.column2))) + Int64(1) |
    +----------------------------------------+------------------------------------------------+--------------------------------------------------------+-------------------------------------------------------------------+
    | [[1], [2]]                             | [1]                                            | 1                                                      | 2                                                                 |
    | [[1, 1]]                               | [2]                                            |                                                        |                                                                   |
    | [[1], [2]]                             | [1, 1]                                         | 2                                                      | 3                                                                 |
    | [[1, 1]]                               |                                                |                                                        |                                                                   |
    | [[1], [2]]                             | [1]                                            | 1                                                      | 2                                                                 |
    | [[1, 1]]                               | [2]                                            | 1                                                      | 2                                                                 |
    | [[1], [2]]                             | [1, 1]                                         |                                                        |                                                                   |
    | [[1, 1]]                               |                                                |                                                        |                                                                   |
    | [[3, 4], [5]]                          | [3, 4]                                         | 3                                                      | 4                                                                 |
    | [[, 6], , [7, 8]]                      | [5]                                            | 4                                                      | 5                                                                 |
    | [[3, 4], [5]]                          | [, 6]                                          | 5                                                      | 6                                                                 |
    | [[, 6], , [7, 8]]                      |                                                |                                                        |                                                                   |
    |                                        | [7, 8]                                         |                                                        |                                                                   |
    | [[3, 4], [5]]                          | [3, 4]                                         |                                                        |                                                                   |
    | [[, 6], , [7, 8]]                      | [5]                                            | 6                                                      | 7                                                                 |
    | [[3, 4], [5]]                          | [, 6]                                          |                                                        |                                                                   |
    | [[, 6], , [7, 8]]                      |                                                |                                                        |                                                                   |
    |                                        | [7, 8]                                         |                                                        |                                                                   |
    | [[3, 4], [5]]                          |                                                | 7                                                      | 8                                                                 |
    | [[, 6], , [7, 8]]                      |                                                | 8                                                      | 9                                                                 |
    +----------------------------------------+------------------------------------------------+--------------------------------------------------------+-------------------------------------------------------------------+
    "
    );

    Ok(())
}
