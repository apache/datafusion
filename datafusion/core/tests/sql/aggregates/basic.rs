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
use arrow::datatypes::TimeUnit;
use datafusion::common::test_util::batches_to_string;
use datafusion_catalog::MemTable;
use datafusion_common::ScalarValue;
use std::cmp::min;

/// Helper function to create the commonly used UInt32 indexed UTF-8 dictionary data type
fn string_dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

use insta::assert_snapshot;
/// Helper functions for aggregate tests with dictionary columns and nulls
/// Creates a dictionary array with null values in the dictionary
fn create_test_dict(
    values: &[Option<&str>],
    indices: &[Option<u32>],
) -> DictionaryArray<UInt32Type> {
    let dict_values = StringArray::from(values.to_vec());
    let dict_indices = UInt32Array::from(indices.to_vec());
    DictionaryArray::new(dict_indices, Arc::new(dict_values))
}

/// Creates test data with both dictionary columns and value column
struct TestData {
    dict_null_keys: DictionaryArray<UInt32Type>,
    dict_null_vals: DictionaryArray<UInt32Type>,
    values: Int32Array,
    schema: Arc<Schema>,
}

impl TestData {
    fn new() -> Self {
        // Create dictionary with null keys
        let dict_null_keys = create_test_dict(
            &[Some("group_a"), Some("group_b")],
            &[
                Some(0), // group_a
                None,    // null key
                Some(1), // group_b
                None,    // null key
                Some(0), // group_a
            ],
        );

        // Create dictionary with null values
        let dict_null_vals = create_test_dict(
            &[Some("group_x"), None, Some("group_y")],
            &[
                Some(0), // group_x
                Some(1), // null value
                Some(2), // group_y
                Some(1), // null value
                Some(0), // group_x
            ],
        );

        // Create test data with nulls
        let values = Int32Array::from(vec![Some(1), None, Some(2), None, Some(3)]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict_null_keys", string_dict_type(), true),
            Field::new("dict_null_vals", string_dict_type(), true),
            Field::new("value", DataType::Int32, true),
        ]));

        Self {
            dict_null_keys,
            dict_null_vals,
            values,
            schema,
        }
    }

    /// Creates extended test data for more comprehensive testing
    fn new_extended() -> Self {
        // Create dictionary with null values in the dictionary array
        let dict_null_vals = create_test_dict(
            &[Some("group_a"), None, Some("group_b")],
            &[
                Some(0), // group_a
                Some(1), // null value
                Some(2), // group_b
                Some(1), // null value
                Some(0), // group_a
                Some(1), // null value
                Some(2), // group_b
                Some(1), // null value
            ],
        );

        // Create dictionary with null keys
        let dict_null_keys = create_test_dict(
            &[Some("group_x"), Some("group_y"), Some("group_z")],
            &[
                Some(0), // group_x
                None,    // null key
                Some(1), // group_y
                None,    // null key
                Some(0), // group_x
                None,    // null key
                Some(2), // group_z
                None,    // null key
            ],
        );

        // Create test data with nulls
        let values = Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            Some(5),
            None,
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict_null_vals", string_dict_type(), true),
            Field::new("dict_null_keys", string_dict_type(), true),
            Field::new("value", DataType::Int32, true),
        ]));

        Self {
            dict_null_keys,
            dict_null_vals,
            values,
            schema,
        }
    }

    /// Creates test data for MIN/MAX testing with varied values
    fn new_for_min_max() -> Self {
        let dict_null_keys = create_test_dict(
            &[Some("group_a"), Some("group_b"), Some("group_c")],
            &[
                Some(0),
                Some(1),
                Some(0),
                Some(2),
                None,
                None, // group_a, group_b, group_a, group_c, null, null
            ],
        );

        let dict_null_vals = create_test_dict(
            &[Some("group_x"), None, Some("group_y")],
            &[
                Some(0),
                Some(1),
                Some(0),
                Some(2),
                Some(1),
                Some(1), // group_x, null, group_x, group_y, null, null
            ],
        );

        let values =
            Int32Array::from(vec![Some(5), Some(1), Some(3), Some(7), Some(2), None]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict_null_keys", string_dict_type(), true),
            Field::new("dict_null_vals", string_dict_type(), true),
            Field::new("value", DataType::Int32, true),
        ]));

        Self {
            dict_null_keys,
            dict_null_vals,
            values,
            schema,
        }
    }

    /// Creates test data for MEDIAN testing with varied values
    fn new_for_median() -> Self {
        let dict_null_vals = create_test_dict(
            &[Some("group_a"), None, Some("group_b")],
            &[Some(0), Some(1), Some(2), Some(1), Some(0)],
        );

        let dict_null_keys = create_test_dict(
            &[Some("group_x"), Some("group_y"), Some("group_z")],
            &[Some(0), None, Some(1), None, Some(2)],
        );

        let values = Int32Array::from(vec![Some(1), None, Some(5), Some(3), Some(7)]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict_null_vals", string_dict_type(), true),
            Field::new("dict_null_keys", string_dict_type(), true),
            Field::new("value", DataType::Int32, true),
        ]));

        Self {
            dict_null_keys,
            dict_null_vals,
            values,
            schema,
        }
    }

    /// Creates test data for FIRST_VALUE/LAST_VALUE testing
    fn new_for_first_last() -> Self {
        let dict_null_keys = create_test_dict(
            &[Some("group_a"), Some("group_b")],
            &[Some(0), None, Some(1), None, Some(0)],
        );

        let dict_null_vals = create_test_dict(
            &[Some("group_x"), None, Some("group_y")],
            &[Some(0), Some(1), Some(2), Some(1), Some(0)],
        );

        let values = Int32Array::from(vec![None, Some(1), Some(2), Some(3), None]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict_null_keys", string_dict_type(), true),
            Field::new("dict_null_vals", string_dict_type(), true),
            Field::new("value", DataType::Int32, true),
        ]));

        Self {
            dict_null_keys,
            dict_null_vals,
            values,
            schema,
        }
    }
}

/// ## Test Helper Function Guide
///
/// This file provides several helper functions to reduce repetitive test patterns:
///
/// ### For tests using TestData:
/// - `run_complete_snapshot_test()` - Complete test: setup → SQL → assert_snapshot!
/// - `run_complete_sorted_snapshot_test()` - Same as above but with sorted output
/// - `run_snapshot_test()` - Setup and execute, returns results for custom assertions
///
/// ### For custom setups:
/// - `run_simple_snapshot_test()` - Execute SQL on existing context, returns results
///
/// ### Supporting functions:
/// - `create_test_dict()` - Create dictionary arrays with slices (preferred)
/// - `setup_test_contexts()` - Setup single and multi-partition contexts
/// - `test_query_consistency()` - Execute and verify consistency across partitions
///
/// ### Usage examples:
/// ```rust
/// // Simple complete test
/// run_complete_snapshot_test(&test_data, "SELECT * FROM t", @"expected output").await?;
///
/// // Multiple tests with different data
/// let results = run_snapshot_test(&test_data, "SELECT * FROM t").await?;
/// assert_snapshot!(batches_to_string(&results), @"expected");
/// ```
async fn setup_test_contexts(
    test_data: &TestData,
) -> Result<(SessionContext, SessionContext)> {
    // Single partition context
    let ctx_single = create_context_with_partitions(test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_context_with_partitions(test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with the specified number of partitions and registers test data
async fn create_context_with_partitions(
    test_data: &TestData,
    num_partitions: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(num_partitions),
    );

    let batches = split_test_data_into_batches(test_data, num_partitions)?;
    let provider = MemTable::try_new(test_data.schema.clone(), batches)?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(ctx)
}
/// Splits test data into multiple batches for partitioning
fn split_test_data_into_batches(
    test_data: &TestData,
    num_partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    debug_assert!(num_partitions > 0, "num_partitions must be greater than 0");
    let total_len = test_data.values.len();
    let chunk_size = total_len.div_ceil(num_partitions); // Ensure we cover all data

    let mut batches = Vec::new();
    let mut start = 0;

    while start < total_len {
        let end = min(start + chunk_size, total_len);
        let len = end - start;

        if len > 0 {
            let batch = RecordBatch::try_new(
                test_data.schema.clone(),
                vec![
                    Arc::new(test_data.dict_null_keys.slice(start, len)),
                    Arc::new(test_data.dict_null_vals.slice(start, len)),
                    Arc::new(test_data.values.slice(start, len)),
                ],
            )?;
            batches.push(vec![batch]);
        }
        start = end;
    }

    Ok(batches)
}

/// Executes a query on both single and multi-partition contexts and verifies consistency
async fn test_query_consistency(
    ctx_single: &SessionContext,
    ctx_multi: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let df_single = ctx_single.sql(sql).await?;
    let results_single = df_single.collect().await?;

    let df_multi = ctx_multi.sql(sql).await?;
    let results_multi = df_multi.collect().await?;

    // Verify results are consistent between single and multiple partitions
    assert_eq!(
        batches_to_string(&results_single),
        batches_to_string(&results_multi),
        "Results should be identical between single and multiple partitions"
    );

    Ok(results_single)
}

#[tokio::test]
async fn csv_query_array_agg_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT array_agg(distinct c2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;

    // The results for this query should be something like the following:
    //    +------------------------------------------+
    //    | ARRAY_AGG(DISTINCT aggregate_test_100.c2) |
    //    +------------------------------------------+
    //    | [4, 2, 3, 5, 1]                          |
    //    +------------------------------------------+
    // Since ARRAY_AGG(DISTINCT) ordering is nondeterministic, check the schema and contents.
    assert_eq!(
        *actual[0].schema(),
        Schema::new(vec![Field::new_list(
            "array_agg(DISTINCT aggregate_test_100.c2)",
            Field::new_list_field(DataType::UInt32, true),
            true
        ),])
    );

    // We should have 1 row containing a list
    let column = actual[0].column(0);
    assert_eq!(column.len(), 1);
    let scalar_vec = ScalarValue::convert_array_to_scalar_vec(&column)?;
    let mut scalars = scalar_vec[0].clone();

    // workaround lack of Ord of ScalarValue
    let cmp = |a: &ScalarValue, b: &ScalarValue| {
        a.try_cmp(b).expect("Can compare ScalarValues")
    };
    scalars.sort_by(cmp);
    assert_eq!(
        scalars,
        vec![
            ScalarValue::UInt32(Some(1)),
            ScalarValue::UInt32(Some(2)),
            ScalarValue::UInt32(Some(3)),
            ScalarValue::UInt32(Some(4)),
            ScalarValue::UInt32(Some(5))
        ]
    );

    Ok(())
}

#[tokio::test]
async fn count_partitioned() -> Result<()> {
    let results =
        execute_with_partition("SELECT count(c1), count(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----------------+----------------+
    | count(test.c1) | count(test.c2) |
    +----------------+----------------+
    | 40             | 40             |
    +----------------+----------------+
    ");
    Ok(())
}

#[tokio::test]
async fn count_aggregated() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, count(c2) FROM test GROUP BY c1", 4).await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----+----------------+
    | c1 | count(test.c2) |
    +----+----------------+
    | 0  | 10             |
    | 1  | 10             |
    | 2  | 10             |
    | 3  | 10             |
    +----+----------------+
    ");
    Ok(())
}

#[tokio::test]
async fn count_aggregated_cube() -> Result<()> {
    let results = execute_with_partition(
        "SELECT c1, c2, count(c3) FROM test GROUP BY CUBE (c1, c2) ORDER BY c1, c2",
        4,
    )
    .await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----+----+----------------+
    | c1 | c2 | count(test.c3) |
    +----+----+----------------+
    |    |    | 40             |
    |    | 1  | 4              |
    |    | 10 | 4              |
    |    | 2  | 4              |
    |    | 3  | 4              |
    |    | 4  | 4              |
    |    | 5  | 4              |
    |    | 6  | 4              |
    |    | 7  | 4              |
    |    | 8  | 4              |
    |    | 9  | 4              |
    | 0  |    | 10             |
    | 0  | 1  | 1              |
    | 0  | 10 | 1              |
    | 0  | 2  | 1              |
    | 0  | 3  | 1              |
    | 0  | 4  | 1              |
    | 0  | 5  | 1              |
    | 0  | 6  | 1              |
    | 0  | 7  | 1              |
    | 0  | 8  | 1              |
    | 0  | 9  | 1              |
    | 1  |    | 10             |
    | 1  | 1  | 1              |
    | 1  | 10 | 1              |
    | 1  | 2  | 1              |
    | 1  | 3  | 1              |
    | 1  | 4  | 1              |
    | 1  | 5  | 1              |
    | 1  | 6  | 1              |
    | 1  | 7  | 1              |
    | 1  | 8  | 1              |
    | 1  | 9  | 1              |
    | 2  |    | 10             |
    | 2  | 1  | 1              |
    | 2  | 10 | 1              |
    | 2  | 2  | 1              |
    | 2  | 3  | 1              |
    | 2  | 4  | 1              |
    | 2  | 5  | 1              |
    | 2  | 6  | 1              |
    | 2  | 7  | 1              |
    | 2  | 8  | 1              |
    | 2  | 9  | 1              |
    | 3  |    | 10             |
    | 3  | 1  | 1              |
    | 3  | 10 | 1              |
    | 3  | 2  | 1              |
    | 3  | 3  | 1              |
    | 3  | 4  | 1              |
    | 3  | 5  | 1              |
    | 3  | 6  | 1              |
    | 3  | 7  | 1              |
    | 3  | 8  | 1              |
    | 3  | 9  | 1              |
    +----+----+----------------+
    ");
    Ok(())
}

async fn run_count_distinct_integers_aggregated_scenario(
    partitions: Vec<Vec<(&str, u64)>>,
) -> Result<Vec<RecordBatch>> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_group", DataType::Utf8, false),
        Field::new("c_int8", DataType::Int8, false),
        Field::new("c_int16", DataType::Int16, false),
        Field::new("c_int32", DataType::Int32, false),
        Field::new("c_int64", DataType::Int64, false),
        Field::new("c_uint8", DataType::UInt8, false),
        Field::new("c_uint16", DataType::UInt16, false),
        Field::new("c_uint32", DataType::UInt32, false),
        Field::new("c_uint64", DataType::UInt64, false),
    ]));

    for (i, partition) in partitions.iter().enumerate() {
        let filename = format!("partition-{i}.csv");
        let file_path = tmp_dir.path().join(filename);
        let mut file = File::create(file_path)?;
        for row in partition {
            let row_str = format!(
                "{},{}\n",
                row.0,
                // Populate values for each of the integer fields in the
                // schema.
                (0..8)
                    .map(|_| { row.1.to_string() })
                    .collect::<Vec<_>>()
                    .join(","),
            );
            file.write_all(row_str.as_bytes())?;
        }
    }
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let results = plan_and_collect(
        &ctx,
        "
          SELECT
            c_group,
            count(c_uint64),
            count(DISTINCT c_int8),
            count(DISTINCT c_int16),
            count(DISTINCT c_int32),
            count(DISTINCT c_int64),
            count(DISTINCT c_uint8),
            count(DISTINCT c_uint16),
            count(DISTINCT c_uint32),
            count(DISTINCT c_uint64)
          FROM test
          GROUP BY c_group
        ",
    )
    .await?;

    Ok(results)
}

#[tokio::test]
async fn count_distinct_integers_aggregated_single_partition() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![
            ("a", 1),
            ("a", 1),
            ("a", 2),
            ("b", 9),
            ("c", 9),
            ("c", 10),
            ("c", 9),
        ],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | c_group | count(test.c_uint64) | count(DISTINCT test.c_int8) | count(DISTINCT test.c_int16) | count(DISTINCT test.c_int32) | count(DISTINCT test.c_int64) | count(DISTINCT test.c_uint8) | count(DISTINCT test.c_uint16) | count(DISTINCT test.c_uint32) | count(DISTINCT test.c_uint64) |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | a       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |
    | b       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |
    | c       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    ");

    Ok(())
}

#[tokio::test]
async fn count_distinct_integers_aggregated_multiple_partitions() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![("a", 1), ("a", 1), ("a", 2), ("b", 9), ("c", 9)],
        vec![("a", 1), ("a", 3), ("b", 8), ("b", 9), ("b", 10), ("b", 11)],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | c_group | count(test.c_uint64) | count(DISTINCT test.c_int8) | count(DISTINCT test.c_int16) | count(DISTINCT test.c_int32) | count(DISTINCT test.c_int64) | count(DISTINCT test.c_uint8) | count(DISTINCT test.c_uint16) | count(DISTINCT test.c_uint32) | count(DISTINCT test.c_uint64) |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | a       | 5                    | 3                           | 3                            | 3                            | 3                            | 3                            | 3                             | 3                             | 3                             |
    | b       | 5                    | 4                           | 4                            | 4                            | 4                            | 4                            | 4                             | 4                             | 4                             |
    | c       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    ");

    Ok(())
}

#[tokio::test]
async fn test_accumulator_row_accumulator() -> Result<()> {
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config(config);
    register_aggregate_csv(&ctx).await?;

    let sql = "SELECT c1, c2, MIN(c13) as min1, MIN(c9) as min2, MAX(c13) as max1, MAX(c9) as max2, AVG(c9) as avg1, MIN(c13) as min3, count(C9) as cnt1, 0.5*SUM(c9-c8) as sum1
    FROM aggregate_test_100
    GROUP BY c1, c2
    ORDER BY c1, c2
    LIMIT 5";

    let actual = execute_to_batches(&ctx, sql).await;
    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+
    | c1 | c2 | min1                           | min2      | max1                           | max2       | avg1               | min3                           | cnt1 | sum1         |
    +----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+
    | a  | 1  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB | 774637006 | waIGbOGl1PM6gnzZ4uuZt4E2yDWRHs | 4015442341 | 2437927011.0       | 0keZ5G8BffGwgF2RwQD59TFzMStxCB | 5    | 6094771121.5 |
    | a  | 2  | b3b9esRhTzFEawbs6XhpKnD9ojutHB | 145294611 | ukyD7b0Efj7tNlFSRmzZ0IqkEzg2a8 | 3717551163 | 2267588664.0       | b3b9esRhTzFEawbs6XhpKnD9ojutHB | 3    | 3401364777.0 |
    | a  | 3  | Amn2K87Db5Es3dFQO9cw9cvpAM6h35 | 431948861 | oLZ21P2JEDooxV1pU31cIxQHEeeoLu | 3998790955 | 2225685115.1666665 | Amn2K87Db5Es3dFQO9cw9cvpAM6h35 | 6    | 6676994872.5 |
    | a  | 4  | KJFcmTVjdkCMv94wYCtfHMFhzyRsmH | 466439833 | ydkwycaISlYSlEq3TlkS2m15I2pcp8 | 2502326480 | 1655431654.0       | KJFcmTVjdkCMv94wYCtfHMFhzyRsmH | 4    | 3310812222.5 |
    | a  | 5  | MeSTAXq8gVxVjbEjgkvU9YLte0X9uE | 141047417 | QJYm7YRA3YetcBHI5wkMZeLXVmfuNy | 2496054700 | 1216992989.6666667 | MeSTAXq8gVxVjbEjgkvU9YLte0X9uE | 3    | 1825431770.0 |
    +----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+
    ");

    Ok(())
}

/// Test that COUNT(DISTINCT) correctly handles dictionary arrays with all null values.
/// Verifies behavior across both single and multiple partitions.
#[tokio::test]
async fn count_distinct_dictionary_all_null_values() -> Result<()> {
    let n: usize = 5;
    let num = Arc::new(Int32Array::from_iter(0..n as i32)) as ArrayRef;

    // Create dictionary where all indices point to a null value (index 0)
    let dict_values = StringArray::from(vec![None, Some("abc")]);
    let dict_indices = Int32Array::from(vec![0; n]);
    let dict = DictionaryArray::new(dict_indices, Arc::new(dict_values));

    let schema = Arc::new(Schema::new(vec![
        Field::new("num1", DataType::Int32, false),
        Field::new("num2", DataType::Int32, false),
        Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![num.clone(), num.clone(), Arc::new(dict)],
    )?;

    // Test with single partition
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let df = ctx
        .sql("SELECT count(distinct dict) as cnt, count(num2) FROM t GROUP BY num1")
        .await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +-----+---------------+
    | cnt | count(t.num2) |
    +-----+---------------+
    | 0   | 1             |
    | 0   | 1             |
    | 0   | 1             |
    | 0   | 1             |
    | 0   | 1             |
    +-----+---------------+
    "###
    );

    // Test with multiple partitions
    let ctx_multi =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    let provider_multi = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx_multi.register_table("t", Arc::new(provider_multi))?;

    let df_multi = ctx_multi
        .sql("SELECT count(distinct dict) as cnt, count(num2) FROM t GROUP BY num1")
        .await?;
    let results_multi = df_multi.collect().await?;

    // Results should be identical across partition configurations
    assert_eq!(
        batches_to_string(&results),
        batches_to_string(&results_multi)
    );

    Ok(())
}

/// Test COUNT(DISTINCT) with mixed null and non-null dictionary values
#[tokio::test]
async fn count_distinct_dictionary_mixed_values() -> Result<()> {
    let n: usize = 6;
    let num = Arc::new(Int32Array::from_iter(0..n as i32)) as ArrayRef;

    // Dictionary values array with nulls and non-nulls
    let dict_values = StringArray::from(vec![None, Some("abc"), Some("def"), None]);
    // Create indices that point to both null and non-null values
    let dict_indices = Int32Array::from(vec![0, 1, 2, 0, 1, 3]);
    let dict = DictionaryArray::new(dict_indices, Arc::new(dict_values));

    let schema = Arc::new(Schema::new(vec![
        Field::new("num1", DataType::Int32, false),
        Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(schema.clone(), vec![num, Arc::new(dict)])?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider))?;

    // COUNT(DISTINCT) should only count non-null values "abc" and "def"
    let df = ctx.sql("SELECT count(distinct dict) FROM t").await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +------------------------+
    | count(DISTINCT t.dict) |
    +------------------------+
    | 2                      |
    +------------------------+
    "###
    );

    Ok(())
}
