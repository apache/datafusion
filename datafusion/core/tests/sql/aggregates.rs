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
fn create_dict(
    values: Vec<Option<&str>>,
    indices: Vec<Option<u32>>,
) -> DictionaryArray<UInt32Type> {
    let dict_values = StringArray::from(values);
    let dict_indices = UInt32Array::from(indices);
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
        let dict_null_keys = create_dict(
            vec![Some("group_a"), Some("group_b")],
            vec![
                Some(0), // group_a
                None,    // null key
                Some(1), // group_b
                None,    // null key
                Some(0), // group_a
            ],
        );

        // Create dictionary with null values
        let dict_null_vals = create_dict(
            vec![Some("group_x"), None, Some("group_y")],
            vec![
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
        let dict_null_vals = create_dict(
            vec![Some("group_a"), None, Some("group_b")],
            vec![
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
        let dict_null_keys = create_dict(
            vec![Some("group_x"), Some("group_y"), Some("group_z")],
            vec![
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
        let dict_null_keys = create_dict(
            vec![Some("group_a"), Some("group_b"), Some("group_c")],
            vec![
                Some(0),
                Some(1),
                Some(0),
                Some(2),
                None,
                None, // group_a, group_b, group_a, group_c, null, null
            ],
        );

        let dict_null_vals = create_dict(
            vec![Some("group_x"), None, Some("group_y")],
            vec![
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
        let dict_null_vals = create_dict(
            vec![Some("group_a"), None, Some("group_b")],
            vec![Some(0), Some(1), Some(2), Some(1), Some(0)],
        );

        let dict_null_keys = create_dict(
            vec![Some("group_x"), Some("group_y"), Some("group_z")],
            vec![Some(0), None, Some(1), None, Some(2)],
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
        let dict_null_keys = create_dict(
            vec![Some("group_a"), Some("group_b")],
            vec![Some(0), None, Some(1), None, Some(0)],
        );

        let dict_null_vals = create_dict(
            vec![Some("group_x"), None, Some("group_y")],
            vec![Some(0), Some(1), Some(2), Some(1), Some(0)],
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

/// Sets up test context and tables for both single and multiple partition scenarios
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
        a.partial_cmp(b).expect("Can compare ScalarValues")
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

/// Comprehensive test for aggregate functions with null values and dictionary columns
/// Tests COUNT, SUM, MIN, and MEDIAN null handling in single comprehensive test
#[tokio::test]
async fn test_aggregates_null_handling_comprehensive() -> Result<()> {
    let test_data_basic = TestData::new();
    let test_data_extended = TestData::new_extended();
    let test_data_min_max = TestData::new_for_min_max();
    let test_data_median = TestData::new_for_median();

    // Test COUNT null exclusion with basic data
    let (ctx_single, ctx_multi) = setup_test_contexts(&test_data_basic).await?;

    let sql_count = "SELECT dict_null_keys, COUNT(value) as cnt FROM t GROUP BY dict_null_keys ORDER BY dict_null_keys NULLS FIRST";
    let results_count =
        test_query_consistency(&ctx_single, &ctx_multi, sql_count).await?;

    assert_snapshot!(
        batches_to_string(&results_count),
        @r###"
    +----------------+-----+
    | dict_null_keys | cnt |
    +----------------+-----+
    |                | 0   |
    | group_a        | 2   |
    | group_b        | 1   |
    +----------------+-----+
    "###
    );

    // Test SUM null handling with extended data
    let (ctx_single_sum, ctx_multi_sum) =
        setup_test_contexts(&test_data_extended).await?;

    let sql_sum = "SELECT dict_null_vals, SUM(value) as total FROM t GROUP BY dict_null_vals ORDER BY dict_null_vals NULLS FIRST";
    let results_sum =
        test_query_consistency(&ctx_single_sum, &ctx_multi_sum, sql_sum).await?;

    assert_snapshot!(
        batches_to_string(&results_sum),
        @r"
    +----------------+-------+
    | dict_null_vals | total |
    +----------------+-------+
    |                | 4     |
    | group_x        | 4     |
    | group_y        | 2     |
    | group_z        | 5     |
    +----------------+-------+
    "
    );

    // Test MIN null handling with min/max data
    let (ctx_single_min, ctx_multi_min) = setup_test_contexts(&test_data_min_max).await?;

    let sql_min = "SELECT dict_null_keys, MIN(value) as minimum FROM t GROUP BY dict_null_keys ORDER BY dict_null_keys NULLS FIRST";
    let results_min =
        test_query_consistency(&ctx_single_min, &ctx_multi_min, sql_min).await?;

    assert_snapshot!(
        batches_to_string(&results_min),
        @r###"
    +----------------+---------+
    | dict_null_keys | minimum |
    +----------------+---------+
    |                | 2       |
    | group_a        | 3       |
    | group_b        | 1       |
    | group_c        | 7       |
    +----------------+---------+
    "###
    );

    // Test MEDIAN null handling with median data
    let (ctx_single_median, ctx_multi_median) =
        setup_test_contexts(&test_data_median).await?;

    let sql_median = "SELECT dict_null_vals, MEDIAN(value) as median_value FROM t GROUP BY dict_null_vals ORDER BY dict_null_vals NULLS FIRST";
    let results_median =
        test_query_consistency(&ctx_single_median, &ctx_multi_median, sql_median).await?;

    assert_snapshot!(
        batches_to_string(&results_median),
        @r"
    +----------------+--------------+
    | dict_null_vals | median_value |
    +----------------+--------------+
    |                | 3            |
    | group_x        | 1            |
    | group_y        | 5            |
    | group_z        | 7            |
    +----------------+--------------+
    ");

    Ok(())
}

/// Test FIRST_VAL and LAST_VAL with null values and GROUP BY dict with null keys and null values - may return null if first/last value is null (single and multiple partitions)
#[tokio::test]
async fn test_first_last_val_null_handling() -> Result<()> {
    let test_data = TestData::new_for_first_last();
    let (ctx_single, ctx_multi) = setup_test_contexts(&test_data).await?;

    // Test FIRST_VALUE and LAST_VALUE with window functions over groups
    let sql = "SELECT dict_null_keys, value, FIRST_VALUE(value) OVER (PARTITION BY dict_null_keys ORDER BY value NULLS FIRST) as first_val, LAST_VALUE(value) OVER (PARTITION BY dict_null_keys ORDER BY value NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val FROM t ORDER BY dict_null_keys NULLS FIRST, value NULLS FIRST";

    let results_single = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(batches_to_string(&results_single), @r"
    +----------------+-------+-----------+----------+
    | dict_null_keys | value | first_val | last_val |
    +----------------+-------+-----------+----------+
    |                | 1     | 1         | 3        |
    |                | 3     | 1         | 3        |
    | group_a        |       |           |          |
    | group_a        |       |           |          |
    | group_b        | 2     | 2         | 2        |
    +----------------+-------+-----------+----------+
    ");

    Ok(())
}

/// Test FIRST_VALUE and LAST_VALUE with ORDER BY - comprehensive null handling
#[tokio::test]
async fn test_first_last_value_order_by_null_handling() -> Result<()> {
    let ctx = SessionContext::new();

    // Create test data with nulls mixed in
    let dict_keys = create_dict(
        vec![Some("group_a"), Some("group_b"), Some("group_c")],
        vec![Some(0), Some(1), Some(2), Some(0), Some(1)],
    );

    let values = Int32Array::from(vec![None, Some(10), Some(20), Some(5), None]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("dict_group", string_dict_type(), true),
        Field::new("value", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(dict_keys), Arc::new(values)],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("test_data", Arc::new(table))?;

    // Test all combinations of FIRST_VALUE and LAST_VALUE with null handling
    let sql = "SELECT 
        dict_group,
        value,
        FIRST_VALUE(value IGNORE NULLS) OVER (ORDER BY value NULLS LAST) as first_ignore_nulls,
        FIRST_VALUE(value RESPECT NULLS) OVER (ORDER BY value NULLS FIRST) as first_respect_nulls,
        LAST_VALUE(value IGNORE NULLS) OVER (ORDER BY value NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_ignore_nulls,
        LAST_VALUE(value RESPECT NULLS) OVER (ORDER BY value NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_respect_nulls
    FROM test_data 
    ORDER BY value NULLS LAST";

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +------------+-------+--------------------+---------------------+-------------------+--------------------+
    | dict_group | value | first_ignore_nulls | first_respect_nulls | last_ignore_nulls | last_respect_nulls |
    +------------+-------+--------------------+---------------------+-------------------+--------------------+
    | group_a    | 5     | 5                  |                     | 20                |                    |
    | group_b    | 10    | 5                  |                     | 20                |                    |
    | group_c    | 20    | 5                  |                     | 20                |                    |
    | group_a    |       | 5                  |                     | 20                |                    |
    | group_b    |       | 5                  |                     | 20                |                    |
    +------------+-------+--------------------+---------------------+-------------------+--------------------+
    "###
    );

    Ok(())
}

/// Test GROUP BY with dictionary columns containing null keys and values for FIRST_VALUE/LAST_VALUE
#[tokio::test]
async fn test_first_last_value_group_by_dict_nulls() -> Result<()> {
    let ctx = SessionContext::new();

    // Create dictionary with null keys
    let dict_null_keys = create_dict(
        vec![Some("group_a"), Some("group_b")],
        vec![
            Some(0), // group_a
            None,    // null key
            Some(1), // group_b
            None,    // null key
            Some(0), // group_a
        ],
    );

    // Create dictionary with null values
    let dict_null_vals = create_dict(
        vec![Some("val_x"), None, Some("val_y")],
        vec![
            Some(0), // val_x
            Some(1), // null value
            Some(2), // val_y
            Some(1), // null value
            Some(0), // val_x
        ],
    );

    // Create test values
    let values = Int32Array::from(vec![Some(10), Some(20), Some(30), Some(40), Some(50)]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("dict_null_keys", string_dict_type(), true),
        Field::new("dict_null_vals", string_dict_type(), true),
        Field::new("value", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(dict_null_keys),
            Arc::new(dict_null_vals),
            Arc::new(values),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("test_data", Arc::new(table))?;

    // Test GROUP BY with null keys
    let sql = "SELECT 
        dict_null_keys,
        FIRST_VALUE(value) as first_val,
        LAST_VALUE(value) as last_val,
        COUNT(*) as cnt
    FROM test_data 
    GROUP BY dict_null_keys 
    ORDER BY dict_null_keys NULLS FIRST";

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +----------------+-----------+----------+-----+
    | dict_null_keys | first_val | last_val | cnt |
    +----------------+-----------+----------+-----+
    |                | 20        | 40       | 2   |
    | group_a        | 10        | 50       | 2   |
    | group_b        | 30        | 30       | 1   |
    +----------------+-----------+----------+-----+
    "###
    );

    // Test GROUP BY with null values in dictionary
    let sql2 = "SELECT 
        dict_null_vals,
        FIRST_VALUE(value) as first_val,
        LAST_VALUE(value) as last_val,
        COUNT(*) as cnt
    FROM test_data 
    GROUP BY dict_null_vals 
    ORDER BY dict_null_vals NULLS FIRST";

    let df2 = ctx.sql(sql2).await?;
    let results2 = df2.collect().await?;

    assert_snapshot!(
        batches_to_string(&results2),
        @r###"
    +----------------+-----------+----------+-----+
    | dict_null_vals | first_val | last_val | cnt |
    +----------------+-----------+----------+-----+
    |                | 20        | 40       | 2   |
    | val_x          | 10        | 50       | 2   |
    | val_y          | 30        | 30       | 1   |
    +----------------+-----------+----------+-----+
    "###
    );

    Ok(())
}

/// Test MAX with dictionary columns containing null keys and values as specified in the SQL query
/// Test data structure for fuzz table with dictionary columns containing nulls
struct FuzzTestData {
    schema: Arc<Schema>,
    u8_low: UInt8Array,
    dictionary_utf8_low: DictionaryArray<UInt32Type>,
    utf8_low: StringArray,
    utf8: StringArray,
}

impl FuzzTestData {
    fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_dict(
            vec![Some("dict_a"), None, Some("dict_b"), Some("dict_c")],
            vec![
                Some(0), // dict_a
                Some(1), // null value
                Some(2), // dict_b
                None,    // null key
                Some(0), // dict_a
                Some(1), // null value
                Some(3), // dict_c
                None,    // null key
            ],
        );

        let u8_low = UInt8Array::from(vec![
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(1),
            Some(3),
            Some(3),
            Some(2),
        ]);

        let utf8_low = StringArray::from(vec![
            Some("str_a"),
            Some("str_b"),
            Some("str_c"),
            Some("str_d"),
            Some("str_a"),
            Some("str_e"),
            Some("str_f"),
            Some("str_c"),
        ]);

        let utf8 = StringArray::from(vec![
            Some("value_1"),
            Some("value_2"),
            Some("value_3"),
            Some("value_4"),
            Some("value_5"),
            None,
            Some("value_6"),
            Some("value_7"),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("u8_low", DataType::UInt8, true),
            Field::new("dictionary_utf8_low", string_dict_type(), true),
            Field::new("utf8_low", DataType::Utf8, true),
            Field::new("utf8", DataType::Utf8, true),
        ]));

        Self {
            schema,
            u8_low,
            dictionary_utf8_low,
            utf8_low,
            utf8,
        }
    }
}

/// Sets up test contexts for fuzz table with both single and multiple partitions
async fn setup_fuzz_test_contexts() -> Result<(SessionContext, SessionContext)> {
    let test_data = FuzzTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz table partitioned into specified number of partitions
async fn create_fuzz_context_with_partitions(
    test_data: &FuzzTestData,
    num_partitions: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(num_partitions),
    );

    let batches = split_fuzz_data_into_batches(test_data, num_partitions)?;
    let provider = MemTable::try_new(test_data.schema.clone(), batches)?;
    ctx.register_table("fuzz_table", Arc::new(provider))?;

    Ok(ctx)
}

/// Splits fuzz test data into multiple batches for partitioning
fn split_fuzz_data_into_batches(
    test_data: &FuzzTestData,
    num_partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    debug_assert!(num_partitions > 0, "num_partitions must be greater than 0");
    let total_len = test_data.u8_low.len();
    let chunk_size = total_len.div_ceil(num_partitions);

    let mut batches = Vec::new();
    let mut start = 0;

    while start < total_len {
        let end = min(start + chunk_size, total_len);
        let len = end - start;

        if len > 0 {
            let batch = RecordBatch::try_new(
                test_data.schema.clone(),
                vec![
                    Arc::new(test_data.u8_low.slice(start, len)),
                    Arc::new(test_data.dictionary_utf8_low.slice(start, len)),
                    Arc::new(test_data.utf8_low.slice(start, len)),
                    Arc::new(test_data.utf8.slice(start, len)),
                ],
            )?;
            batches.push(vec![batch]);
        }
        start = end;
    }

    Ok(batches)
}

/// Test MAX with fuzz table containing dictionary columns with null keys and values (single and multiple partitions)
#[tokio::test]
async fn test_max_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_test_contexts().await?;

    // Execute the SQL query with MAX aggregations
    let sql = "SELECT
        u8_low,
        dictionary_utf8_low,
        utf8_low,
        max(utf8_low) as col1,
        max(utf8) as col2
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        dictionary_utf8_low,
        utf8_low
    ORDER BY u8_low, dictionary_utf8_low NULLS FIRST, utf8_low";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +--------+---------------------+----------+-------+---------+
    | u8_low | dictionary_utf8_low | utf8_low | col1  | col2    |
    +--------+---------------------+----------+-------+---------+
    | 1      |                     | str_b    | str_b | value_2 |
    | 1      | dict_a              | str_a    | str_a | value_5 |
    | 2      |                     | str_c    | str_c | value_7 |
    | 2      |                     | str_d    | str_d | value_4 |
    | 2      | dict_b              | str_c    | str_c | value_3 |
    | 3      |                     | str_e    | str_e |         |
    | 3      | dict_c              | str_f    | str_f | value_6 |
    +--------+---------------------+----------+-------+---------+
    "
    );

    Ok(())
}

/// Test data structure for fuzz table with timestamp and dictionary columns containing nulls
struct FuzzTimestampTestData {
    schema: Arc<Schema>,
    utf8_low: StringArray,
    u8_low: UInt8Array,
    dictionary_utf8_low: DictionaryArray<UInt32Type>,
    timestamp_us: TimestampMicrosecondArray,
}

impl FuzzTimestampTestData {
    fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_dict(
            vec![Some("dict_x"), None, Some("dict_y"), Some("dict_z")],
            vec![
                Some(0), // dict_x
                Some(1), // null value
                Some(2), // dict_y
                None,    // null key
                Some(0), // dict_x
                Some(1), // null value
                Some(3), // dict_z
                None,    // null key
                Some(2), // dict_y
            ],
        );

        let utf8_low = StringArray::from(vec![
            Some("alpha"),
            Some("beta"),
            Some("gamma"),
            Some("delta"),
            Some("alpha"),
            Some("epsilon"),
            Some("zeta"),
            Some("delta"),
            Some("gamma"),
        ]);

        let u8_low = UInt8Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(20),
            Some(10),
            Some(40),
            Some(30),
            Some(20),
            Some(30),
        ]);

        // Create timestamp data with some nulls
        let timestamp_us = TimestampMicrosecondArray::from(vec![
            Some(1000000), // 1970-01-01 00:00:01
            Some(2000000), // 1970-01-01 00:00:02
            Some(3000000), // 1970-01-01 00:00:03
            None,          // null timestamp
            Some(1500000), // 1970-01-01 00:00:01.5
            Some(4000000), // 1970-01-01 00:00:04
            Some(2500000), // 1970-01-01 00:00:02.5
            Some(3500000), // 1970-01-01 00:00:03.5
            Some(2800000), // 1970-01-01 00:00:02.8
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("utf8_low", DataType::Utf8, true),
            Field::new("u8_low", DataType::UInt8, true),
            Field::new("dictionary_utf8_low", string_dict_type(), true),
            Field::new(
                "timestamp_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        Self {
            schema,
            utf8_low,
            u8_low,
            dictionary_utf8_low,
            timestamp_us,
        }
    }
}

/// Sets up test contexts for fuzz table with timestamps and both single and multiple partitions
async fn setup_fuzz_timestamp_test_contexts() -> Result<(SessionContext, SessionContext)>
{
    let test_data = FuzzTimestampTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_timestamp_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_timestamp_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz timestamp table partitioned into specified number of partitions
async fn create_fuzz_timestamp_context_with_partitions(
    test_data: &FuzzTimestampTestData,
    num_partitions: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(num_partitions),
    );

    let batches = split_fuzz_timestamp_data_into_batches(test_data, num_partitions)?;
    let provider = MemTable::try_new(test_data.schema.clone(), batches)?;
    ctx.register_table("fuzz_table", Arc::new(provider))?;

    Ok(ctx)
}

/// Splits fuzz timestamp test data into multiple batches for partitioning
fn split_fuzz_timestamp_data_into_batches(
    test_data: &FuzzTimestampTestData,
    num_partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    debug_assert!(num_partitions > 0, "num_partitions must be greater than 0");
    let total_len = test_data.utf8_low.len();
    let chunk_size = total_len.div_ceil(num_partitions);

    let mut batches = Vec::new();
    let mut start = 0;

    while start < total_len {
        let end = min(start + chunk_size, total_len);
        let len = end - start;

        if len > 0 {
            let batch = RecordBatch::try_new(
                test_data.schema.clone(),
                vec![
                    Arc::new(test_data.utf8_low.slice(start, len)),
                    Arc::new(test_data.u8_low.slice(start, len)),
                    Arc::new(test_data.dictionary_utf8_low.slice(start, len)),
                    Arc::new(test_data.timestamp_us.slice(start, len)),
                ],
            )?;
            batches.push(vec![batch]);
        }
        start = end;
    }

    Ok(batches)
}

/// Test MIN with fuzz table containing dictionary columns with null keys and values and timestamp data (single and multiple partitions)
#[tokio::test]
async fn test_min_timestamp_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_timestamp_test_contexts().await?;

    // Execute the SQL query with MIN aggregation on timestamp
    let sql = "SELECT
        utf8_low,
        u8_low,
        dictionary_utf8_low,
        min(timestamp_us) as col1
    FROM
        fuzz_table
    GROUP BY
        utf8_low,
        u8_low,
        dictionary_utf8_low
    ORDER BY utf8_low, u8_low, dictionary_utf8_low NULLS FIRST";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +----------+--------+---------------------+-------------------------+
    | utf8_low | u8_low | dictionary_utf8_low | col1                    |
    +----------+--------+---------------------+-------------------------+
    | alpha    | 10     | dict_x              | 1970-01-01T00:00:01     |
    | beta     | 20     |                     | 1970-01-01T00:00:02     |
    | delta    | 20     |                     | 1970-01-01T00:00:03.500 |
    | epsilon  | 40     |                     | 1970-01-01T00:00:04     |
    | gamma    | 30     | dict_y              | 1970-01-01T00:00:02.800 |
    | zeta     | 30     | dict_z              | 1970-01-01T00:00:02.500 |
    +----------+--------+---------------------+-------------------------+
    "
    );

    Ok(())
}

/// Test data structure for fuzz table with duration, large_binary and dictionary columns containing nulls
struct FuzzCountTestData {
    schema: Arc<Schema>,
    u8_low: UInt8Array,
    utf8_low: StringArray,
    dictionary_utf8_low: DictionaryArray<UInt32Type>,
    duration_nanosecond: DurationNanosecondArray,
    large_binary: LargeBinaryArray,
}

impl FuzzCountTestData {
    fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_dict(
            vec![
                Some("group_alpha"),
                None,
                Some("group_beta"),
                Some("group_gamma"),
            ],
            vec![
                Some(0), // group_alpha
                Some(1), // null value
                Some(2), // group_beta
                None,    // null key
                Some(0), // group_alpha
                Some(1), // null value
                Some(3), // group_gamma
                None,    // null key
                Some(2), // group_beta
                Some(0), // group_alpha
            ],
        );

        let u8_low = UInt8Array::from(vec![
            Some(5),
            Some(10),
            Some(15),
            Some(10),
            Some(5),
            Some(20),
            Some(25),
            Some(10),
            Some(15),
            Some(5),
        ]);

        let utf8_low = StringArray::from(vec![
            Some("text_a"),
            Some("text_b"),
            Some("text_c"),
            Some("text_d"),
            Some("text_a"),
            Some("text_e"),
            Some("text_f"),
            Some("text_d"),
            Some("text_c"),
            Some("text_a"),
        ]);

        // Create duration data with some nulls (nanoseconds)
        let duration_nanosecond = DurationNanosecondArray::from(vec![
            Some(1000000000), // 1 second
            Some(2000000000), // 2 seconds
            None,             // null duration
            Some(3000000000), // 3 seconds
            Some(1500000000), // 1.5 seconds
            None,             // null duration
            Some(4000000000), // 4 seconds
            Some(2500000000), // 2.5 seconds
            Some(3500000000), // 3.5 seconds
            Some(1200000000), // 1.2 seconds
        ]);

        // Create large binary data with some nulls and duplicates
        let large_binary = LargeBinaryArray::from(vec![
            Some(b"binary_data_1".as_slice()),
            Some(b"binary_data_2".as_slice()),
            Some(b"binary_data_3".as_slice()),
            None,                              // null binary
            Some(b"binary_data_1".as_slice()), // duplicate
            Some(b"binary_data_4".as_slice()),
            Some(b"binary_data_5".as_slice()),
            None,                              // null binary
            Some(b"binary_data_3".as_slice()), // duplicate
            Some(b"binary_data_1".as_slice()), // duplicate
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("u8_low", DataType::UInt8, true),
            Field::new("utf8_low", DataType::Utf8, true),
            Field::new("dictionary_utf8_low", string_dict_type(), true),
            Field::new(
                "duration_nanosecond",
                DataType::Duration(TimeUnit::Nanosecond),
                true,
            ),
            Field::new("large_binary", DataType::LargeBinary, true),
        ]));

        Self {
            schema,
            u8_low,
            utf8_low,
            dictionary_utf8_low,
            duration_nanosecond,
            large_binary,
        }
    }
}

/// Sets up test contexts for fuzz table with duration/binary columns and both single and multiple partitions
async fn setup_fuzz_count_test_contexts() -> Result<(SessionContext, SessionContext)> {
    let test_data = FuzzCountTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_count_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_count_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz count table partitioned into specified number of partitions
async fn create_fuzz_count_context_with_partitions(
    test_data: &FuzzCountTestData,
    num_partitions: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(num_partitions),
    );

    let batches = split_fuzz_count_data_into_batches(test_data, num_partitions)?;
    let provider = MemTable::try_new(test_data.schema.clone(), batches)?;
    ctx.register_table("fuzz_table", Arc::new(provider))?;

    Ok(ctx)
}

/// Splits fuzz count test data into multiple batches for partitioning
fn split_fuzz_count_data_into_batches(
    test_data: &FuzzCountTestData,
    num_partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    debug_assert!(num_partitions > 0, "num_partitions must be greater than 0");
    let total_len = test_data.u8_low.len();
    let chunk_size = total_len.div_ceil(num_partitions);

    let mut batches = Vec::new();
    let mut start = 0;

    while start < total_len {
        let end = min(start + chunk_size, total_len);
        let len = end - start;

        if len > 0 {
            let batch = RecordBatch::try_new(
                test_data.schema.clone(),
                vec![
                    Arc::new(test_data.u8_low.slice(start, len)),
                    Arc::new(test_data.utf8_low.slice(start, len)),
                    Arc::new(test_data.dictionary_utf8_low.slice(start, len)),
                    Arc::new(test_data.duration_nanosecond.slice(start, len)),
                    Arc::new(test_data.large_binary.slice(start, len)),
                ],
            )?;
            batches.push(vec![batch]);
        }
        start = end;
    }

    Ok(batches)
}

/// Test COUNT and COUNT DISTINCT with fuzz table containing dictionary columns with null keys and values (single and multiple partitions)
#[tokio::test]
async fn test_count_distinct_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_count_test_contexts().await?;

    // Execute the SQL query with COUNT and COUNT DISTINCT aggregations
    let sql = "SELECT
        u8_low,
        utf8_low,
        dictionary_utf8_low,
        count(duration_nanosecond) as col1,
        count(DISTINCT large_binary) as col2
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        utf8_low,
        dictionary_utf8_low
    ORDER BY u8_low, utf8_low, dictionary_utf8_low NULLS FIRST";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +--------+----------+---------------------+------+------+
    | u8_low | utf8_low | dictionary_utf8_low | col1 | col2 |
    +--------+----------+---------------------+------+------+
    | 5      | text_a   | group_alpha         | 3    | 1    |
    | 10     | text_b   |                     | 1    | 1    |
    | 10     | text_d   |                     | 2    | 0    |
    | 15     | text_c   | group_beta          | 1    | 1    |
    | 20     | text_e   |                     | 0    | 1    |
    | 25     | text_f   | group_gamma         | 1    | 1    |
    +--------+----------+---------------------+------+------+
    "###
    );

    Ok(())
}

/// Test data structure for fuzz table with numeric types for median testing and dictionary columns containing nulls
struct FuzzMedianTestData {
    schema: Arc<Schema>,
    u8_low: UInt8Array,
    dictionary_utf8_low: DictionaryArray<UInt32Type>,
    u64: UInt64Array,
    u16: UInt16Array,
    u32: UInt32Array,
    decimal128: Decimal128Array,
}

impl FuzzMedianTestData {
    fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_dict(
            vec![
                Some("group_one"),
                None,
                Some("group_two"),
                Some("group_three"),
            ],
            vec![
                Some(0), // group_one
                Some(1), // null value
                Some(2), // group_two
                None,    // null key
                Some(0), // group_one
                Some(1), // null value
                Some(3), // group_three
                None,    // null key
                Some(2), // group_two
                Some(0), // group_one
                Some(1), // null value
                Some(3), // group_three
            ],
        );

        let u8_low = UInt8Array::from(vec![
            Some(100),
            Some(200),
            Some(100),
            Some(200),
            Some(100),
            Some(50),
            Some(50),
            Some(200),
            Some(100),
            Some(100),
            Some(75),
            Some(50),
        ]);

        // Create u64 data with some nulls and duplicates for DISTINCT testing
        let u64 = UInt64Array::from(vec![
            Some(1000),
            Some(2000),
            Some(1500),
            Some(3000),
            Some(1000), // duplicate
            None,       // null
            Some(5000),
            Some(2500),
            Some(1500), // duplicate
            Some(1200),
            Some(4000),
            Some(5000), // duplicate
        ]);

        // Create u16 data with some nulls and duplicates
        let u16 = UInt16Array::from(vec![
            Some(10),
            Some(20),
            Some(15),
            None,     // null
            Some(10), // duplicate
            Some(30),
            Some(50),
            Some(25),
            Some(15), // duplicate
            Some(12),
            None,     // null
            Some(50), // duplicate
        ]);

        // Create u32 data with some nulls and duplicates
        let u32 = UInt32Array::from(vec![
            Some(100000),
            Some(200000),
            Some(150000),
            Some(300000),
            Some(100000), // duplicate
            Some(400000),
            Some(500000),
            None,         // null
            Some(150000), // duplicate
            Some(120000),
            Some(450000),
            None, // null
        ]);

        // Create decimal128 data with precision 10, scale 2
        let decimal128 = Decimal128Array::from(vec![
            Some(12345), // 123.45
            Some(67890), // 678.90
            Some(11111), // 111.11
            None,        // null
            Some(12345), // 123.45 duplicate
            Some(98765), // 987.65
            Some(55555), // 555.55
            Some(33333), // 333.33
            Some(11111), // 111.11 duplicate
            Some(12500), // 125.00
            None,        // null
            Some(55555), // 555.55 duplicate
        ])
        .with_precision_and_scale(10, 2)
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("u8_low", DataType::UInt8, true),
            Field::new("dictionary_utf8_low", string_dict_type(), true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("decimal128", DataType::Decimal128(10, 2), true),
        ]));

        Self {
            schema,
            u8_low,
            dictionary_utf8_low,
            u64,
            u16,
            u32,
            decimal128,
        }
    }
}

/// Sets up test contexts for fuzz table with numeric types for median testing and both single and multiple partitions
async fn setup_fuzz_median_test_contexts() -> Result<(SessionContext, SessionContext)> {
    let test_data = FuzzMedianTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_median_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_median_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz median table partitioned into specified number of partitions
async fn create_fuzz_median_context_with_partitions(
    test_data: &FuzzMedianTestData,
    num_partitions: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(num_partitions),
    );

    let batches = split_fuzz_median_data_into_batches(test_data, num_partitions)?;
    let provider = MemTable::try_new(test_data.schema.clone(), batches)?;
    ctx.register_table("fuzz_table", Arc::new(provider))?;

    Ok(ctx)
}

/// Splits fuzz median test data into multiple batches for partitioning
fn split_fuzz_median_data_into_batches(
    test_data: &FuzzMedianTestData,
    num_partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    debug_assert!(num_partitions > 0, "num_partitions must be greater than 0");
    let total_len = test_data.u8_low.len();
    let chunk_size = total_len.div_ceil(num_partitions);

    let mut batches = Vec::new();
    let mut start = 0;

    while start < total_len {
        let end = min(start + chunk_size, total_len);
        let len = end - start;

        if len > 0 {
            let batch = RecordBatch::try_new(
                test_data.schema.clone(),
                vec![
                    Arc::new(test_data.u8_low.slice(start, len)),
                    Arc::new(test_data.dictionary_utf8_low.slice(start, len)),
                    Arc::new(test_data.u64.slice(start, len)),
                    Arc::new(test_data.u16.slice(start, len)),
                    Arc::new(test_data.u32.slice(start, len)),
                    Arc::new(test_data.decimal128.slice(start, len)),
                ],
            )?;
            batches.push(vec![batch]);
        }
        start = end;
    }

    Ok(batches)
}

/// Test MEDIAN and MEDIAN DISTINCT with fuzz table containing various numeric types and dictionary columns with null keys and values (single and multiple partitions)
#[tokio::test]
async fn test_median_distinct_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_median_test_contexts().await?;

    // Execute the SQL query with MEDIAN and MEDIAN DISTINCT aggregations
    let sql = "SELECT
        u8_low,
        dictionary_utf8_low,
        median(DISTINCT u64) as col1,
        median(DISTINCT u16) as col2,
        median(u64) as col3,
        median(decimal128) as col4,
        median(DISTINCT u32) as col5
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        dictionary_utf8_low
    ORDER BY u8_low, dictionary_utf8_low NULLS FIRST";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +--------+---------------------+------+------+------+--------+--------+
    | u8_low | dictionary_utf8_low | col1 | col2 | col3 | col4   | col5   |
    +--------+---------------------+------+------+------+--------+--------+
    | 50     |                     |      | 30   |      | 987.65 | 400000 |
    | 50     | group_three         | 5000 | 50   | 5000 | 555.55 | 500000 |
    | 75     |                     | 4000 |      | 4000 |        | 450000 |
    | 100    | group_one           | 1100 | 11   | 1000 | 123.45 | 110000 |
    | 100    | group_two           | 1500 | 15   | 1500 | 111.11 | 150000 |
    | 200    |                     | 2500 | 22   | 2500 | 506.11 | 250000 |
    +--------+---------------------+------+------+------+--------+--------+
    "
    );

    Ok(())
}
