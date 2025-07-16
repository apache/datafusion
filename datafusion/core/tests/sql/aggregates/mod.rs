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

//! Aggregate function tests

use super::*;
use arrow::{
    array::{
        types::UInt32Type, Decimal128Array, DictionaryArray, DurationNanosecondArray,
        Int32Array, LargeBinaryArray, StringArray, TimestampMicrosecondArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{test_util::batches_to_string, Result},
    execution::{config::SessionConfig, context::SessionContext},
};
use datafusion_catalog::MemTable;
use std::{cmp::min, sync::Arc};
/// Helper function to create the commonly used UInt32 indexed UTF-8 dictionary data type
pub fn string_dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

/// Helper functions for aggregate tests with dictionary columns and nulls
/// Creates a dictionary array with null values in the dictionary
pub fn create_test_dict(
    values: &[Option<&str>],
    indices: &[Option<u32>],
) -> DictionaryArray<UInt32Type> {
    let dict_values = StringArray::from(values.to_vec());
    let dict_indices = UInt32Array::from(indices.to_vec());
    DictionaryArray::new(dict_indices, Arc::new(dict_values))
}

/// Creates test data with both dictionary columns and value column
pub struct TestData {
    pub dict_null_keys: DictionaryArray<UInt32Type>,
    pub dict_null_vals: DictionaryArray<UInt32Type>,
    pub values: Int32Array,
    pub schema: Arc<Schema>,
}

impl TestData {
    pub fn new() -> Self {
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
    pub fn new_extended() -> Self {
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
    pub fn new_for_min_max() -> Self {
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
    pub fn new_for_median() -> Self {
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
    pub fn new_for_first_last() -> Self {
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

/// Sets up test contexts for TestData with both single and multiple partitions
pub async fn setup_test_contexts(
    test_data: &TestData,
) -> Result<(SessionContext, SessionContext)> {
    // Single partition context
    let ctx_single = create_context_with_partitions(test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_context_with_partitions(test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with the specified number of partitions and registers test data
pub async fn create_context_with_partitions(
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
pub fn split_test_data_into_batches(
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
pub async fn test_query_consistency(
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

/// Helper function to run snapshot tests with consistent setup, execution, and assertion
/// This reduces the repetitive pattern of "setup data → SQL → assert_snapshot!"
pub async fn run_snapshot_test(
    test_data: &TestData,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let (ctx_single, ctx_multi) = setup_test_contexts(test_data).await?;
    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;
    Ok(results)
}

/// Test data structure for fuzz table with dictionary columns containing nulls
pub struct FuzzTestData {
    pub schema: Arc<Schema>,
    pub u8_low: UInt8Array,
    pub dictionary_utf8_low: DictionaryArray<UInt32Type>,
    pub utf8_low: StringArray,
    pub utf8: StringArray,
}

impl FuzzTestData {
    pub fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_test_dict(
            &[Some("dict_a"), None, Some("dict_b"), Some("dict_c")],
            &[
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
pub async fn setup_fuzz_test_contexts() -> Result<(SessionContext, SessionContext)> {
    let test_data = FuzzTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz table partitioned into specified number of partitions
pub async fn create_fuzz_context_with_partitions(
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
pub fn split_fuzz_data_into_batches(
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

/// Test data structure for fuzz table with duration, large_binary and dictionary columns containing nulls
pub struct FuzzCountTestData {
    pub schema: Arc<Schema>,
    pub u8_low: UInt8Array,
    pub utf8_low: StringArray,
    pub dictionary_utf8_low: DictionaryArray<UInt32Type>,
    pub duration_nanosecond: DurationNanosecondArray,
    pub large_binary: LargeBinaryArray,
}

impl FuzzCountTestData {
    pub fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_test_dict(
            &[
                Some("group_alpha"),
                None,
                Some("group_beta"),
                Some("group_gamma"),
            ],
            &[
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
pub async fn setup_fuzz_count_test_contexts() -> Result<(SessionContext, SessionContext)>
{
    let test_data = FuzzCountTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_count_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_count_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz count table partitioned into specified number of partitions
pub async fn create_fuzz_count_context_with_partitions(
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
pub fn split_fuzz_count_data_into_batches(
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

/// Test data structure for fuzz table with numeric types for median testing and dictionary columns containing nulls
pub struct FuzzMedianTestData {
    pub schema: Arc<Schema>,
    pub u8_low: UInt8Array,
    pub dictionary_utf8_low: DictionaryArray<UInt32Type>,
    pub u64: UInt64Array,
    pub u16: UInt16Array,
    pub u32: UInt32Array,
    pub decimal128: Decimal128Array,
}

impl FuzzMedianTestData {
    pub fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_test_dict(
            &[
                Some("group_one"),
                None,
                Some("group_two"),
                Some("group_three"),
            ],
            &[
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
pub async fn setup_fuzz_median_test_contexts() -> Result<(SessionContext, SessionContext)>
{
    let test_data = FuzzMedianTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_median_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_median_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz median table partitioned into specified number of partitions
pub async fn create_fuzz_median_context_with_partitions(
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
pub fn split_fuzz_median_data_into_batches(
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

/// Test data structure for fuzz table with timestamp and dictionary columns containing nulls
pub struct FuzzTimestampTestData {
    pub schema: Arc<Schema>,
    pub utf8_low: StringArray,
    pub u8_low: UInt8Array,
    pub dictionary_utf8_low: DictionaryArray<UInt32Type>,
    pub timestamp_us: TimestampMicrosecondArray,
}

impl FuzzTimestampTestData {
    pub fn new() -> Self {
        // Create dictionary columns with null keys and values
        let dictionary_utf8_low = create_test_dict(
            &[Some("dict_x"), None, Some("dict_y"), Some("dict_z")],
            &[
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
pub async fn setup_fuzz_timestamp_test_contexts(
) -> Result<(SessionContext, SessionContext)> {
    let test_data = FuzzTimestampTestData::new();

    // Single partition context
    let ctx_single = create_fuzz_timestamp_context_with_partitions(&test_data, 1).await?;

    // Multiple partition context
    let ctx_multi = create_fuzz_timestamp_context_with_partitions(&test_data, 3).await?;

    Ok((ctx_single, ctx_multi))
}

/// Creates a session context with fuzz timestamp table partitioned into specified number of partitions
pub async fn create_fuzz_timestamp_context_with_partitions(
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
pub fn split_fuzz_timestamp_data_into_batches(
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

pub mod basic;
pub mod dict_nulls;
