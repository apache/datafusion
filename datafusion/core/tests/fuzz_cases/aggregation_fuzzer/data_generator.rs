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

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{
    BinaryType, BinaryViewType, BooleanType, ByteArrayType, ByteViewType, DataType,
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Field, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, LargeBinaryType,
    LargeUtf8Type, Schema, StringViewType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type, Utf8Type,
};
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result};
use datafusion_physical_expr::{expressions::col, PhysicalSortExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::sorts::sort::sort_batch;
use rand::{
    rngs::{StdRng, ThreadRng},
    thread_rng, Rng, SeedableRng,
};
use test_utils::{
    array_gen::{
        BinaryArrayGenerator, BooleanArrayGenerator, DecimalArrayGenerator,
        PrimitiveArrayGenerator, StringArrayGenerator,
    },
    stagger_batch,
};

/// Config for Dataset generator
///
/// # Parameters
///   - `columns`, you just need to define `column name`s and `column data type`s
///     for the test datasets, and then they will be randomly generated from the generator
///     when you call `generate` function
///         
///   - `rows_num_range`, the number of rows in the datasets will be randomly generated
///      within this range
///
///   - `sort_keys`, if `sort_keys` are defined, when you call the `generate` function, the generator
///      will generate one `base dataset` firstly. Then the `base dataset` will be sorted
///      based on each `sort_key` respectively. And finally `len(sort_keys) + 1` datasets
///      will be returned
///
#[derive(Debug, Clone)]
pub struct DatasetGeneratorConfig {
    /// Descriptions of columns in datasets, it's `required`
    pub columns: Vec<ColumnDescr>,

    /// Rows num range of the generated datasets, it's `required`
    pub rows_num_range: (usize, usize),

    /// Additional optional sort keys
    ///
    /// The generated datasets always include a non-sorted copy. For each
    /// element in `sort_keys_set`, an additional dataset is created that
    /// is sorted by these values as well.
    pub sort_keys_set: Vec<Vec<String>>,
}

impl DatasetGeneratorConfig {
    /// Return a list of all column names
    pub fn all_columns(&self) -> Vec<&str> {
        self.columns.iter().map(|d| d.name.as_str()).collect()
    }

    /// Return a list of column names that are "numeric"
    pub fn numeric_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter_map(|d| {
                if d.column_type.is_numeric()
                    && !matches!(d.column_type, DataType::Float32 | DataType::Float64)
                {
                    Some(d.name.as_str())
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Dataset generator
///
/// It will generate one random [`Dataset`] when `generate` function is called.
///
/// The generation logic in `generate`:
///
///   - Randomly generate a base record from `batch_generator` firstly.
///     And `columns`, `rows_num_range` in `config`(detail can see `DataSetsGeneratorConfig`),
///     will be used in generation.
///
///   - Sort the batch according to `sort_keys` in `config` to generate another
///     `len(sort_keys)` sorted batches.
///   
///   - Split each batch to multiple batches which each sub-batch in has the randomly `rows num`,
///     and this multiple batches will be used to create the `Dataset`.
///
pub struct DatasetGenerator {
    batch_generator: RecordBatchGenerator,
    sort_keys_set: Vec<Vec<String>>,
}

impl DatasetGenerator {
    pub fn new(config: DatasetGeneratorConfig) -> Self {
        let batch_generator = RecordBatchGenerator::new(
            config.rows_num_range.0,
            config.rows_num_range.1,
            config.columns,
        );

        Self {
            batch_generator,
            sort_keys_set: config.sort_keys_set,
        }
    }

    pub fn generate(&self) -> Result<Vec<Dataset>> {
        let mut datasets = Vec::with_capacity(self.sort_keys_set.len() + 1);

        // Generate the base batch (unsorted)
        let base_batch = self.batch_generator.generate()?;
        let batches = stagger_batch(base_batch.clone());
        let dataset = Dataset::new(batches, Vec::new());
        datasets.push(dataset);

        // Generate the related sorted batches
        let schema = base_batch.schema_ref();
        for sort_keys in self.sort_keys_set.clone() {
            let sort_exprs = sort_keys
                .iter()
                .map(|key| {
                    let col_expr = col(key, schema)?;
                    Ok(PhysicalSortExpr::new_default(col_expr))
                })
                .collect::<Result<LexOrdering>>()?;
            let sorted_batch = sort_batch(&base_batch, sort_exprs.as_ref(), None)?;

            let batches = stagger_batch(sorted_batch);
            let dataset = Dataset::new(batches, sort_keys);
            datasets.push(dataset);
        }

        Ok(datasets)
    }
}

/// Single test data set
#[derive(Debug)]
pub struct Dataset {
    pub batches: Vec<RecordBatch>,
    pub total_rows_num: usize,
    pub sort_keys: Vec<String>,
}

impl Dataset {
    pub fn new(batches: Vec<RecordBatch>, sort_keys: Vec<String>) -> Self {
        let total_rows_num = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();

        Self {
            batches,
            total_rows_num,
            sort_keys,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDescr {
    /// Column name
    name: String,

    /// Data type of this column
    column_type: DataType,

    /// The maximum number of distinct values in this column.
    ///
    /// See [`ColumnDescr::with_max_num_distinct`] for more information
    max_num_distinct: Option<usize>,
}

impl ColumnDescr {
    #[inline]
    pub fn new(name: &str, column_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            column_type,
            max_num_distinct: None,
        }
    }

    /// set the maximum number of distinct values in this column
    ///
    /// If `None`, the number of distinct values is randomly selected between 1
    /// and the number of rows.
    pub fn with_max_num_distinct(mut self, num_distinct: usize) -> Self {
        self.max_num_distinct = Some(num_distinct);
        self
    }
}

/// Record batch generator
struct RecordBatchGenerator {
    min_rows_nun: usize,

    max_rows_num: usize,

    columns: Vec<ColumnDescr>,

    candidate_null_pcts: Vec<f64>,
}

macro_rules! generate_string_array {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT:expr, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $ARROW_TYPE: ident) => {{
        let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
        let null_pct = $SELF.candidate_null_pcts[null_pct_idx];
        let max_len = $BATCH_GEN_RNG.gen_range(1..50);

        let mut generator = StringArrayGenerator {
            max_len,
            num_strings: $NUM_ROWS,
            num_distinct_strings: $MAX_NUM_DISTINCT,
            null_pct,
            rng: $ARRAY_GEN_RNG,
        };

        match $ARROW_TYPE::DATA_TYPE {
            DataType::Utf8 => generator.gen_data::<i32>(),
            DataType::LargeUtf8 => generator.gen_data::<i64>(),
            DataType::Utf8View => generator.gen_string_view(),
            _ => unreachable!(),
        }
    }};
}

macro_rules! generate_decimal_array {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT: expr, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $PRECISION: ident, $SCALE: ident, $ARROW_TYPE: ident) => {{
        let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
        let null_pct = $SELF.candidate_null_pcts[null_pct_idx];

        let mut generator = DecimalArrayGenerator {
            precision: $PRECISION,
            scale: $SCALE,
            num_decimals: $NUM_ROWS,
            num_distinct_decimals: $MAX_NUM_DISTINCT,
            null_pct,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$ARROW_TYPE>()
    }};
}

// Generating `BooleanArray` due to it being a special type in Arrow (bit-packed)
macro_rules! generate_boolean_array {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT:expr, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $ARROW_TYPE: ident) => {{
        // Select a null percentage from the candidate percentages
        let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
        let null_pct = $SELF.candidate_null_pcts[null_pct_idx];

        let num_distinct_booleans = if $MAX_NUM_DISTINCT >= 2 { 2 } else { 1 };

        let mut generator = BooleanArrayGenerator {
            num_booleans: $NUM_ROWS,
            num_distinct_booleans,
            null_pct,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$ARROW_TYPE>()
    }};
}

macro_rules! generate_primitive_array {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT:expr, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $ARROW_TYPE:ident) => {{
        let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
        let null_pct = $SELF.candidate_null_pcts[null_pct_idx];

        let mut generator = PrimitiveArrayGenerator {
            num_primitives: $NUM_ROWS,
            num_distinct_primitives: $MAX_NUM_DISTINCT,
            null_pct,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$ARROW_TYPE>()
    }};
}

macro_rules! generate_binary_array {
    (
        $SELF:ident,
        $NUM_ROWS:ident,
        $MAX_NUM_DISTINCT:expr,
        $BATCH_GEN_RNG:ident,
        $ARRAY_GEN_RNG:ident,
        $ARROW_TYPE:ident
    ) => {{
        let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
        let null_pct = $SELF.candidate_null_pcts[null_pct_idx];

        let max_len = $BATCH_GEN_RNG.gen_range(1..100);

        let mut generator = BinaryArrayGenerator {
            max_len,
            num_binaries: $NUM_ROWS,
            num_distinct_binaries: $MAX_NUM_DISTINCT,
            null_pct,
            rng: $ARRAY_GEN_RNG,
        };

        match $ARROW_TYPE::DATA_TYPE {
            DataType::Binary => generator.gen_data::<i32>(),
            DataType::LargeBinary => generator.gen_data::<i64>(),
            DataType::BinaryView => generator.gen_binary_view(),
            _ => unreachable!(),
        }
    }};
}

impl RecordBatchGenerator {
    fn new(min_rows_nun: usize, max_rows_num: usize, columns: Vec<ColumnDescr>) -> Self {
        let candidate_null_pcts = vec![0.0, 0.01, 0.1, 0.5];

        Self {
            min_rows_nun,
            max_rows_num,
            columns,
            candidate_null_pcts,
        }
    }

    fn generate(&self) -> Result<RecordBatch> {
        let mut rng = thread_rng();
        let num_rows = rng.gen_range(self.min_rows_nun..=self.max_rows_num);
        let array_gen_rng = StdRng::from_seed(rng.gen());

        // Build arrays
        let mut arrays = Vec::with_capacity(self.columns.len());
        for col in self.columns.iter() {
            let array = self.generate_array_of_type(
                col,
                num_rows,
                &mut rng,
                array_gen_rng.clone(),
            );
            arrays.push(array);
        }

        // Build schema
        let fields = self
            .columns
            .iter()
            .map(|col| Field::new(col.name.clone(), col.column_type.clone(), true))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));

        RecordBatch::try_new(schema, arrays).map_err(|e| arrow_datafusion_err!(e))
    }

    fn generate_array_of_type(
        &self,
        col: &ColumnDescr,
        num_rows: usize,
        batch_gen_rng: &mut ThreadRng,
        array_gen_rng: StdRng,
    ) -> ArrayRef {
        let num_distinct = if num_rows > 1 {
            batch_gen_rng.gen_range(1..num_rows)
        } else {
            num_rows
        };
        // cap to at most the num_distinct values
        let max_num_distinct = col
            .max_num_distinct
            .map(|max| num_distinct.min(max))
            .unwrap_or(num_distinct);

        match col.column_type {
            DataType::Int8 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Int8Type
                )
            }
            DataType::Int16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Int16Type
                )
            }
            DataType::Int32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Int32Type
                )
            }
            DataType::Int64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Int64Type
                )
            }
            DataType::UInt8 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt8Type
                )
            }
            DataType::UInt16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt16Type
                )
            }
            DataType::UInt32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt32Type
                )
            }
            DataType::UInt64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt64Type
                )
            }
            DataType::Float32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Float32Type
                )
            }
            DataType::Float64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Float64Type
                )
            }
            DataType::Date32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Date32Type
                )
            }
            DataType::Date64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Date64Type
                )
            }
            DataType::Time32(TimeUnit::Second) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Time32SecondType
                )
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Time32MillisecondType
                )
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Time64MicrosecondType
                )
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Time64NanosecondType
                )
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    IntervalYearMonthType
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    IntervalDayTimeType
                )
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    IntervalMonthDayNanoType
                )
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    TimestampSecondType
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    TimestampMillisecondType
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    TimestampMicrosecondType
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    TimestampNanosecondType
                )
            }
            DataType::Binary => {
                generate_binary_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    BinaryType
                )
            }
            DataType::LargeBinary => {
                generate_binary_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    LargeBinaryType
                )
            }
            DataType::BinaryView => {
                generate_binary_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    BinaryViewType
                )
            }
            DataType::Decimal128(precision, scale) => {
                generate_decimal_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    precision,
                    scale,
                    Decimal128Type
                )
            }
            DataType::Decimal256(precision, scale) => {
                generate_decimal_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    precision,
                    scale,
                    Decimal256Type
                )
            }
            DataType::Utf8 => {
                generate_string_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    Utf8Type
                )
            }
            DataType::LargeUtf8 => {
                generate_string_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    LargeUtf8Type
                )
            }
            DataType::Utf8View => {
                generate_string_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    StringViewType
                )
            }
            DataType::Boolean => {
                generate_boolean_array! {
                    self,
                    num_rows,
                    max_num_distinct,
                    batch_gen_rng,
                    array_gen_rng,
                    BooleanType
                }
            }
            _ => {
                panic!("Unsupported data generator type: {}", col.column_type)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use arrow::array::UInt32Array;

    use crate::fuzz_cases::aggregation_fuzzer::check_equality_of_batches;

    use super::*;

    #[test]
    fn test_generated_datasets() {
        // The test datasets generation config
        // We expect that after calling `generate`
        //  - Generates two datasets
        //  - They have two columns, "a" and "b",
        //    "a"'s type is `Utf8`, and "b"'s type is `UInt32`
        //  - One of them is unsorted, another is sorted by column "b"
        //  - Their rows num should be same and between [16, 32]
        let config = DatasetGeneratorConfig {
            columns: vec![
                ColumnDescr::new("a", DataType::Utf8),
                ColumnDescr::new("b", DataType::UInt32),
            ],
            rows_num_range: (16, 32),
            sort_keys_set: vec![vec!["b".to_string()]],
        };

        let gen = DatasetGenerator::new(config);
        let datasets = gen.generate().unwrap();

        // Should Generate 2 datasets
        assert_eq!(datasets.len(), 2);

        // Should have 2 column "a" and "b",
        // "a"'s type is `Utf8`, and "b"'s type is `UInt32`
        let check_fields = |batch: &RecordBatch| {
            assert_eq!(batch.num_columns(), 2);
            let fields = batch.schema().fields().clone();
            assert_eq!(fields[0].name(), "a");
            assert_eq!(*fields[0].data_type(), DataType::Utf8);
            assert_eq!(fields[1].name(), "b");
            assert_eq!(*fields[1].data_type(), DataType::UInt32);
        };

        let batch = &datasets[0].batches[0];
        check_fields(batch);
        let batch = &datasets[1].batches[0];
        check_fields(batch);

        // One of the batches should be sorted by "b"
        let sorted_batches = &datasets[1].batches;
        let b_vals = sorted_batches.iter().flat_map(|batch| {
            let uint_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            uint_array.iter()
        });
        let mut prev_b_val = u32::MIN;
        for b_val in b_vals {
            let b_val = b_val.unwrap_or(u32::MIN);
            assert!(b_val >= prev_b_val);
            prev_b_val = b_val;
        }

        // Two batches should be the same after sorting
        check_equality_of_batches(&datasets[0].batches, &datasets[1].batches).unwrap();

        // The number of rows should be between [16, 32]
        let rows_num0 = datasets[0]
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        let rows_num1 = datasets[1]
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        assert_eq!(rows_num0, rows_num1);
        assert!(rows_num0 >= 16);
        assert!(rows_num0 <= 32);
    }
}
