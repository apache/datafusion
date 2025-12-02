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

use arrow::array::{ArrayRef, DictionaryArray, PrimitiveArray, RecordBatch};
use arrow::datatypes::{
    ArrowPrimitiveType, BooleanType, DataType, Date32Type, Date64Type, Decimal128Type,
    Decimal256Type, Decimal32Type, Decimal64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Field,
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType,
    Schema, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use arrow_schema::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE,
    DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE,
};
use datafusion_common::{arrow_datafusion_err, Result};
use rand::{rng, rngs::StdRng, Rng, SeedableRng};
use test_utils::array_gen::{
    BinaryArrayGenerator, BooleanArrayGenerator, DecimalArrayGenerator,
    PrimitiveArrayGenerator, StringArrayGenerator,
};

/// Columns that are supported by the record batch generator
/// The RNG is used to generate the precision and scale for the decimal columns, thread
/// RNG is not used because this is used in fuzzing and deterministic results are preferred
pub fn get_supported_types_columns(rng_seed: u64) -> Vec<ColumnDescr> {
    let mut rng = StdRng::seed_from_u64(rng_seed);
    vec![
        ColumnDescr::new("i8", DataType::Int8),
        ColumnDescr::new("i16", DataType::Int16),
        ColumnDescr::new("i32", DataType::Int32),
        ColumnDescr::new("i64", DataType::Int64),
        ColumnDescr::new("u8", DataType::UInt8),
        ColumnDescr::new("u16", DataType::UInt16),
        ColumnDescr::new("u32", DataType::UInt32),
        ColumnDescr::new("u64", DataType::UInt64),
        ColumnDescr::new("date32", DataType::Date32),
        ColumnDescr::new("date64", DataType::Date64),
        ColumnDescr::new("time32_s", DataType::Time32(TimeUnit::Second)),
        ColumnDescr::new("time32_ms", DataType::Time32(TimeUnit::Millisecond)),
        ColumnDescr::new("time64_us", DataType::Time64(TimeUnit::Microsecond)),
        ColumnDescr::new("time64_ns", DataType::Time64(TimeUnit::Nanosecond)),
        ColumnDescr::new("timestamp_s", DataType::Timestamp(TimeUnit::Second, None)),
        ColumnDescr::new(
            "timestamp_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        ColumnDescr::new(
            "timestamp_us",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        ColumnDescr::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
        ColumnDescr::new("float32", DataType::Float32),
        ColumnDescr::new("float64", DataType::Float64),
        ColumnDescr::new(
            "interval_year_month",
            DataType::Interval(IntervalUnit::YearMonth),
        ),
        ColumnDescr::new(
            "interval_day_time",
            DataType::Interval(IntervalUnit::DayTime),
        ),
        ColumnDescr::new(
            "interval_month_day_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
        ),
        // Internal error: AggregationFuzzer task error: JoinError::Panic(Id(29108), "called `Option::unwrap()` on a `None` value", ...).
        // ColumnDescr::new(
        //     "duration_seconds",
        //     DataType::Duration(TimeUnit::Second),
        // ),
        ColumnDescr::new(
            "duration_milliseconds",
            DataType::Duration(TimeUnit::Millisecond),
        ),
        ColumnDescr::new(
            "duration_microsecond",
            DataType::Duration(TimeUnit::Microsecond),
        ),
        ColumnDescr::new(
            "duration_nanosecond",
            DataType::Duration(TimeUnit::Nanosecond),
        ),
        ColumnDescr::new("decimal32", {
            let precision: u8 = rng.random_range(1..=DECIMAL32_MAX_PRECISION);
            let scale: i8 = rng.random_range(
                i8::MIN..=std::cmp::min(precision as i8, DECIMAL32_MAX_SCALE),
            );
            DataType::Decimal32(precision, scale)
        }),
        ColumnDescr::new("decimal64", {
            let precision: u8 = rng.random_range(1..=DECIMAL64_MAX_PRECISION);
            let scale: i8 = rng.random_range(
                i8::MIN..=std::cmp::min(precision as i8, DECIMAL64_MAX_SCALE),
            );
            DataType::Decimal64(precision, scale)
        }),
        ColumnDescr::new("decimal128", {
            let precision: u8 = rng.random_range(1..=DECIMAL128_MAX_PRECISION);
            let scale: i8 = rng.random_range(
                i8::MIN..=std::cmp::min(precision as i8, DECIMAL128_MAX_SCALE),
            );
            DataType::Decimal128(precision, scale)
        }),
        ColumnDescr::new("decimal256", {
            let precision: u8 = rng.random_range(1..=DECIMAL256_MAX_PRECISION);
            let scale: i8 = rng.random_range(
                i8::MIN..=std::cmp::min(precision as i8, DECIMAL256_MAX_SCALE),
            );
            DataType::Decimal256(precision, scale)
        }),
        ColumnDescr::new("utf8", DataType::Utf8),
        ColumnDescr::new("largeutf8", DataType::LargeUtf8),
        ColumnDescr::new("utf8view", DataType::Utf8View),
        ColumnDescr::new("u8_low", DataType::UInt8).with_max_num_distinct(10),
        ColumnDescr::new("utf8_low", DataType::Utf8).with_max_num_distinct(10),
        ColumnDescr::new("bool", DataType::Boolean),
        ColumnDescr::new("binary", DataType::Binary),
        ColumnDescr::new("large_binary", DataType::LargeBinary),
        ColumnDescr::new("binaryview", DataType::BinaryView),
        ColumnDescr::new(
            "dictionary_utf8_low",
            DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
        )
        .with_max_num_distinct(10),
    ]
}

#[derive(Debug, Clone)]
pub struct ColumnDescr {
    /// Column name
    pub name: String,

    /// Data type of this column
    pub column_type: DataType,

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

    pub fn get_max_num_distinct(&self) -> Option<usize> {
        self.max_num_distinct
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
pub struct RecordBatchGenerator {
    pub min_rows_num: usize,

    pub max_rows_num: usize,

    pub columns: Vec<ColumnDescr>,

    pub candidate_null_pcts: Vec<f64>,

    /// If a seed is provided when constructing the generator, it will be used to
    /// create `rng` and the pseudo-randomly generated batches will be deterministic.
    /// Otherwise, `rng` will be initialized using `rng()` and the batches
    /// generated will be different each time.
    rng: StdRng,
}

macro_rules! generate_decimal_array {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT: expr, $NULL_PCT:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $PRECISION: ident, $SCALE: ident, $ARROW_TYPE: ident) => {{
        let mut generator = DecimalArrayGenerator {
            precision: $PRECISION,
            scale: $SCALE,
            num_decimals: $NUM_ROWS,
            num_distinct_decimals: $MAX_NUM_DISTINCT,
            null_pct: $NULL_PCT,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$ARROW_TYPE>()
    }};
}

// Generating `BooleanArray` due to it being a special type in Arrow (bit-packed)
macro_rules! generate_boolean_array {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT:expr, $NULL_PCT:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $ARROW_TYPE: ident) => {{
        let num_distinct_booleans = if $MAX_NUM_DISTINCT >= 2 { 2 } else { 1 };

        let mut generator = BooleanArrayGenerator {
            num_booleans: $NUM_ROWS,
            num_distinct_booleans,
            null_pct: $NULL_PCT,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$ARROW_TYPE>()
    }};
}

macro_rules! generate_primitive_array {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT:expr, $NULL_PCT:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $ARROW_TYPE:ident) => {{
        let mut generator = PrimitiveArrayGenerator {
            num_primitives: $NUM_ROWS,
            num_distinct_primitives: $MAX_NUM_DISTINCT,
            null_pct: $NULL_PCT,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$ARROW_TYPE>()
    }};
}

macro_rules! generate_dict {
    ($SELF:ident, $NUM_ROWS:ident, $MAX_NUM_DISTINCT:expr, $NULL_PCT:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $ARROW_TYPE:ident, $VALUES: ident) => {{
        debug_assert_eq!($VALUES.len(), $MAX_NUM_DISTINCT);
        let keys: PrimitiveArray<$ARROW_TYPE> = (0..$NUM_ROWS)
            .map(|_| {
                if $BATCH_GEN_RNG.random::<f64>() < $NULL_PCT {
                    None
                } else if $MAX_NUM_DISTINCT > 1 {
                    let range = 0..($MAX_NUM_DISTINCT
                        as <$ARROW_TYPE as ArrowPrimitiveType>::Native);
                    Some($ARRAY_GEN_RNG.random_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        let dict = DictionaryArray::new(keys, $VALUES);
        Arc::new(dict) as ArrayRef
    }};
}

impl RecordBatchGenerator {
    /// Create a new `RecordBatchGenerator` with a random seed. The generated
    /// batches will be different each time.
    pub fn new(
        min_rows_nun: usize,
        max_rows_num: usize,
        columns: Vec<ColumnDescr>,
    ) -> Self {
        let candidate_null_pcts = vec![0.0, 0.01, 0.1, 0.5];

        Self {
            min_rows_num: min_rows_nun,
            max_rows_num,
            columns,
            candidate_null_pcts,
            rng: StdRng::from_rng(&mut rng()),
        }
    }

    /// Set a seed for the generator. The pseudo-randomly generated batches will be
    /// deterministic for the same seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rng = StdRng::seed_from_u64(seed);
        self
    }

    pub fn generate(&mut self) -> Result<RecordBatch> {
        let num_rows = self.rng.random_range(self.min_rows_num..=self.max_rows_num);
        let array_gen_rng = StdRng::from_seed(self.rng.random());
        let mut batch_gen_rng = StdRng::from_seed(self.rng.random());
        let columns = self.columns.clone();

        // Build arrays
        let mut arrays = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let array = self.generate_array_of_type(
                col,
                num_rows,
                &mut batch_gen_rng,
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
        &mut self,
        col: &ColumnDescr,
        num_rows: usize,
        batch_gen_rng: &mut StdRng,
        array_gen_rng: StdRng,
    ) -> ArrayRef {
        let null_pct_idx = batch_gen_rng.random_range(0..self.candidate_null_pcts.len());
        let null_pct = self.candidate_null_pcts[null_pct_idx];

        Self::generate_array_of_type_inner(
            col,
            num_rows,
            batch_gen_rng,
            array_gen_rng,
            null_pct,
        )
    }

    fn generate_array_of_type_inner(
        col: &ColumnDescr,
        num_rows: usize,
        batch_gen_rng: &mut StdRng,
        array_gen_rng: StdRng,
        null_pct: f64,
    ) -> ArrayRef {
        let num_distinct = if num_rows > 1 {
            batch_gen_rng.random_range(1..num_rows)
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    IntervalMonthDayNanoType
                )
            }
            DataType::Duration(TimeUnit::Second) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    DurationSecondType
                )
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    DurationMillisecondType
                )
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    DurationMicrosecondType
                )
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    DurationNanosecondType
                )
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
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
                    null_pct,
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
                    null_pct,
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
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    TimestampNanosecondType
                )
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let max_len = batch_gen_rng.random_range(1..50);

                let mut generator = StringArrayGenerator {
                    max_len,
                    num_strings: num_rows,
                    num_distinct_strings: max_num_distinct,
                    null_pct,
                    rng: array_gen_rng,
                };

                match col.column_type {
                    DataType::Utf8 => generator.gen_data::<i32>(),
                    DataType::LargeUtf8 => generator.gen_data::<i64>(),
                    DataType::Utf8View => generator.gen_string_view(),
                    _ => unreachable!(),
                }
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let max_len = batch_gen_rng.random_range(1..100);

                let mut generator = BinaryArrayGenerator {
                    max_len,
                    num_binaries: num_rows,
                    num_distinct_binaries: max_num_distinct,
                    null_pct,
                    rng: array_gen_rng,
                };

                match col.column_type {
                    DataType::Binary => generator.gen_data::<i32>(),
                    DataType::LargeBinary => generator.gen_data::<i64>(),
                    DataType::BinaryView => generator.gen_binary_view(),
                    _ => unreachable!(),
                }
            }
            DataType::Decimal32(precision, scale) => {
                generate_decimal_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    precision,
                    scale,
                    Decimal32Type
                )
            }
            DataType::Decimal64(precision, scale) => {
                generate_decimal_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    precision,
                    scale,
                    Decimal64Type
                )
            }
            DataType::Decimal128(precision, scale) => {
                generate_decimal_array!(
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
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
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    precision,
                    scale,
                    Decimal256Type
                )
            }
            DataType::Boolean => {
                generate_boolean_array! {
                    self,
                    num_rows,
                    max_num_distinct,
                    null_pct,
                    batch_gen_rng,
                    array_gen_rng,
                    BooleanType
                }
            }
            DataType::Dictionary(ref key_type, ref value_type)
                if key_type.is_dictionary_key_type() =>
            {
                // We generate just num_distinct values because they will be reused by different keys
                let mut array_gen_rng = array_gen_rng;
                debug_assert!((0.0..=1.0).contains(&null_pct));
                let values = Self::generate_array_of_type_inner(
                    &ColumnDescr::new("values", *value_type.clone()),
                    num_distinct,
                    batch_gen_rng,
                    array_gen_rng.clone(),
                    null_pct, // generate some null values
                );

                match key_type.as_ref() {
                    // new key types can be added here
                    DataType::UInt64 => generate_dict!(
                        self,
                        num_rows,
                        num_distinct,
                        null_pct,
                        batch_gen_rng,
                        array_gen_rng,
                        UInt64Type,
                        values
                    ),
                    _ => panic!("Invalid dictionary keys type: {key_type}"),
                }
            }
            _ => {
                panic!("Unsupported data generator type: {}", col.column_type)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator_with_fixed_seed_deterministic() {
        let mut gen1 = RecordBatchGenerator::new(
            16,
            32,
            vec![
                ColumnDescr::new("a", DataType::Utf8),
                ColumnDescr::new("b", DataType::UInt32),
            ],
        )
        .with_seed(310104);

        let mut gen2 = RecordBatchGenerator::new(
            16,
            32,
            vec![
                ColumnDescr::new("a", DataType::Utf8),
                ColumnDescr::new("b", DataType::UInt32),
            ],
        )
        .with_seed(310104);

        let batch1 = gen1.generate().unwrap();
        let batch2 = gen2.generate().unwrap();

        let batch1_formatted = format!("{batch1:?}");
        let batch2_formatted = format!("{batch2:?}");

        assert_eq!(batch1_formatted, batch2_formatted);
    }
}
