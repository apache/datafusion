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

use arrow::datatypes::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result};
use datafusion_physical_expr::{expressions::col, PhysicalSortExpr};
use datafusion_physical_plan::sorts::sort::sort_batch;
use rand::{
    rngs::{StdRng, ThreadRng},
    thread_rng, Rng, SeedableRng,
};
use test_utils::{
    array_gen::{PrimitiveArrayGenerator, StringArrayGenerator},
    stagger_batch,
};

/// Config for Data sets generator
///
/// # Parameters
///   - `columns`, you just need to define `column name`s and `column data type`s
///     fot the test datasets, and then they will be randomly generated from generator
///     when you can `generate` function
///         
///   - `rows_num_range`, the rows num of the datasets will be randomly generated
///      among this range
///
///   - `sort_keys`, if `sort_keys` are defined, when you can `generate`, the generator
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
    /// element in `sort_keys_set`, an additional datasets is created that
    /// is sorted by these values as well.
    pub sort_keys_set: Vec<Vec<String>>,
}

impl DatasetGeneratorConfig {
    /// return a list of all column names
    pub fn all_columns(&self) -> Vec<&str> {
        self.columns.iter().map(|d| d.name.as_str()).collect()
    }

    /// return a list of column names that are "numeric"
    pub fn numeric_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter_map(|d| {
                if d.column_type.is_numeric() {
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
/// It will generate one random [`Dataset`]s when `generate` function is called.
///
/// The generation logic in `generate`:
///
///   - Randomly generate a base record from `batch_generator` firstly.
///     And `columns`, `rows_num_range` in `config`(detail can see `DataSetsGeneratorConfig`),
///     will be used in generation.
///   
///   - Sort the batch according to `sort_keys` in `config` to generator another
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
                .collect::<Result<Vec<_>>>()?;
            let sorted_batch = sort_batch(&base_batch, &sort_exprs, None)?;

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
    // Column name
    name: String,

    // Data type of this column
    column_type: DataType,
}

impl ColumnDescr {
    #[inline]
    pub fn new(name: &str, column_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            column_type,
        }
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
    ($SELF:ident, $NUM_ROWS:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $OFFSET_TYPE:ty) => {{
        let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
        let null_pct = $SELF.candidate_null_pcts[null_pct_idx];
        let max_len = $BATCH_GEN_RNG.gen_range(1..50);
        let num_distinct_strings = if $NUM_ROWS > 1 {
            $BATCH_GEN_RNG.gen_range(1..$NUM_ROWS)
        } else {
            $NUM_ROWS
        };

        let mut generator = StringArrayGenerator {
            max_len,
            num_strings: $NUM_ROWS,
            num_distinct_strings,
            null_pct,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$OFFSET_TYPE>()
    }};
}

macro_rules! generate_primitive_array {
    ($SELF:ident, $NUM_ROWS:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $ARROW_TYPE:ident) => {
        paste::paste! {{
            let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
            let null_pct = $SELF.candidate_null_pcts[null_pct_idx];
            let num_distinct_primitives = if $NUM_ROWS > 1 {
                $BATCH_GEN_RNG.gen_range(1..$NUM_ROWS)
            } else {
                $NUM_ROWS
            };

            let mut generator = PrimitiveArrayGenerator {
                num_primitives: $NUM_ROWS,
                num_distinct_primitives,
                null_pct,
                rng: $ARRAY_GEN_RNG,
            };

            generator.gen_data::<$ARROW_TYPE>()
    }}}
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
                col.column_type.clone(),
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
        data_type: DataType,
        num_rows: usize,
        batch_gen_rng: &mut ThreadRng,
        array_gen_rng: StdRng,
    ) -> ArrayRef {
        match data_type {
            DataType::Int8 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Int8Type
                )
            }
            DataType::Int16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Int16Type
                )
            }
            DataType::Int32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Int32Type
                )
            }
            DataType::Int64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Int64Type
                )
            }
            DataType::UInt8 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt8Type
                )
            }
            DataType::UInt16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt16Type
                )
            }
            DataType::UInt32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt32Type
                )
            }
            DataType::UInt64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    UInt64Type
                )
            }
            DataType::Float32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Float32Type
                )
            }
            DataType::Float64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Float64Type
                )
            }
            DataType::Date32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Date32Type
                )
            }
            DataType::Date64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    Date64Type
                )
            }
            DataType::Utf8 => {
                generate_string_array!(self, num_rows, batch_gen_rng, array_gen_rng, i32)
            }
            DataType::LargeUtf8 => {
                generate_string_array!(self, num_rows, batch_gen_rng, array_gen_rng, i64)
            }
            _ => {
                panic!("Unsupported data generator type: {data_type}")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use arrow_array::UInt32Array;

    use crate::fuzz_cases::aggregation_fuzzer::check_equality_of_batches;

    use super::*;

    #[test]
    fn test_generated_datasets() {
        // The test datasets generation config
        // We expect that after calling `generate`
        //  - Generate 2 datasets
        //  - They have 2 column "a" and "b",
        //    "a"'s type is `Utf8`, and "b"'s type is `UInt32`
        //  - One of them is unsorted, another is sorted by column "b"
        //  - Their rows num should be same and between [16, 32]
        let config = DatasetGeneratorConfig {
            columns: vec![
                ColumnDescr {
                    name: "a".to_string(),
                    column_type: DataType::Utf8,
                },
                ColumnDescr {
                    name: "b".to_string(),
                    column_type: DataType::UInt32,
                },
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

        // One batches should be sort by "b"
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

        // Two batches should be same after sorting
        check_equality_of_batches(&datasets[0].batches, &datasets[1].batches).unwrap();

        // Rows num should between [16, 32]
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
