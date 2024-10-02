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

use arrow::array::ArrayBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion_physical_expr::{expressions::col, PhysicalSortExpr};
use datafusion_physical_plan::sorts::sort::sort_batch;
use rand::{
    rngs::{StdRng, ThreadRng},
    thread_rng, Rng, SeedableRng,
};
use rand_distr::Alphanumeric;
use test_utils::{
    array_gen::{PrimitiveArrayGenerator, StringArrayGenerator},
    stagger_batch, StringBatchGenerator,
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
pub struct DatasetsGeneratorBuilder {
    // Descriptions of columns in datasets, it's `required`
    columns: Option<Vec<ColumnDescr>>,

    // Rows num range of the generated datasets, it's `required`
    rows_num_range: Option<(usize, usize)>,

    // Sort keys used to generate the sorted data set, it's optional
    sort_keys_set: Vec<Vec<String>>,
}

impl DatasetsGeneratorBuilder {
    pub fn build(self) -> DataSetsGenerator {
        let columns = self.columns.expect("columns is required");
        let rows_num_range = self.rows_num_range.expect("rows_num_range is required");

        let batch_generator =
            RecordBatchGenerator::new(rows_num_range.0, rows_num_range.1, columns);

        DataSetsGenerator {
            sort_keys_set: self.sort_keys_set,
            batch_generator,
        }
    }

    pub fn columns(mut self, columns: Vec<ColumnDescr>) -> Self {
        self.columns = Some(columns);
        self
    }

    pub fn rows_num_range(mut self, rows_num_range: (usize, usize)) -> Self {
        self.rows_num_range = Some(rows_num_range);
        self
    }

    pub fn sort_keys_set(mut self, sort_keys_set: Vec<Vec<String>>) -> Self {
        self.sort_keys_set = sort_keys_set;
        self
    }
}

/// Data sets generator
///
/// It will generate one `dataset`s when `generate` function is called.
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
struct DataSetsGenerator {
    sort_keys_set: Vec<Vec<String>>,
    batch_generator: RecordBatchGenerator,
}

impl DataSetsGenerator {
    fn generate(&self) -> Vec<Dataset> {
        let mut datasets = Vec::with_capacity(self.sort_keys_set.len() + 1);

        // Generate the base batch
        let base_batch = self.batch_generator.generate();
        let total_rows_num = base_batch.num_rows();
        let batches = stagger_batch(base_batch.clone());
        let dataset = Dataset {
            batches,
            total_rows_num,
            sort_keys: Vec::new(),
        };
        datasets.push(dataset);

        // Generate the related sorted batches
        let schema = base_batch.schema_ref();
        for sort_keys in self.sort_keys_set.clone() {
            let sort_exprs = sort_keys
                .iter()
                .map(|key| {
                    let col_expr = col(&key, &schema)
                        .expect(
                            &format!("sort key must be valid, invalid sort key:{key}, schema:{schema:?}")
                        );
                    PhysicalSortExpr::new_default(col_expr)
                })
                .collect::<Vec<_>>();
            let sorted_batch = sort_batch(&base_batch, &sort_exprs, None)
                .expect("sort batch should not fail");

            let batches = stagger_batch(sorted_batch);
            let dataset = Dataset {
                batches,
                total_rows_num,
                sort_keys: Vec::new(),
            };
            datasets.push(dataset);
        }

        datasets
    }
}

/// Single test data set
#[derive(Debug)]
pub struct Dataset {
    pub batches: Vec<RecordBatch>,
    pub total_rows_num: usize,
    pub sort_keys: Vec<String>,
}

#[derive(Debug, Clone)]
struct ColumnDescr {
    // Column name
    name: String,

    // Data type of this column
    column_type: DataType,
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
    ($SELF:ident, $NUM_ROWS:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $DATA_TYPE:ident) => {
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

            generator.[< gen_data_ $DATA_TYPE >]()
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

    fn generate(&self) -> RecordBatch {
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

        RecordBatch::try_new(schema, arrays).unwrap()
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
                    i8
                )
            }
            DataType::Int16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    i16
                )
            }
            DataType::Int32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    i32
                )
            }
            DataType::Int64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    i64
                )
            }
            DataType::UInt8 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u8
                )
            }
            DataType::UInt16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u16
                )
            }
            DataType::UInt32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u32
                )
            }
            DataType::UInt64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u64
                )
            }
            DataType::Float32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    f32
                )
            }
            DataType::Float64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    f64
                )
            }
            DataType::Utf8 => {
                generate_string_array!(self, num_rows, batch_gen_rng, array_gen_rng, i32)
            }
            DataType::LargeUtf8 => {
                generate_string_array!(self, num_rows, batch_gen_rng, array_gen_rng, i64)
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    // use arrow::util::pretty::pretty_format_batches;

    // use super::*;

    // #[test]
    // fn simple_test() {
    //     let config = DatasetsGeneratorBuilder {
    //         columns: vec![
    //             ColumnDescr {
    //                 name: "a".to_string(),
    //                 column_type: DataType::Utf8,
    //             },
    //             ColumnDescr {
    //                 name: "b".to_string(),
    //                 column_type: DataType::UInt32,
    //             },
    //         ],
    //         rows_num_range: (16, 32),
    //         sort_keys_set: vec![vec!["b".to_string()]],
    //     };

    //     let gen = DataSetsGenerator::new(config);
    //     let datasets = gen.generate();

    //     for (round, dataset) in datasets.into_iter().enumerate() {
    //         println!("### round:{round} ###");
    //         let num_rows = dataset
    //             .batches
    //             .iter()
    //             .map(|batch| batch.num_rows())
    //             .collect::<Vec<_>>();
    //         println!("num_rows:{num_rows:?}");
    //         println!("{}", pretty_format_batches(&dataset.batches).unwrap());
    //     }
    // }
}
