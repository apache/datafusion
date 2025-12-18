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

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::sorts::sort::sort_batch;
use test_utils::stagger_batch;

use crate::fuzz_cases::record_batch_generator::{ColumnDescr, RecordBatchGenerator};

/// Config for Dataset generator
///
/// # Parameters
///   - `columns`, you just need to define `column name`s and `column data type`s
///     for the test datasets, and then they will be randomly generated from the generator
///     when you call `generate` function
///         
///   - `rows_num_range`, the number of rows in the datasets will be randomly generated
///     within this range
///
///   - `sort_keys`, if `sort_keys` are defined, when you call the `generate` function, the generator
///     will generate one `base dataset` firstly. Then the `base dataset` will be sorted
///     based on each `sort_key` respectively. And finally `len(sort_keys) + 1` datasets
///     will be returned
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
/// It will generate random [`Dataset`]s when the `generate` function is called. For each
/// sort key in `sort_keys_set`, an additional sorted dataset will be generated, and the
/// dataset will be chunked into staggered batches.
///
/// # Example
/// For `DatasetGenerator` with `sort_keys_set = [["a"], ["b"]]`, it will generate 2
/// datasets. The first one will be sorted by column `a` and get randomly chunked
/// into staggered batches. It might look like the following:
/// ```text
/// a b
/// ----
/// 1 2 <-- batch 1
/// 1 1
///
/// 2 1 <-- batch 2
///
/// 3 3 <-- batch 3
/// 4 3
/// 4 1
/// ```
///
/// # Implementation details:
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

    pub fn generate(&mut self) -> Result<Vec<Dataset>> {
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
                .map(|key| col(key, schema).map(PhysicalSortExpr::new_default))
                .collect::<Result<Vec<_>>>()?;
            let batch = if let Some(ordering) = LexOrdering::new(sort_exprs) {
                sort_batch(&base_batch, &ordering, None)?
            } else {
                base_batch.clone()
            };
            let batches = stagger_batch(batch);
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

        let mut data_gen = DatasetGenerator::new(config);
        let datasets = data_gen.generate().unwrap();

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
