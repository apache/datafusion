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

use crate::aggregates::group_values::GroupValues;
use arrow::array::{
    Array, ArrayRef, ListArray, PrimitiveArray, RunArray, StructArray,
    downcast_run_end_index,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{Result, internal_err};
use datafusion_execution::memory_pool::proxy::{HashTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use hashbrown::hash_table::HashTable;
use log::debug;
use std::mem::size_of;
use std::sync::Arc;

/// A [`GroupValues`] making use of [`Rows`]
///
/// This is a general implementation of [`GroupValues`] that works for any
/// combination of data types and number of columns, including nested types such as
/// structs and lists.
///
/// It uses the arrow-rs [`Rows`] to store the group values, which is a row-wise
/// representation.
pub struct GroupValuesRows {
    /// The output schema
    schema: SchemaRef,

    /// Converter for the group values
    row_converter: RowConverter,

    /// Logically maps group values to a group_index in
    /// [`Self::group_values`] and in each accumulator
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys (group values) in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, group_index)
    map: HashTable<(u64, usize)>,

    /// The size of `map` in bytes
    map_size: usize,

    /// The actual group by values, stored in arrow [`Row`] format.
    /// `group_values[i]` holds the group value for group_index `i`.
    ///
    /// The row format is used to compare group keys quickly and store
    /// them efficiently in memory. Quick comparison is especially
    /// important for multi-column group keys.
    ///
    /// [`Row`]: arrow::row::Row
    group_values: Option<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    /// Random state for creating hashes
    random_state: RandomState,
}

impl GroupValuesRows {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        // Print a debugging message, so it is clear when the (slower) fallback
        // GroupValuesRows is used.
        debug!("Creating GroupValuesRows for schema: {schema}");
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let map = HashTable::with_capacity(0);

        let starting_rows_capacity = 1000;

        let starting_data_capacity = 64 * starting_rows_capacity;
        let rows_buffer =
            row_converter.empty_rows(starting_rows_capacity, starting_data_capacity);
        Ok(Self {
            schema,
            row_converter,
            map,
            map_size: 0,
            group_values: None,
            hashes_buffer: Default::default(),
            rows_buffer,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        })
    }
}

impl GroupValues for GroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // Convert the group keys into the row format
        let group_rows = &mut self.rows_buffer;
        group_rows.clear();
        self.row_converter.append(group_rows, cols)?;
        let n_rows = group_rows.num_rows();

        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => self.row_converter.empty_rows(0, 0),
        };

        // tracks to which group each of the input rows belongs
        groups.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let entry = self.map.find_mut(target_hash, |(exist_hash, group_idx)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                target_hash == *exist_hash
                    // verify that the group that we are inserting with hash is
                    // actually the same key value as the group in
                    // existing_idx  (aka group_values @ row)
                    && group_rows.row(row) == group_values.row(*group_idx)
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    let group_idx = group_values.num_rows();
                    group_values.push(group_rows.row(row));

                    // for hasher function, use precomputed hash value
                    self.map.insert_accounted(
                        (target_hash, group_idx),
                        |(hash, _group_index)| *hash,
                        &mut self.map_size,
                    );
                    group_idx
                }
            };
            groups.push(group_idx);
        }

        self.group_values = Some(group_values);

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size = self.group_values.as_ref().map(|v| v.size()).unwrap_or(0);
        self.row_converter.size()
            + group_values_size
            + self.map_size
            + self.rows_buffer.size()
            + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values
            .as_ref()
            .map(|group_values| group_values.num_rows())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = self
            .group_values
            .take()
            .expect("Can not emit from empty rows");

        let mut output = match emit_to {
            EmitTo::All => {
                let output = self.row_converter.convert_rows(&group_values)?;
                group_values.clear();
                self.map.clear();
                output
            }
            EmitTo::First(n) => {
                let groups_rows = group_values.iter().take(n);
                let output = self.row_converter.convert_rows(groups_rows)?;
                // Clear out first n group keys by copying them to a new Rows.
                // TODO file some ticket in arrow-rs to make this more efficient?
                let mut new_group_values = self.row_converter.empty_rows(0, 0);
                for row in group_values.iter().skip(n) {
                    new_group_values.push(row);
                }
                std::mem::swap(&mut new_group_values, &mut group_values);

                self.map.retain(|(_exists_hash, group_idx)| {
                    // Decrement group index by n
                    match group_idx.checked_sub(n) {
                        // Group index was >= n, shift value down
                        Some(sub) => {
                            *group_idx = sub;
                            true
                        }
                        // Group index was < n, so remove from table
                        None => false,
                    }
                });
                output
            }
        };

        // TODO: Materialize dictionaries in group keys
        // https://github.com/apache/datafusion/issues/7647
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            *array = dictionary_encode_if_necessary(array, expected)?;
        }

        self.group_values = Some(group_values);
        Ok(output)
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.group_values = self.group_values.take().map(|mut rows| {
            rows.clear();
            rows
        });
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(num_rows);
    }
}

fn dictionary_encode_if_necessary(
    array: &ArrayRef,
    expected: &DataType,
) -> Result<ArrayRef> {
    match (expected, array.data_type()) {
        (DataType::Struct(expected_fields), _) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays = expected_fields
                .iter()
                .zip(struct_array.columns())
                .map(|(expected_field, column)| {
                    dictionary_encode_if_necessary(column, expected_field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Arc::new(StructArray::try_new(
                expected_fields.clone(),
                arrays,
                struct_array.nulls().cloned(),
            )?))
        }
        (DataType::List(expected_field), &DataType::List(_)) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();

            Ok(Arc::new(ListArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_field),
                list.offsets().clone(),
                dictionary_encode_if_necessary(
                    list.values(),
                    expected_field.data_type(),
                )?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Dictionary(_, _), _) => Ok(cast(array.as_ref(), expected)?),
        (
            DataType::RunEndEncoded(run_ends_field, expected_values_field),
            &DataType::RunEndEncoded(_, _),
        ) => {
            macro_rules! reencode_ree {
                ($run_end_type:ty) => {{
                    let run_array = array
                        .as_any()
                        .downcast_ref::<RunArray<$run_end_type>>()
                        .unwrap();
                    let values = dictionary_encode_if_necessary(
                        &(Arc::clone(run_array.values()) as ArrayRef),
                        expected_values_field.data_type(),
                    )?;
                    let run_ends = PrimitiveArray::<$run_end_type>::new(
                        run_array.run_ends().inner().clone(),
                        None,
                    );
                    Ok(Arc::new(RunArray::try_new(&run_ends, &values)?))
                }};
            }
            downcast_run_end_index! {
                run_ends_field.data_type() => (reencode_ree),
                _ => unreachable!("unsupported run end type: {}", run_ends_field.data_type()),
            }
        }
        (DataType::RunEndEncoded(_, _), _) => Ok(cast(array.as_ref(), expected)?),
        (_, _) => Ok(Arc::<dyn Array>::clone(array)),
    }
}

mod playground {
    use std::ops::Index;

    use arrow::array::{AsArray, Datum};
    use datafusion_execution::TaskContext;

    use crate::{ExecutionPlan, test::TestMemoryExec};

    use super::*;
    use arrow::array::{Array, ArrayRef, StringArray};
    use std::string::String;
    struct TrivialGroupBy {
        seen_strings: Vec<String>,
        cur_size: usize,
    }
    impl GroupValues for TrivialGroupBy {
        // trivial apprach assume theres only one columns
        fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
            let n_rows = cols[0].len();
            let column_count = cols.len();
            // iterate all the rows, for each row, concate each value into a final string
            for row_idx in 0..n_rows {
                // grab the underlying value; assume its a string
                let mut cur_str = String::from("");
                for col_idx in 0..column_count {
                    let array = cols.get(col_idx).unwrap();
                    let string_array =
                        array.as_any().downcast_ref::<StringArray>().unwrap();
                    if string_array.is_valid(row_idx) {
                        let value = string_array.value(row_idx);
                        cur_str.push_str(value);
                    }
                }
                let idx = if let Some(i) =
                    self.seen_strings.iter().position(|x| x == &cur_str)
                {
                    i
                } else {
                    self.seen_strings.push(cur_str);
                    self.seen_strings.len() - 1
                };
                groups.push(idx);
            }
            println!("{:?}", self.seen_strings);
            Ok(())
        }
        fn len(&self) -> usize {
            self.seen_strings.len()
        }
        fn is_empty(&self) -> bool {
            self.seen_strings.is_empty()
        }
        fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
            let strings_to_emit = match emit_to {
                EmitTo::All => {
                    // take all groups, clear internal state
                    std::mem::take(&mut self.seen_strings)
                }
                EmitTo::First(n) => {
                    // take only the first n groups
                    // drain removes them from seen_strings
                    self.seen_strings.drain(..n).collect()
                }
            };

            // convert our Vec<String> back into an Arrow StringArray
            let array: ArrayRef = Arc::new(StringArray::from(
                strings_to_emit
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            ));

            // return one array per GROUP BY column
            // since we're trivial and only support one column, just return one
            Ok(vec![array])
        }

        fn size(&self) -> usize {
            self.cur_size
        }
        fn clear_shrink(&mut self, num_rows: usize) {
            let _ = num_rows;
        }
    }

    #[test]
    fn test_trivial_group_by_single_column() {
        // Test grouping on a single string column
        let strings = vec!["apple", "banana", "apple", "cherry", "banana"];
        let array: ArrayRef = Arc::new(StringArray::from(strings));

        let mut group_by = TrivialGroupBy {
            seen_strings: Vec::new(),
            cur_size: 0,
        };

        // Intern the group keys
        let mut groups = Vec::new();
        group_by.intern(&[array], &mut groups).unwrap();

        // Should have assigned group ids: 0, 1, 0, 2, 1
        assert_eq!(groups, vec![0, 1, 0, 2, 1]);
        assert_eq!(group_by.len(), 3); // apple, banana, cherry

        // Emit all groups
        let emitted = group_by.emit(EmitTo::All).unwrap();
        assert_eq!(emitted.len(), 1); // One column
        let emitted_array = emitted[0].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(emitted_array.value(0), "apple");
        assert_eq!(emitted_array.value(1), "banana");
        assert_eq!(emitted_array.value(2), "cherry");
    }

    #[test]
    fn test_trivial_group_by_two_columns() {
        // Test grouping on two string columns
        let col1 = vec!["a", "a", "b", "a", "b"];
        let col2 = vec!["x", "y", "x", "x", "y"];

        let array1: ArrayRef = Arc::new(StringArray::from(col1));
        let array2: ArrayRef = Arc::new(StringArray::from(col2));

        let mut group_by = TrivialGroupBy {
            seen_strings: Vec::new(),
            cur_size: 0,
        };

        // Intern: concatenates ("a" + "x"), ("a" + "y"), ("b" + "x"), etc.
        let mut groups = Vec::new();
        group_by.intern(&[array1, array2], &mut groups).unwrap();

        // Should have 4 distinct groups: "ax", "ay", "bx", "by"
        assert_eq!(group_by.len(), 4);
        assert_eq!(groups, vec![0, 1, 2, 0, 3]); // group ids assigned

        // Emit all groups
        let emitted = group_by.emit(EmitTo::All).unwrap();
        assert_eq!(emitted.len(), 1); // One output column (concatenated strings)
        let emitted_array = emitted[0].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(emitted_array.value(0), "ax");
        assert_eq!(emitted_array.value(1), "ay");
        assert_eq!(emitted_array.value(2), "bx");
        assert_eq!(emitted_array.value(3), "by");
    }

    #[tokio::test]
    async fn test_trivial_group_by_dictionary() -> Result<()> {
        use crate::aggregates::RecordBatch;
        use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
        use crate::common::collect;
        use crate::test::TestMemoryExec;
        use arrow::array::DictionaryArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_functions_aggregate::count::count_udaf;
        use datafusion_physical_expr::aggregate::AggregateExprBuilder;
        use datafusion_physical_expr::expressions::col;

        // Create schema with dictionary column and value column
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "color",
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("amount", DataType::UInt32, false),
        ]));

        // Create dictionary array
        let values = StringArray::from(vec!["red", "blue", "green"]);
        let keys = arrow::array::UInt8Array::from(vec![0, 1, 0, 2, 1]);
        let dict_array: ArrayRef = Arc::new(DictionaryArray::<
            arrow::datatypes::UInt8Type,
        >::try_new(keys, Arc::new(values))?);

        // Create value column
        let amount_array: ArrayRef =
            Arc::new(arrow::array::UInt32Array::from(vec![1, 2, 3, 4, 5]));

        // Create batch
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![dict_array, amount_array])?;

        // Create in-memory source with the batch
        let source =
            TestMemoryExec::try_new(&vec![vec![batch]], Arc::clone(&schema), None)?;

        // Create GROUP BY expression
        let group_expr = vec![(col("color", &schema)?, "color".to_string())];

        // Create COUNT(amount) aggregate expression
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("amount", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_amount")
                .build()?,
        )];

        // Create AggregateExec
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::SinglePartitioned,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            Arc::new(source),
            Arc::clone(&schema),
        )?;

        let output =
            collect(aggregate_exec.execute(0, Arc::new(TaskContext::default()))?).await?;
        println!("Output batch: {:#?}", output);
        Ok(())
    }
}
