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

//! DataFusion data sources

pub mod datasource;
pub mod empty;
pub mod file_format;
pub mod listing;
pub mod memory;
pub mod object_store;

use arrow::datatypes::{DataType, Field};
use futures::Stream;

pub use self::datasource::{TableProvider, TableType};
pub use self::memory::MemTable;
use self::object_store::{FileMeta, SizedFile};
use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::error::Result;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::{Accumulator, ColumnStatistics, Statistics};
use crate::scalar::ScalarValue;
use futures::StreamExt;
use std::pin::Pin;

/// Get all files as well as the file level summary statistics (no statistic for partition columns).
/// If the optional `limit` is provided, includes only sufficient files.
/// Needed to read up to `limit` number of rows.
/// TODO fix case where `num_rows` and `total_byte_size` are not defined (stat should be None instead of Some(0))
pub async fn get_statistics_with_limit(
    all_files: impl Stream<Item = Result<(PartitionedFile, Statistics)>>,
    file_schema: SchemaRef,
    limit: Option<usize>,
) -> Result<(Vec<PartitionedFile>, Statistics)> {
    let mut result_files = vec![];

    let total_num_fields = total_number_of_fields(&file_schema);
    let mut total_byte_size = 0;
    let mut null_counts = vec![0; total_num_fields];
    let mut has_statistics = false;
    let (mut max_values, mut min_values) = create_max_min_accs(&file_schema);

    let mut num_rows = 0;
    let mut is_exact = true;
    // fusing the stream allows us to call next safely even once it is finished
    let mut all_files = Box::pin(all_files.fuse());
    while let Some(res) = all_files.next().await {
        let (file, file_stats) = res?;
        result_files.push(file);
        is_exact &= file_stats.is_exact;
        num_rows += file_stats.num_rows.unwrap_or(0);
        total_byte_size += file_stats.total_byte_size.unwrap_or(0);
        if let Some(vec) = &file_stats.column_statistics {
            has_statistics = true;
            for (i, cs) in vec.iter().enumerate() {
                null_counts[i] += cs.null_count.unwrap_or(0);

                if let Some(max_value) = &mut max_values[i] {
                    if let Some(file_max) = cs.max_value.clone() {
                        match max_value.update(&[file_max]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                }

                if let Some(min_value) = &mut min_values[i] {
                    if let Some(file_min) = cs.min_value.clone() {
                        match min_value.update(&[file_min]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                }
            }
        }
        if num_rows > limit.unwrap_or(usize::MAX) {
            break;
        }
    }
    // if we still have files in the stream, it means that the limit kicked
    // in and that the statistic could have been different if we processed
    // the files in a different order.
    if all_files.next().await.is_some() {
        is_exact = false;
    }

    let column_stats = if has_statistics {
        Some(get_col_stats(
            &*file_schema,
            null_counts,
            &mut max_values,
            &mut min_values,
        ))
    } else {
        None
    };

    let statistics = Statistics {
        num_rows: Some(num_rows as usize),
        total_byte_size: Some(total_byte_size as usize),
        column_statistics: column_stats,
        is_exact,
    };

    Ok((result_files, statistics))
}

#[derive(Debug, Clone)]
/// A single file that should be read, along with its schema, statistics
/// and partition column values that need to be appended to each row.
pub struct PartitionedFile {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub file_meta: FileMeta,
    /// Values of partition columns to be appended to each row
    pub partition_values: Vec<ScalarValue>,
    // We may include row group range here for a more fine-grained parallel execution
}

impl PartitionedFile {
    /// Create a simple file without metadata or partition
    pub fn new(path: String, size: u64) -> Self {
        Self {
            file_meta: FileMeta {
                sized_file: SizedFile { path, size },
                last_modified: None,
            },
            partition_values: vec![],
        }
    }
}

/// Stream of files get listed from object store
pub type PartitionedFileStream =
    Pin<Box<dyn Stream<Item = Result<PartitionedFile>> + Send + Sync + 'static>>;

impl std::fmt::Display for PartitionedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.file_meta)
    }
}

fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let flat_schema = flatten_schema(schema);
    let max_values: Vec<Option<MaxAccumulator>> = flat_schema
        .iter()
        .map(|field| MaxAccumulator::try_new(field.data_type()).ok())
        .collect::<Vec<_>>();
    let min_values: Vec<Option<MinAccumulator>> = flat_schema
        .iter()
        .map(|field| MinAccumulator::try_new(field.data_type()).ok())
        .collect::<Vec<_>>();
    (max_values, min_values)
}

fn get_col_stats(
    schema: &Schema,
    null_counts: Vec<usize>,
    max_values: &mut Vec<Option<MaxAccumulator>>,
    min_values: &mut Vec<Option<MinAccumulator>>,
) -> Vec<ColumnStatistics> {
    let total_num_fields = total_number_of_fields(schema);
    (0..total_num_fields)
        .map(|i| {
            let max_value = match &max_values[i] {
                Some(max_value) => max_value.evaluate().ok(),
                None => None,
            };
            let min_value = match &min_values[i] {
                Some(min_value) => min_value.evaluate().ok(),
                None => None,
            };
            ColumnStatistics {
                null_count: Some(null_counts[i] as usize),
                max_value,
                min_value,
                distinct_count: None,
            }
        })
        .collect()
}

fn total_number_of_fields(schema: &Schema) -> usize {
    fn count_children(field: &Field) -> usize {
        let mut num_children: usize = 0;
        match field.data_type() {
            DataType::Struct(fields) | DataType::Union(fields) => {
                let counts_arr = fields.iter().map(|f| count_children(f)).collect::<Vec<usize>>();
                let c: usize = counts_arr.iter().sum();
                num_children += c
            },
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => {
                let c: usize = count_children(f);
                num_children += c
            },
            _ => num_children += 1,
        }
        num_children
    }
    let top_level_fields = schema.fields();
    let top_level_counts = top_level_fields.iter().map(|i| count_children(i)).collect::<Vec<usize>>();
    top_level_counts.iter().sum()
}

fn flatten_schema(schema: &Schema) -> Vec<Field> {
    fn fetch_children(field: Field) -> Vec<Field> {
        let mut collected_fields: Vec<Field> = vec![];
        let data_type = field.data_type();
        match data_type {
            DataType::Struct(fields) | DataType::Union(fields) => collected_fields
                .extend(fields.iter().map(|f| {
                    let full_name = format!("{}.{}", field.name(), f.name());
                    let f_new = Field::new(&full_name, f.data_type().clone(), f.is_nullable());
                    fetch_children(f_new)
                }).flatten()),
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => {
                let full_name = format!("{}.{}", field.name(), f.name());
                let f_new = Field::new(&full_name, f.data_type().clone(), f.is_nullable());
                collected_fields.extend(fetch_children(f_new))
            },
            _ => collected_fields.push(field),
        }
        collected_fields
    }
    let top_level_fields = schema.fields();
    let flatten = top_level_fields
        .iter()
        .map(|f| fetch_children(f.clone()))
        .flatten()
        .collect();
    flatten
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit;

    use super::*;

    #[tokio::test]
    async fn test_total_number_of_fields() -> Result<()> {
        let fields: Vec<Field> = vec![
            Field::new("id", DataType::Int16, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("nested1", DataType::Struct(vec![
                Field::new("str1", DataType::Utf8, false),
                Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            ]), false),
            Field::new("nested2", DataType::Struct(vec![
                Field::new("nested_a", DataType::Struct(vec![
                    Field::new("another_nested", DataType::Struct(vec![
                        Field::new("idx", DataType::UInt8, false),
                        Field::new("no", DataType::UInt8, false),
                    ]), false),
                    Field::new("id2", DataType::UInt16, false),
                ]), false),
                Field::new("nested_b", DataType::Struct(vec![
                    Field::new("nested_x", DataType::Struct(vec![
                        Field::new("nested_y", DataType::Struct(vec![
                            Field::new("desc", DataType::Utf8, false),
                        ]), false),
                    ]), false),
                ]), false),
            ]), false),
        ];

        assert_eq!(8, total_number_of_fields(&Schema::new(fields)));

        Ok(())
    }

    #[tokio::test]
    async fn test_total_number_of_fields_empty_struct() -> Result<()> {

        let fields = vec![
            Field::new("empty_nested", DataType::Struct(vec![]), false),
        ];

        assert_eq!(0, total_number_of_fields(&Schema::new(fields)));

        Ok(())
    }

    #[tokio::test]
    async fn test_flatten_schema() -> Result<()> {
        let fields: Vec<Field> = vec![
            Field::new("id", DataType::Int16, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("nested1", DataType::Struct(vec![
                Field::new("str1", DataType::Utf8, false),
                Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            ]), false),
            Field::new("nested2", DataType::Struct(vec![
                Field::new("nested_a", DataType::Struct(vec![
                    Field::new("another_nested", DataType::Struct(vec![
                        Field::new("idx", DataType::UInt8, false),
                        Field::new("no", DataType::UInt8, false),
                    ]), false),
                    Field::new("id2", DataType::UInt16, false),
                ]), false),
                Field::new("nested_b", DataType::Struct(vec![
                    Field::new("nested_x", DataType::Struct(vec![
                        Field::new("nested_y", DataType::Struct(vec![
                            Field::new("desc", DataType::Utf8, false),
                        ]), false),
                    ]), false),
                ]), false),
            ]), false),
        ];

        let flat_schema = flatten_schema(&Schema::new(fields));
        assert_eq!(8, flat_schema.len());
        assert_eq!(Field::new("id", DataType::Int16, false), flat_schema[0]);
        assert_eq!(Field::new("name", DataType::Utf8, false), flat_schema[1]);
        assert_eq!(Field::new("nested1.str1", DataType::Utf8, false), flat_schema[2]);
        assert_eq!(Field::new("nested1.ts", DataType::Timestamp(TimeUnit::Millisecond, None), false), flat_schema[3]);
        assert_eq!(Field::new("nested2.nested_a.another_nested.idx", DataType::UInt8, false), flat_schema[4]);
        assert_eq!(Field::new("nested2.nested_a.another_nested.no", DataType::UInt8, false), flat_schema[5]);
        assert_eq!(Field::new("nested2.nested_a.id2", DataType::UInt16, false), flat_schema[6]);
        assert_eq!(Field::new("nested2.nested_b.nested_x.nested_y.desc", DataType::Utf8, false), flat_schema[7]);

        Ok(())
    }
    
}