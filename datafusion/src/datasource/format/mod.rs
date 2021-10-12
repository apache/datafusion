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

//! Module containing helper methods for the various file formats

pub mod csv;
pub mod json;
pub mod parquet;

use std::pin::Pin;
use std::sync::Arc;

use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::{Accumulator, ColumnStatistics, ExecutionPlan, Statistics};

use super::PartitionedFile;

use async_trait::async_trait;
use futures::Stream;

/// A stream of String that can be used accross await calls
pub type StringStream = Pin<Box<dyn Stream<Item = String> + Send + Sync>>;

/// Convert a vec into a `StringStream`
pub fn string_stream(strings: Vec<String>) -> StringStream {
    Box::pin(futures::stream::iter(strings))
}

/// This trait abstracts all the file format specific implementations
/// from the `TableProvider`. This helps code re-utilization accross
/// providers that support the the same file formats.
#[async_trait]
pub trait FileFormat: Send + Sync {
    /// Open the files at the paths provided by iterator and infer the
    /// common schema
    async fn infer_schema(&self, paths: StringStream) -> Result<SchemaRef>;

    /// Open the file at the given path and infer its statistics
    async fn infer_stats(&self, path: &str) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    /// TODO group params into TableDescription(schema,files,stats) and
    /// ScanOptions(projection,batch_size,filters) to avoid too_many_arguments
    #[allow(clippy::too_many_arguments)]
    async fn create_executor(
        &self,
        schema: SchemaRef,
        files: Vec<Vec<PartitionedFile>>,
        statistics: Statistics,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Get all files as well as the summary statistic
/// if the optional `limit` is provided, includes only sufficient files
/// needed to read up to `limit` number of rows
/// TODO fix case where `num_rows` and `total_byte_size` are not defined (stat should be None instead of Some(0))
pub fn get_statistics_with_limit(
    all_files: &[PartitionedFile],
    schema: SchemaRef,
    limit: Option<usize>,
) -> (Vec<PartitionedFile>, Statistics) {
    let mut all_files = all_files.to_vec();

    let mut total_byte_size = 0;
    let mut null_counts = vec![0; schema.fields().len()];
    let mut has_statistics = false;
    let (mut max_values, mut min_values) = create_max_min_accs(&schema);

    let mut num_rows = 0;
    let mut num_files = 0;
    let mut is_exact = true;
    for file in &all_files {
        num_files += 1;
        let file_stats = &file.statistics;
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
    if num_files < all_files.len() {
        is_exact = false;
        all_files.truncate(num_files);
    }

    let column_stats = if has_statistics {
        Some(get_col_stats(
            &*schema,
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
    (all_files, statistics)
}

fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let max_values: Vec<Option<MaxAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| MaxAccumulator::try_new(field.data_type()).ok())
        .collect::<Vec<_>>();
    let min_values: Vec<Option<MinAccumulator>> = schema
        .fields()
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
    (0..schema.fields().len())
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
