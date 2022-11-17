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

#![allow(clippy::module_inception)]
pub mod datasource;
pub mod default_table_source;
pub mod empty;
pub mod file_format;
pub mod listing;
pub mod listing_table_factory;
pub mod memory;
pub mod object_store;
pub mod view;

use arrow::datatypes::{DataType, Field};
use futures::Stream;
use std::collections::HashMap;

pub use self::datasource::TableProvider;
pub use self::default_table_source::{
    provider_as_source, source_as_provider, DefaultTableSource,
};
use self::listing::PartitionedFile;
pub use self::memory::MemTable;
pub use self::view::ViewTable;
use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::config::OPT_FILE_FORMAT_COERCE_TYPES;
use crate::error::Result;
pub use crate::logical_expr::TableType;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::{Accumulator, ColumnStatistics, Statistics};
use arrow::error::ArrowError;
use datafusion_common::DataFusionError;
use datafusion_common::SchemaError::SchemaMergeError;
use futures::StreamExt;
use itertools::Itertools;

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

    let mut total_byte_size = 0;
    let mut null_counts = vec![0; file_schema.fields().len()];
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
                        match max_value.update_batch(&[file_max.to_array()]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    } else {
                        max_values[i] = None;
                    }
                }

                if let Some(min_value) = &mut min_values[i] {
                    if let Some(file_min) = cs.min_value.clone() {
                        match min_value.update_batch(&[file_min.to_array()]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    } else {
                        min_values[i] = None;
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
            &file_schema,
            null_counts,
            &mut max_values,
            &mut min_values,
        ))
    } else {
        None
    };

    let statistics = Statistics {
        num_rows: Some(num_rows),
        total_byte_size: Some(total_byte_size),
        column_statistics: column_stats,
        is_exact,
    };

    Ok((result_files, statistics))
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
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
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
                null_count: Some(null_counts[i]),
                max_value,
                min_value,
                distinct_count: None,
            }
        })
        .collect()
}

/// Specialized copy of Schema::try_merge that supports merging fields that have different,
/// but compatible, data types
pub(crate) fn try_merge_schemas(
    schemas: impl IntoIterator<Item = Schema>,
    merge_compatible_types: bool,
) -> Result<Schema> {
    let mut metadata = HashMap::new();
    let mut fields: Vec<Field> = vec![];
    for schema in schemas {
        for (key, value) in &schema.metadata {
            if let Some(old_val) = metadata.get(key) {
                if old_val != value {
                    return Err(DataFusionError::ArrowError(ArrowError::SchemaError(
                        format!(
                            "Fail to merge schema due to conflicting metadata. \
                                     Key '{}' has different values '{}' and '{}'",
                            key, old_val, value
                        ),
                    )));
                }
            }
            metadata.insert(key.to_owned(), value.to_owned());
        }
        for field in &schema.fields {
            if let Some((i, merge_field)) =
                fields.iter().find_position(|f| f.name() == field.name())
            {
                if merge_field.data_type() != field.data_type() {
                    if let Some(new_type) =
                        get_wider_type(merge_field.data_type(), field.data_type())
                    {
                        if &new_type != merge_field.data_type() {
                            if merge_compatible_types {
                                fields[i] = merge_field.clone().with_data_type(new_type);
                            } else {
                                return Err(DataFusionError::Execution(format!(
                                    "Schema merge failed due to different, but compatible, data types for field '{}' ({} vs {}). \
                                    Set '{}=true' (or call with_coerce_types(true) on reader options) \
                                    to enable merging this field",
                                    field.name(),
                                    merge_field.data_type(),
                                    field.data_type(),
                                    OPT_FILE_FORMAT_COERCE_TYPES
                                )));
                            }
                        }
                    } else {
                        return Err(DataFusionError::SchemaError(SchemaMergeError {
                            field_name: field.name().to_owned(),
                            data_types: (
                                field.data_type().clone(),
                                merge_field.data_type().clone(),
                            ),
                        }));
                    }
                }
            } else {
                // first time seeing this field
                fields.push(field.clone());
            }
        }
    }
    Ok(Schema::new_with_metadata(fields, metadata))
}

pub(crate) fn get_wider_type(t1: &DataType, t2: &DataType) -> Option<DataType> {
    fn _get_wider_type(t1: &DataType, t2: &DataType) -> Option<DataType> {
        use DataType::*;
        match (t1, t2) {
            (Null, _) => Some(t2.clone()),
            (Int8, Int16 | Int32 | Int64 | Float32 | Float64) => Some(t2.clone()),
            (Int16, Int32 | Int64 | Float32 | Float64) => Some(t2.clone()),
            (Int32, Int64 | Float32 | Float64) => Some(t2.clone()),
            (UInt8, UInt16 | UInt32 | UInt64 | Float32 | Float64) => Some(t2.clone()),
            (UInt16, UInt32 | UInt64 | Float32 | Float64) => Some(t2.clone()),
            (UInt32, UInt64 | Float32 | Float64) => Some(t2.clone()),
            (Float32, Float64) => Some(t2.clone()),
            _ => None,
        }
    }
    // try both combinations
    _get_wider_type(t1, t2).or_else(|| _get_wider_type(t2, t1))
}
