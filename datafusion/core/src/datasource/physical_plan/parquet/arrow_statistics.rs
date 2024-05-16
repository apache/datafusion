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

//! Extract parquet statistics and convert it to Arrow statistics

use std::{fs::File, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, UInt64Array};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    file::statistics::{Statistics as ParquetStatistics, ValueStatistics},
};

use super::statistics::parquet_column;

/// statistics extracted from `Statistics` as Arrow `ArrayRef`s
///
/// # Note:
/// If the corresponding `Statistics` is not present, or has no information for
/// a column, a NULL is present in the  corresponding array entry
#[derive(Debug)]
pub struct ArrowStatistics {
    /// min values
    min: ArrayRef,
    /// max values
    max: ArrayRef,
    /// Row counts (UInt64Array)
    row_count: ArrayRef,
    /// Null Counts (UInt64Array)
    null_count: ArrayRef,
}

impl ArrowStatistics {
    /// Create a new instance of `ArrowStatistics`
    pub fn new(
        min: ArrayRef,
        max: ArrayRef,
        row_count: ArrayRef,
        null_count: ArrayRef,
    ) -> Self {
        Self {
            min,
            max,
            row_count,
            null_count,
        }
    }

    /// Get the min values
    pub fn min(&self) -> &ArrayRef {
        &self.min
    }

    /// Get the max values
    pub fn max(&self) -> &ArrayRef {
        &self.max
    }

    /// Get the row counts
    pub fn row_count(&self) -> &ArrayRef {
        &self.row_count
    }

    /// Get the null counts
    pub fn null_count(&self) -> &ArrayRef {
        &self.null_count
    }
}

/// Extract `ArrowStatistics` from the  parquet [`Statistics`]
pub fn parquet_stats_to_arrow(
    arrow_datatype: &DataType,
    statistics: &ParquetColumnStatistics,
) -> Result<ArrowStatistics> {
    // check of the data type is Int64
    if !matches!(arrow_datatype, DataType::Int64) {
        return Err(DataFusionError::Internal(format!(
            "Unsupported data type {:?} for statistics",
            arrow_datatype
        )));
    }

    // row counts
    let row_count = statistics
        .rg_statistics
        .iter()
        .map(|rg| rg.row_count)
        .collect::<Vec<_>>();

    // get null counts
    let parquet_stats = statistics.rg_statistics.iter().map(|rg| rg.statistics);
    let null_counts = parquet_stats
        // .map(|stats| stats.and_then(|s| Some(s.null_count())))
        .map(|stats| stats.map(|s| s.null_count()))
        .collect::<Vec<_>>();

    // get min and max values
    let parquet_stats_with_min_max = statistics
        .rg_statistics
        .iter()
        .map(|rg| rg.get_statistics())
        .collect::<Vec<_>>();

    let mins = parquet_stats_with_min_max
        .iter()
        .map(|stats| {
            stats.and_then(|s| {
                let stats = ParquetColumnRowGroupStatistics::try_as_i64(s)?;
                Some(*stats.min())
            })
        })
        .collect::<Vec<_>>();

    let maxs = parquet_stats_with_min_max
        .iter()
        .map(|stats| {
            stats.and_then(|s| {
                let stats = ParquetColumnRowGroupStatistics::try_as_i64(s)?;
                Some(*stats.max())
            })
        })
        .collect::<Vec<_>>();

    Ok(ArrowStatistics {
        min: Arc::new(Int64Array::from(mins)),
        max: Arc::new(Int64Array::from(maxs)),
        row_count: Arc::new(UInt64Array::from(row_count)),
        null_count: Arc::new(UInt64Array::from(null_counts)),
    })
}

/// All row group statistics of a file for a column
pub struct ParquetColumnStatistics<'a> {
    // todo: do we need this?
    // arrow column schema
    // column_schema: &'a FieldRef,
    _column_name: &'a str, // todo: do we need this?
    rg_statistics: Vec<ParquetColumnRowGroupStatistics<'a>>,
}

/// Row group statistics of a column
pub struct ParquetColumnRowGroupStatistics<'a> {
    row_count: u64,
    statistics: Option<&'a ParquetStatistics>,
}

impl<'a> ParquetColumnRowGroupStatistics<'a> {
    /// Create a new instance of `ParquetColumnRowGroupStatistics`
    pub fn new(row_count: u64, statistics: Option<&'a ParquetStatistics>) -> Self {
        Self {
            row_count,
            statistics,
        }
    }

    /// Return statistics if it exists and has min max
    /// Otherwise return None
    pub fn get_statistics(&self) -> Option<&'a ParquetStatistics> {
        let stats = self.statistics?;
        if stats.has_min_max_set() {
            Some(stats)
        } else {
            None
        }
    }

    /// Return the statistics as ValuesStatistcs<i64> if the column is i64
    /// Otherwise return None
    fn try_as_i64(stats: &'a ParquetStatistics) -> Option<&'a ValueStatistics<i64>> {
        if let parquet::file::statistics::Statistics::Int64(statistics) = stats {
            Some(statistics)
        } else {
            None
        }
    }
}

impl<'a> ParquetColumnStatistics<'a> {
    /// Create a new instance of `ParquetColumnStatistics`
    pub fn new(
        _column_name: &'a str,
        rg_statistics: Vec<ParquetColumnRowGroupStatistics<'a>>,
    ) -> Self {
        Self {
            _column_name,
            rg_statistics,
        }
    }

    /// Extract statistics of all columns from a parquet file metadata
    pub fn from_parquet_statistics(
        reader: &'a ParquetRecordBatchReaderBuilder<File>,
    ) -> Result<Vec<Self>> {
        // Get metadata & schemas
        let metadata = reader.metadata();
        let parquet_schema = reader.parquet_schema();
        let arrow_schema = reader.schema();

        // Get colum names from  arrow schema & its index in the parquet schema
        let columns = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let col_name = f.name();
                let col_idx = parquet_column(parquet_schema, arrow_schema, col_name);
                match col_idx {
                    Some(idx) => Ok((col_name, idx)),
                    None => Err(DataFusionError::Internal(format!(
                        "Column {} in Arrow schema not found in Parquet schema",
                        col_name,
                    ))),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Get statistics for each column
        let col_stats = columns
            .iter()
            .map(|(col_name, col_idx)| {
                let rg_statistics = metadata
                    .row_groups()
                    .iter()
                    .map(|rg_meta| {
                        let row_count = rg_meta.num_rows() as u64;
                        let statistics = rg_meta.column(col_idx.0).statistics();
                        ParquetColumnRowGroupStatistics::new(row_count, statistics)
                    })
                    .collect::<Vec<_>>();

                ParquetColumnStatistics::new(col_name, rg_statistics)
            })
            .collect::<Vec<_>>();

        Ok(col_stats)
    }
}
