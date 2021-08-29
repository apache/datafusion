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

//! Parquet data source

use std::any::Any;
use std::fs::File;
use std::sync::Arc;

use parquet::arrow::ArrowReader;
use parquet::arrow::ParquetFileArrowReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::statistics::Statistics as ParquetStatistics;

use super::datasource::TableProviderFilterPushDown;
use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use crate::datasource::datasource::Statistics;
use crate::datasource::{
    create_max_min_accs, get_col_stats, get_statistics_with_limit, FileAndSchema,
    PartitionedFile, TableDescriptor, TableDescriptorBuilder, TableProvider,
};
use crate::error::Result;
use crate::logical_plan::{combine_filters, Expr};
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::parquet::ParquetExec;
use crate::physical_plan::{Accumulator, ExecutionPlan};
use crate::scalar::ScalarValue;

/// Table-based representation of a `ParquetFile`.
pub struct ParquetTable {
    /// Descriptor of the table, including schema, files, etc.
    pub desc: Arc<ParquetTableDescriptor>,
    max_partitions: usize,
    enable_pruning: bool,
}

impl ParquetTable {
    /// Attempt to initialize a new `ParquetTable` from a file path.
    pub fn try_new(path: impl Into<String>, max_partitions: usize) -> Result<Self> {
        let path = path.into();
        let table_desc = ParquetTableDescriptor::new(path.as_str());
        Ok(Self {
            desc: Arc::new(table_desc?),
            max_partitions,
            enable_pruning: true,
        })
    }

    /// Attempt to initialize a new `ParquetTable` from a file path and known schema.
    /// If collect_statistics is `false`, doesn't read files until necessary by scan
    pub fn try_new_with_schema(
        path: impl Into<String>,
        schema: Schema,
        max_partitions: usize,
        collect_statistics: bool,
    ) -> Result<Self> {
        let path = path.into();
        let table_desc = ParquetTableDescriptor::new_with_schema(
            path.as_str(),
            Some(schema),
            collect_statistics,
        );
        Ok(Self {
            desc: Arc::new(table_desc?),
            max_partitions,
            enable_pruning: true,
        })
    }

    /// Attempt to initialize a new `ParquetTable` from a table descriptor.
    pub fn try_new_with_desc(
        desc: Arc<ParquetTableDescriptor>,
        max_partitions: usize,
        enable_pruning: bool,
    ) -> Result<Self> {
        Ok(Self {
            desc,
            max_partitions,
            enable_pruning,
        })
    }

    /// Get the path for the Parquet file(s) represented by this ParquetTable instance
    pub fn path(&self) -> &str {
        &self.desc.descriptor.path
    }

    /// Get parquet pruning option
    pub fn get_enable_pruning(&self) -> bool {
        self.enable_pruning
    }

    /// Set parquet pruning option
    pub fn with_enable_pruning(mut self, enable_pruning: bool) -> Self {
        self.enable_pruning = enable_pruning;
        self
    }
}

impl TableProvider for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this parquet file.
    fn schema(&self) -> SchemaRef {
        self.desc.schema()
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    /// Scan the file(s), using the provided projection, and return one BatchIterator per
    /// partition.
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        let predicate = if self.enable_pruning {
            combine_filters(filters)
        } else {
            None
        };
        Ok(Arc::new(ParquetExec::try_new(
            self.desc.clone(),
            projection.clone(),
            predicate,
            limit
                .map(|l| std::cmp::min(l, batch_size))
                .unwrap_or(batch_size),
            self.max_partitions,
            limit,
        )?))
    }

    fn statistics(&self) -> Statistics {
        self.desc.statistics()
    }

    fn has_exact_statistics(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
/// Descriptor for a parquet root path
pub struct ParquetTableDescriptor {
    /// metadata for files inside the root path
    pub descriptor: TableDescriptor,
}

impl ParquetTableDescriptor {
    /// Construct a new parquet descriptor for a root path
    pub fn new(root_path: &str) -> Result<Self> {
        let table_desc = Self::build_table_desc(root_path, "parquet", None, true);
        Ok(Self {
            descriptor: table_desc?,
        })
    }

    /// Construct a new parquet descriptor for a root path with known schema
    pub fn new_with_schema(
        root_path: &str,
        schema: Option<Schema>,
        collect_statistics: bool,
    ) -> Result<Self> {
        let table_desc =
            Self::build_table_desc(root_path, "parquet", schema, collect_statistics);
        Ok(Self {
            descriptor: table_desc?,
        })
    }

    /// Get file schema for all parquet files
    pub fn schema(&self) -> SchemaRef {
        self.descriptor.schema.clone()
    }

    /// Get the summary statistics for all parquet files
    pub fn statistics(&self) -> Statistics {
        get_statistics_with_limit(&self.descriptor, None).1
    }

    fn summarize_min_max(
        max_values: &mut Vec<Option<MaxAccumulator>>,
        min_values: &mut Vec<Option<MinAccumulator>>,
        fields: &[Field],
        i: usize,
        stat: &ParquetStatistics,
    ) {
        match stat {
            ParquetStatistics::Boolean(s) => {
                if let DataType::Boolean = fields[i].data_type() {
                    if s.has_min_max_set() {
                        if let Some(max_value) = &mut max_values[i] {
                            match max_value
                                .update(&[ScalarValue::Boolean(Some(*s.max()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    max_values[i] = None;
                                }
                            }
                        }
                        if let Some(min_value) = &mut min_values[i] {
                            match min_value
                                .update(&[ScalarValue::Boolean(Some(*s.min()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    min_values[i] = None;
                                }
                            }
                        }
                    }
                }
            }
            ParquetStatistics::Int32(s) => {
                if let DataType::Int32 = fields[i].data_type() {
                    if s.has_min_max_set() {
                        if let Some(max_value) = &mut max_values[i] {
                            match max_value.update(&[ScalarValue::Int32(Some(*s.max()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    max_values[i] = None;
                                }
                            }
                        }
                        if let Some(min_value) = &mut min_values[i] {
                            match min_value.update(&[ScalarValue::Int32(Some(*s.min()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    min_values[i] = None;
                                }
                            }
                        }
                    }
                }
            }
            ParquetStatistics::Int64(s) => {
                if let DataType::Int64 = fields[i].data_type() {
                    if s.has_min_max_set() {
                        if let Some(max_value) = &mut max_values[i] {
                            match max_value.update(&[ScalarValue::Int64(Some(*s.max()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    max_values[i] = None;
                                }
                            }
                        }
                        if let Some(min_value) = &mut min_values[i] {
                            match min_value.update(&[ScalarValue::Int64(Some(*s.min()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    min_values[i] = None;
                                }
                            }
                        }
                    }
                }
            }
            ParquetStatistics::Float(s) => {
                if let DataType::Float32 = fields[i].data_type() {
                    if s.has_min_max_set() {
                        if let Some(max_value) = &mut max_values[i] {
                            match max_value
                                .update(&[ScalarValue::Float32(Some(*s.max()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    max_values[i] = None;
                                }
                            }
                        }
                        if let Some(min_value) = &mut min_values[i] {
                            match min_value
                                .update(&[ScalarValue::Float32(Some(*s.min()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    min_values[i] = None;
                                }
                            }
                        }
                    }
                }
            }
            ParquetStatistics::Double(s) => {
                if let DataType::Float64 = fields[i].data_type() {
                    if s.has_min_max_set() {
                        if let Some(max_value) = &mut max_values[i] {
                            match max_value
                                .update(&[ScalarValue::Float64(Some(*s.max()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    max_values[i] = None;
                                }
                            }
                        }
                        if let Some(min_value) = &mut min_values[i] {
                            match min_value
                                .update(&[ScalarValue::Float64(Some(*s.min()))])
                            {
                                Ok(_) => {}
                                Err(_) => {
                                    min_values[i] = None;
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

impl TableDescriptorBuilder for ParquetTableDescriptor {
    fn file_meta(path: &str) -> Result<FileAndSchema> {
        let file = File::open(path)?;
        let file_reader = Arc::new(SerializedFileReader::new(file)?);
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let path = path.to_string();
        let schema = arrow_reader.get_schema()?;
        let num_fields = schema.fields().len();
        let fields = schema.fields().to_vec();
        let meta_data = arrow_reader.get_metadata();

        let mut num_rows = 0;
        let mut total_byte_size = 0;
        let mut null_counts = vec![0; num_fields];
        let mut has_statistics = false;

        let (mut max_values, mut min_values) = create_max_min_accs(&schema);

        for row_group_meta in meta_data.row_groups() {
            num_rows += row_group_meta.num_rows();
            total_byte_size += row_group_meta.total_byte_size();

            let columns_null_counts = row_group_meta
                .columns()
                .iter()
                .flat_map(|c| c.statistics().map(|stats| stats.null_count()));

            for (i, cnt) in columns_null_counts.enumerate() {
                null_counts[i] += cnt as usize
            }

            for (i, column) in row_group_meta.columns().iter().enumerate() {
                if let Some(stat) = column.statistics() {
                    has_statistics = true;
                    ParquetTableDescriptor::summarize_min_max(
                        &mut max_values,
                        &mut min_values,
                        &fields,
                        i,
                        stat,
                    )
                }
            }
        }

        let column_stats = if has_statistics {
            Some(get_col_stats(
                &schema,
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
        };

        Ok(FileAndSchema {
            file: PartitionedFile { path, statistics },
            schema,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        TimestampNanosecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = None;
        let exec = table.scan(&projection, 2, &[], None)?;
        let stream = exec.execute(0).await?;

        let _ = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        // test metadata
        assert_eq!(table.statistics().num_rows, Some(8));
        assert_eq!(table.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;

        let x: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(
            "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Binary\n\
             string_col: Binary\n\
             timestamp_col: Timestamp(Nanosecond, None)",
            y
        );

        let projection = None;
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![1]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![0]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![10]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![6]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![7]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![9]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<&str> = vec![];
        for i in 0..batch.num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );

        Ok(())
    }

    fn load_table(name: &str) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, name);
        let table = ParquetTable::try_new(&filename, 2)?;
        Ok(Arc::new(table))
    }

    async fn get_first_batch(
        table: Arc<dyn TableProvider>,
        projection: &Option<Vec<usize>>,
    ) -> Result<RecordBatch> {
        let exec = table.scan(projection, 1024, &[], None)?;
        let mut it = exec.execute(0).await?;
        it.next()
            .await
            .expect("should have received at least one batch")
            .map_err(|e| e.into())
    }

    #[test]
    fn combine_zero_filters() {
        let result = combine_filters(&[]);
        assert_eq!(result, None);
    }

    #[test]
    fn combine_one_filter() {
        use crate::logical_plan::{binary_expr, col, lit, Operator};
        let filter = binary_expr(col("c1"), Operator::Lt, lit(1));
        let result = combine_filters(&[filter.clone()]);
        assert_eq!(result, Some(filter));
    }

    #[test]
    fn combine_multiple_filters() {
        use crate::logical_plan::{and, binary_expr, col, lit, Operator};
        let filter1 = binary_expr(col("c1"), Operator::Lt, lit(1));
        let filter2 = binary_expr(col("c2"), Operator::Lt, lit(2));
        let filter3 = binary_expr(col("c3"), Operator::Lt, lit(3));
        let result =
            combine_filters(&[filter1.clone(), filter2.clone(), filter3.clone()]);
        assert_eq!(result, Some(and(and(filter1, filter2), filter3)));
    }
}
