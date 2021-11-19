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

//! Execution plan for reading CSV files

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

use arrow::csv;
use arrow::datatypes::SchemaRef;
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use super::file_stream::{BatchIter, FileStream};
use super::PhysicalPlanConfig;

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    base_config: PhysicalPlanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    has_header: bool,
    delimiter: u8,
}

impl CsvExec {
    /// Create a new CSV reader execution plan provided base and specific configurations
    pub fn new(base_config: PhysicalPlanConfig, has_header: bool, delimiter: u8) -> Self {
        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            has_header,
            delimiter,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &PhysicalPlanConfig {
        &self.base_config
    }
    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.has_header
    }
    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }
}

#[async_trait]
impl ExecutionPlan for CsvExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let batch_size = self.base_config.batch_size;
        let file_schema = Arc::clone(&self.base_config.file_schema);
        let file_projection = self.base_config.file_column_projection_indices();
        let has_header = self.has_header;
        let delimiter = self.delimiter;
        let start_line = if has_header { 1 } else { 0 };

        let fun = move |file, remaining: &Option<usize>| {
            let bounds = remaining.map(|x| (0, x + start_line));
            Box::new(csv::Reader::new(
                file,
                Arc::clone(&file_schema),
                has_header,
                Some(delimiter),
                batch_size,
                bounds,
                file_projection.clone(),
            )) as BatchIter
        };

        Ok(Box::pin(FileStream::new(
            Arc::clone(&self.base_config.object_store),
            self.base_config.file_groups[partition].clone(),
            fun,
            Arc::clone(&self.projected_schema),
            self.base_config.limit,
            self.base_config.table_partition_cols.clone(),
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "CsvExec: files={}, has_header={}, batch_size={}, limit={:?}",
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                    self.has_header,
                    self.base_config.batch_size,
                    self.base_config.limit,
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        datasource::object_store::local::{local_unpartitioned_file, LocalFileSystem},
        scalar::ScalarValue,
        test_util::aggr_test_schema,
    };
    use futures::StreamExt;

    #[tokio::test]
    async fn csv_exec_with_projection() -> Result<()> {
        let file_schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema,
                file_groups: vec![vec![local_unpartitioned_file(path)]],
                statistics: Statistics::default(),
                projection: Some(vec![0, 2, 4]),
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0).await?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = vec![
            "+----+-----+------------+",
            "| c1 | c3  | c5         |",
            "+----+-----+------------+",
            "| c  | 1   | 2033001162 |",
            "| d  | -40 | 706441268  |",
            "| b  | 29  | 994303988  |",
            "| a  | -85 | 1171968280 |",
            "| b  | -82 | 1824882165 |",
            "+----+-----+------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }

    #[tokio::test]
    async fn csv_exec_with_limit() -> Result<()> {
        let file_schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema,
                file_groups: vec![vec![local_unpartitioned_file(path)]],
                statistics: Statistics::default(),
                projection: None,
                batch_size: 1024,
                limit: Some(5),
                table_partition_cols: vec![],
            },
            true,
            b',',
        );
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(13, csv.projected_schema.fields().len());
        assert_eq!(13, csv.schema().fields().len());

        let mut it = csv.execute(0).await?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        assert_eq!(5, batch.num_rows());

        let expected = vec![
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c1 | c2 | c3  | c4     | c5         | c6                   | c7  | c8    | c9         | c10                  | c11         | c12                 | c13                            |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c  | 2  | 1   | 18109  | 2033001162 | -6513304855495910254 | 25  | 43062 | 1491205016 | 5863949479783605708  | 0.110830784 | 0.9294097332465232  | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |",
            "| d  | 5  | -40 | 22614  | 706441268  | -7542719935673075327 | 155 | 14337 | 3373581039 | 11720144131976083864 | 0.69632107  | 0.3114712539863804  | C2GT5KVyOPZpgKVl110TyZO0NcJ434 |",
            "| b  | 1  | 29  | -18218 | 994303988  | 5983957848665088916  | 204 | 9489  | 3275293996 | 14857091259186476033 | 0.53840446  | 0.17909035118828576 | AyYVExXK6AR2qUTxNZ7qRHQOVGMLcz |",
            "| a  | 1  | -85 | -15154 | 1171968280 | 1919439543497968449  | 77  | 52286 | 774637006  | 12101411955859039553 | 0.12285209  | 0.6864391962767343  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB |",
            "| b  | 5  | -82 | 22080  | 1824882165 | 7373730676428214987  | 208 | 34331 | 3342719438 | 3330177516592499461  | 0.82634634  | 0.40975383525297016 | Ig1QcuKsjHXkproePdERo2w0mYzIqd |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);

        Ok(())
    }

    #[tokio::test]
    async fn csv_exec_with_partition() -> Result<()> {
        let file_schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        // we don't have `/date=xx/` in the path but that is ok because
        // partitions are resolved during scan anyway
        let path = format!("{}/csv/{}", testdata, filename);
        let mut partitioned_file = local_unpartitioned_file(path);
        partitioned_file.partition_values =
            vec![ScalarValue::Utf8(Some("2021-10-26".to_owned()))];
        let csv = CsvExec::new(
            PhysicalPlanConfig {
                // we should be able to project on the partition column
                // wich is supposed to be after the file fields
                projection: Some(vec![0, file_schema.fields().len()]),
                object_store: Arc::new(LocalFileSystem {}),
                file_schema,
                file_groups: vec![vec![partitioned_file]],
                statistics: Statistics::default(),
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec!["date".to_owned()],
            },
            true,
            b',',
        );
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(2, csv.projected_schema.fields().len());
        assert_eq!(2, csv.schema().fields().len());

        let mut it = csv.execute(0).await?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(2, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = vec![
            "+----+------------+",
            "| c1 | date       |",
            "+----+------------+",
            "| c  | 2021-10-26 |",
            "| d  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "| a  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "+----+------------+",
        ];
        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }
}
