//
//
// hybrid_table.rs
// Copyright (C) 2022 Author zombie <zombie@zombie-ub2104>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use super::{
    listing::{helpers, ListingOptions, ListingTableConfig},
    object_store::ObjectStore,
    PartitionedFile, TableProvider,
};
use crate::error::{DataFusionError, Result};
use crate::execution::runtime_env::RuntimeEnv;
use crate::logical_plan::{combine_filters, Expr};
use crate::physical_plan::file_format::{
    FileScanConfig, DEFAULT_PARTITION_COLUMN_DATATYPE,
};
use crate::physical_plan::hybrid::HybridExec;
use crate::physical_plan::{ExecutionPlan, Statistics};
use arrow::array::TimestampSecondArray;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{
    stream::{self},
    StreamExt, TryStreamExt,
};
use std::sync::Arc;
use std::{any::Any, convert::TryInto};

pub struct HybridTable {
    schema: SchemaRef,
    // in-memory table data
    batches: Vec<Vec<RecordBatch>>,
    // on-disk table data  with parquet format
    object_store: Arc<dyn ObjectStore>,
    // table path
    table_path: String,
    // File fields only
    file_schema: SchemaRef,
    // File fields + partition columns
    table_schema: SchemaRef,
    options: ListingOptions,
}

impl HybridTable {
    pub fn try_new(
        config: ListingTableConfig,
        schema: SchemaRef,
        batches: &[Vec<RecordBatch>],
    ) -> Result<Self> {
        let file_schema = config
            .file_schema
            .ok_or_else(|| DataFusionError::Internal("No schema provided.".into()))?;
        let options = config.options.ok_or_else(|| {
            DataFusionError::Internal("No ListingOptions provided".into())
        })?;
        // Add the partition columns to the file schema
        let mut table_fields = file_schema.fields().clone();
        for part in &options.table_partition_cols {
            table_fields.push(Field::new(
                part,
                DEFAULT_PARTITION_COLUMN_DATATYPE.clone(),
                false,
            ));
        }
        Ok(Self {
            schema,
            batches: batches.to_vec(),
            object_store: config.object_store.clone(),
            table_path: config.table_path.clone(),
            file_schema,
            table_schema: Arc::new(Schema::new(table_fields)),
            options,
        })
    }

    async fn list_files_for_scan<'a>(
        &'a self,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        // list files (with partitions)
        let file_list = helpers::pruned_partition_list(
            self.object_store.as_ref(),
            &self.table_path,
            filters,
            &self.options.file_extension,
            &self.options.table_partition_cols,
        )
        .await?;

        // collect the statistics if required by the config
        let object_store = Arc::clone(&self.object_store);
        let files = file_list.then(move |part_file| {
            let object_store = object_store.clone();
            async move {
                let part_file = part_file?;
                let statistics = if self.options.collect_stat {
                    let object_reader = object_store
                        .file_reader(part_file.file_meta.sized_file.clone())?;
                    self.options.format.infer_stats(object_reader).await?
                } else {
                    Statistics::default()
                };
                Ok((part_file, statistics)) as Result<(PartitionedFile, Statistics)>
            }
        });

        let (files, statistics) =
            super::get_statistics_with_limit(files, self.schema(), limit).await?;

        Ok((
            helpers::split_files(files, self.options.target_partitions),
            statistics,
        ))
    }
}

#[async_trait]
impl TableProvider for HybridTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (partitioned_file_lists, _) =
            self.list_files_for_scan(filters, limit).await?;
        let predicate = combine_filters(filters);
        let fconfig = FileScanConfig {
            object_store: Arc::clone(&self.object_store),
            file_schema: Arc::clone(&self.file_schema),
            file_groups: partitioned_file_lists,
            statistics: Statistics::default(),
            projection: projection.clone(),
            limit,
            table_partition_cols: self.options.table_partition_cols.clone(),
        };
        let he = HybridExec::try_new(&self.batches.clone(), fconfig, predicate)?;
        Ok(Arc::new(he))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::ExecutionContext;
    use crate::prelude::ExecutionConfig;
    use crate::{
        datasource::{
            file_format::{avro::AvroFormat, parquet::ParquetFormat},
            object_store::local::LocalFileSystem,
            TableProvider,
        },
        logical_plan::{col, lit},
        test::{columns, object_store::TestObjectStore},
    };
    use parquet::arrow::ArrowReader;
    use parquet::arrow::ParquetFileArrowReader;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;
    use std::path::PathBuf;
    #[tokio::test]
    async fn test_hybrid_exec() -> Result<()> {
        let path = String::from(
            "/home/zombie/OpenSource/arrow-datafusion/datafusion/tests/cpu_usage/part-00016-92d3d3b0-90a1-48f7-8955-6b25fe2c1311-c000.gz.parquet",
        );
        let parquet_file_reader = get_test_reader(&path);
        let mut arrow_reader = ParquetFileArrowReader::new(parquet_file_reader);
        let schema = Arc::new(arrow_reader.get_schema().unwrap());
        let record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");
        let batches = vec![record_batch_reader
            .collect::<ArrowResult<Vec<RecordBatch>>>()
            .unwrap()];
        assert_eq!(batches.len(), 1);
        let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let mut ctx = ExecutionContext::new();
        ctx.register_hybrid_table("cpu_usage", &path, opt, &batches, Some(schema.clone())).await?;
        let df = ctx.sql("SELECT * FROM cpu_usage limit 1;").await?;
        df.show().await?;
        Ok(())
    }

    fn get_test_reader(path: &str) -> Arc<dyn FileReader> {
        let file = get_test_file(path);
        let reader =
            SerializedFileReader::new(file).expect("Failed to create serialized reader");
        Arc::new(reader)
    }

    fn get_test_file(file_name: &str) -> File {
        let mut path = PathBuf::new();
        path.push(file_name);
        File::open(path.as_path()).expect("File not found!")
    }
}
