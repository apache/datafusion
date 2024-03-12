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

//! Factory for creating ListingTables with default options

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "parquet")]
use crate::datasource::file_format::parquet::ParquetFormat;
use crate::datasource::file_format::{
    arrow::ArrowFormat, avro::AvroFormat, csv::CsvFormat, json::JsonFormat, FileFormat,
};
use crate::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use crate::datasource::provider::TableProviderFactory;
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::config::TableOptions;
use datafusion_common::{arrow_datafusion_err, DataFusionError, FileType};
use datafusion_expr::CreateExternalTable;

use async_trait::async_trait;

/// A `TableProviderFactory` capable of creating new `ListingTable`s
#[derive(Debug, Default)]
pub struct ListingTableFactory {}

impl ListingTableFactory {
    /// Creates a new `ListingTableFactory`
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TableProviderFactory for ListingTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion_common::Result<Arc<dyn TableProvider>> {
        let mut table_options =
            TableOptions::default_from_session_config(state.config_options());
        let file_type = FileType::from_str(cmd.file_type.as_str()).map_err(|_| {
            DataFusionError::Execution(format!("Unknown FileType {}", cmd.file_type))
        })?;
        table_options.set_file_format(file_type.clone());
        table_options.alter_with_string_hash_map(&cmd.options)?;
        let file_extension = get_extension(cmd.location.as_str());
        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::CSV => {
                let mut csv_options = table_options.csv;
                csv_options.has_header = cmd.has_header;
                csv_options.delimiter = cmd.delimiter as u8;
                csv_options.compression = cmd.file_compression_type;
                Arc::new(CsvFormat::default().with_options(csv_options))
            }
            #[cfg(feature = "parquet")]
            FileType::PARQUET => {
                Arc::new(ParquetFormat::default().with_options(table_options.parquet))
            }
            FileType::AVRO => Arc::new(AvroFormat),
            FileType::JSON => {
                let mut json_options = table_options.json;
                json_options.compression = cmd.file_compression_type;
                Arc::new(JsonFormat::default().with_options(json_options))
            }
            FileType::ARROW => Arc::new(ArrowFormat),
        };

        let (provided_schema, table_partition_cols) = if cmd.schema.fields().is_empty() {
            (
                None,
                cmd.table_partition_cols
                    .iter()
                    .map(|x| {
                        (
                            x.clone(),
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            let schema: SchemaRef = Arc::new(cmd.schema.as_ref().to_owned().into());
            let table_partition_cols = cmd
                .table_partition_cols
                .iter()
                .map(|col| {
                    schema
                        .field_with_name(col)
                        .map_err(|e| arrow_datafusion_err!(e))
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?
                .into_iter()
                .map(|f| (f.name().to_owned(), f.data_type().to_owned()))
                .collect();
            // exclude partition columns to support creating partitioned external table
            // with a specified column definition like
            // `create external table a(c0 int, c1 int) stored as csv partitioned by (c1)...`
            let mut project_idx = Vec::new();
            for i in 0..schema.fields().len() {
                if !cmd.table_partition_cols.contains(schema.field(i).name()) {
                    project_idx.push(i);
                }
            }
            let schema = Arc::new(schema.project(&project_idx)?);
            (Some(schema), table_partition_cols)
        };

        let table_path = ListingTableUrl::parse(&cmd.location)?;

        let options = ListingOptions::new(file_format)
            .with_collect_stat(state.config().collect_statistics())
            .with_file_extension(file_extension)
            .with_target_partitions(state.config().target_partitions())
            .with_table_partition_cols(table_partition_cols)
            .with_file_sort_order(cmd.order_exprs.clone());

        let resolved_schema = match provided_schema {
            None => options.infer_schema(state, &table_path).await?,
            Some(s) => s,
        };
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?
            .with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
        let table = provider
            .with_definition(cmd.definition.clone())
            .with_constraints(cmd.constraints.clone())
            .with_column_defaults(cmd.column_defaults.clone());
        Ok(Arc::new(table))
    }
}

// Get file extension from path
fn get_extension(path: &str) -> String {
    let res = Path::new(path).extension().and_then(|ext| ext.to_str());
    match res {
        Some(ext) => format!(".{ext}"),
        None => "".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::execution::context::SessionContext;

    use datafusion_common::parsers::CompressionTypeVariant;
    use datafusion_common::{Constraints, DFSchema, OwnedTableReference};

    #[tokio::test]
    async fn test_create_using_non_std_file_ext() {
        let csv_file = tempfile::Builder::new()
            .prefix("foo")
            .suffix(".tbl")
            .tempfile()
            .unwrap();

        let factory = ListingTableFactory::new();
        let context = SessionContext::new();
        let state = context.state();
        let name = OwnedTableReference::bare("foo".to_string());
        let cmd = CreateExternalTable {
            name,
            location: csv_file.path().to_str().unwrap().to_string(),
            file_type: "csv".to_string(),
            has_header: true,
            delimiter: ',',
            schema: Arc::new(DFSchema::empty()),
            table_partition_cols: vec![],
            if_not_exists: false,
            file_compression_type: CompressionTypeVariant::UNCOMPRESSED,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
        };
        let table_provider = factory.create(&state, &cmd).await.unwrap();
        let listing_table = table_provider
            .as_any()
            .downcast_ref::<ListingTable>()
            .unwrap();
        let listing_options = listing_table.options();
        assert_eq!(".tbl", listing_options.file_extension);
    }

    #[tokio::test]
    async fn test_create_using_non_std_file_ext_csv_options() {
        let csv_file = tempfile::Builder::new()
            .prefix("foo")
            .suffix(".tbl")
            .tempfile()
            .unwrap();

        let factory = ListingTableFactory::new();
        let context = SessionContext::new();
        let state = context.state();
        let name = OwnedTableReference::bare("foo".to_string());

        let mut options = HashMap::new();
        options.insert("csv.schema_infer_max_rec".to_owned(), "1000".to_owned());
        let cmd = CreateExternalTable {
            name,
            location: csv_file.path().to_str().unwrap().to_string(),
            file_type: "csv".to_string(),
            has_header: true,
            delimiter: ',',
            schema: Arc::new(DFSchema::empty()),
            table_partition_cols: vec![],
            if_not_exists: false,
            file_compression_type: CompressionTypeVariant::UNCOMPRESSED,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
        };
        let table_provider = factory.create(&state, &cmd).await.unwrap();
        let listing_table = table_provider
            .as_any()
            .downcast_ref::<ListingTable>()
            .unwrap();

        let format = listing_table.options().format.clone();
        let csv_format = format.as_any().downcast_ref::<CsvFormat>().unwrap();
        let csv_options = csv_format.options().clone();
        assert_eq!(csv_options.schema_infer_max_rec, 1000);
        let listing_options = listing_table.options();
        assert_eq!(".tbl", listing_options.file_extension);
    }
}
