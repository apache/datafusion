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

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::catalog::{TableProvider, TableProviderFactory};
use crate::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use crate::execution::context::SessionState;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::{arrow_datafusion_err, plan_err, DataFusionError, ToDFSchema};
use datafusion_common::{config_datafusion_err, Result};
use datafusion_expr::CreateExternalTable;

use async_trait::async_trait;
use datafusion_catalog::Session;

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
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        // TODO (https://github.com/apache/datafusion/issues/11600) remove downcast_ref from here. Should file format factory be an extension to session state?
        let session_state = state.as_any().downcast_ref::<SessionState>().unwrap();
        let file_format = session_state
            .get_file_format_factory(cmd.file_type.as_str())
            .ok_or(config_datafusion_err!(
                "Unable to create table with format {}! Could not find FileFormat.",
                cmd.file_type
            ))?
            .create(session_state, &cmd.options)?;

        let file_extension = get_extension(cmd.location.as_str());

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
            .with_table_partition_cols(table_partition_cols);

        options
            .validate_partitions(session_state, &table_path)
            .await?;

        let resolved_schema = match provided_schema {
            // We will need to check the table columns against the schema
            // this is done so that we can do an ORDER BY for external table creation
            // specifically for parquet file format.
            // See: https://github.com/apache/datafusion/issues/7317
            None => {
                let schema = options.infer_schema(session_state, &table_path).await?;
                let df_schema = schema.clone().to_dfschema()?;
                let column_refs: HashSet<_> = cmd
                    .order_exprs
                    .iter()
                    .flat_map(|sort| sort.iter())
                    .flat_map(|s| s.expr.column_refs())
                    .collect();

                for column in &column_refs {
                    if !df_schema.has_column(column) {
                        return plan_err!("Column {column} is not in schema");
                    }
                }

                schema
            }
            Some(s) => s,
        };
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options.with_file_sort_order(cmd.order_exprs.clone()))
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
    use crate::{
        datasource::file_format::csv::CsvFormat, execution::context::SessionContext,
    };

    use datafusion_common::{Constraints, DFSchema, TableReference};

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
        let name = TableReference::bare("foo");
        let cmd = CreateExternalTable {
            name,
            location: csv_file.path().to_str().unwrap().to_string(),
            file_type: "csv".to_string(),
            schema: Arc::new(DFSchema::empty()),
            table_partition_cols: vec![],
            if_not_exists: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::from([("format.has_header".into(), "true".into())]),
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
        let name = TableReference::bare("foo");

        let mut options = HashMap::new();
        options.insert("format.schema_infer_max_rec".to_owned(), "1000".to_owned());
        options.insert("format.has_header".into(), "true".into());
        let cmd = CreateExternalTable {
            name,
            location: csv_file.path().to_str().unwrap().to_string(),
            file_type: "csv".to_string(),
            schema: Arc::new(DFSchema::empty()),
            table_partition_cols: vec![],
            if_not_exists: false,
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
