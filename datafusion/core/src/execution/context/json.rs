// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::super::options::ReadOptions;
use super::{DataFilePaths, DataFrame, ExecutionPlan, Result, SessionContext};
use crate::execution::options::JsonReadOptions;
use datafusion_common::TableReference;
use datafusion_datasource_json::source::plan_to_json;
use std::sync::Arc;

impl SessionContext {
    /// Creates a [`DataFrame`] for reading an JSON data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`super::ListingTable`].
    ///
    /// For an example, see [`read_csv`](Self::read_csv)
    pub async fn read_json<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: JsonReadOptions<'_>,
    ) -> Result<DataFrame> {
        self._read_type(table_paths, options).await
    }

    /// Registers a JSON file as a table that it can be referenced
    /// from SQL statements executed against this context.
    pub async fn register_json(
        &self,
        table_ref: impl Into<TableReference>,
        table_path: impl AsRef<str>,
        options: JsonReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options
            .to_listing_options(&self.copied_config(), self.copied_table_options());

        self.register_type_check(table_path.as_ref(), &listing_options.file_extension)?;

        self.register_listing_table(
            table_ref,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;
        Ok(())
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    pub async fn write_json(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        plan_to_json(self.task_ctx(), plan, path).await
    }
}
