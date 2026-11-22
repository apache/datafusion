// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::super::options::{AvroReadOptions, ReadOptions};
use super::{DataFilePaths, DataFrame, Result, SessionContext};
use datafusion_common::TableReference;
use std::sync::Arc;

impl SessionContext {
    /// Creates a [`DataFrame`] for reading an Avro data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`super::ListingTable`].
    ///
    /// For an example, see [`read_csv`](Self::read_csv)
    pub async fn read_avro<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: AvroReadOptions<'_>,
    ) -> Result<DataFrame> {
        self._read_type(table_paths, options).await
    }

    /// Registers an Avro file as a table that can be referenced from
    /// SQL statements executed against this context.
    pub async fn register_avro(
        &self,
        table_ref: impl Into<TableReference>,
        table_path: impl AsRef<str>,
        options: AvroReadOptions<'_>,
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
}
