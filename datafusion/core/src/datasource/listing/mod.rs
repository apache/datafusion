// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! A table that uses the `ObjectStore` listing capability
//! to get the list of files to process.

mod table;
pub use datafusion_catalog_listing::helpers;
pub use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
// Keep for backwards compatibility until removed
#[expect(deprecated)]
pub use datafusion_datasource::PartitionedFileStream;
pub use datafusion_datasource::{FileRange, ListingTableUrl, PartitionedFile};
pub use table::ListingTableConfigExt;
