// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

pub mod file_format;
pub mod source;

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::{file::FileSource, file_scan_config::FileScanConfig};
use datafusion_execution::object_store::ObjectStoreUrl;
pub use file_format::*;

/// Returns a [`FileScanConfig`] for given `file_groups`
pub fn partitioned_csv_config(
    file_groups: Vec<FileGroup>,
    file_source: Arc<dyn FileSource>,
) -> Result<FileScanConfig> {
    Ok(
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(file_groups)
            .build(),
    )
}
