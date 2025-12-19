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

use std::sync::Arc;

use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};

use abi_stable::library::{RootModule, development_utils::compute_library_path};
use datafusion::datasource::TableProvider;
use ffi_module_interface::TableProviderModuleRef;

#[tokio::main]
async fn main() -> Result<()> {
    // Find the location of the library. This is specific to the build environment,
    // so you will need to change the approach here based on your use case.
    let target: &std::path::Path = "../../../../target/".as_ref();
    let library_path = compute_library_path::<TableProviderModuleRef>(target)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Load the module
    let table_provider_module =
        TableProviderModuleRef::load_from_directory(&library_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // By calling the code below, the table provided will be created within
    // the module's code.
    let ffi_table_provider =
        table_provider_module
            .create_table()
            .ok_or(DataFusionError::NotImplemented(
                "External table provider failed to implement create_table".to_string(),
            ))?();

    // In order to access the table provider within this executable, we need to
    // turn it into a `TableProvider`.
    let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_table_provider).into();

    let ctx = SessionContext::new();

    // Display the data to show the full cycle works.
    ctx.register_table("external_table", foreign_table_provider)?;
    let df = ctx.table("external_table").await?;
    df.show().await?;

    Ok(())
}
