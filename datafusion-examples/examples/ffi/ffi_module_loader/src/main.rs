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
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::TaskContextProvider,
    prelude::SessionContext,
};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use ffi_module_interface::TableProviderModule;

#[tokio::main]
async fn main() -> Result<()> {
    // Find the location of the library. This is specific to the build environment,
    // so you will need to change the approach here based on your use case.
    let lib_prefix = if cfg!(target_os = "windows") {
        ""
    } else {
        "lib"
    };
    let lib_ext = if cfg!(target_os = "macos") {
        "dylib"
    } else if cfg!(target_os = "windows") {
        "dll"
    } else {
        "so"
    };

    let build_type = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    let library_path = format!(
        "../../../../target/{build_type}/{lib_prefix}ffi_example_table_provider.{lib_ext}"
    );

    // Load the library using libloading
    let lib = unsafe {
        libloading::Library::new(&library_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    };

    let get_module: libloading::Symbol<extern "C" fn() -> TableProviderModule> = unsafe {
        lib.get(b"ffi_example_get_module")
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    };

    let table_provider_module = get_module();

    let ctx = Arc::new(SessionContext::new());
    let codec = FFI_LogicalExtensionCodec::new_default(
        &(Arc::clone(&ctx) as Arc<dyn TaskContextProvider>),
    );

    // By calling the code below, the table provided will be created within
    // the module's code.
    let ffi_table_provider = (table_provider_module.create_table)(codec);

    // In order to access the table provider within this executable, we need to
    // turn it into a `TableProvider`.
    let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_table_provider).into();

    // Display the data to show the full cycle works.
    ctx.register_table("external_table", foreign_table_provider)?;
    let df = ctx.table("external_table").await?;
    df.show().await?;

    Ok(())
}
