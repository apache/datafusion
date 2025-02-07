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

use abi_stable::{
    declare_root_module_statics, export_root_module,
    library::{LibraryError, RootModule},
    package_version_strings,
    prefix_type::PrefixTypeTrait,
    sabi_types::VersionStrings,
    StableAbi,
};

use super::table_provider::FFI_TableProvider;
use arrow_array::RecordBatch;
use async_provider::create_async_table_provider;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::record_batch,
};
use sync_provider::create_sync_table_provider;

mod async_provider;
mod sync_provider;

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = TableProviderModuleRef)))]
/// This struct defines the module interfaces. It is to be shared by
/// both the module loading program and library that implements the
/// module. It is possible to move this definition into the loading
/// program and reference it in the modules, but this example shows
/// how a user may wish to separate these concerns.
pub struct TableProviderModule {
    /// Constructs the table provider
    pub create_table: extern "C" fn(synchronous: bool) -> FFI_TableProvider,

    pub version: extern "C" fn() -> u64,
}

impl RootModule for TableProviderModuleRef {
    declare_root_module_statics! {TableProviderModuleRef}
    const BASE_NAME: &'static str = "datafusion_ffi";
    const NAME: &'static str = "datafusion_ffi";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    fn initialization(self) -> Result<Self, LibraryError> {
        Ok(self)
    }
}

fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Float64, true),
    ]))
}

pub fn create_record_batch(start_value: i32, num_values: usize) -> RecordBatch {
    let end_value = start_value + num_values as i32;
    let a_vals: Vec<i32> = (start_value..end_value).collect();
    let b_vals: Vec<f64> = a_vals.iter().map(|v| *v as f64).collect();

    record_batch!(("a", Int32, a_vals), ("b", Float64, b_vals)).unwrap()
}

/// Here we only wish to create a simple table provider as an example.
/// We create an in-memory table and convert it to it's FFI counterpart.
extern "C" fn construct_table_provider(synchronous: bool) -> FFI_TableProvider {
    match synchronous {
        true => create_sync_table_provider(),
        false => create_async_table_provider(),
    }
}

#[export_root_module]
/// This defines the entry point for using the module.
pub fn get_simple_memory_table() -> TableProviderModuleRef {
    TableProviderModule {
        create_table: construct_table_provider,
        version: super::version,
    }
    .leak_into_prefix()
}
