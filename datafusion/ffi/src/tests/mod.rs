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

use abi_stable::library::{LibraryError, RootModule};
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::sabi_types::VersionStrings;
use abi_stable::{
    StableAbi, declare_root_module_statics, export_root_module, package_version_strings,
};
use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use async_provider::create_async_table_provider;
use catalog::create_catalog_provider;
use datafusion_common::record_batch;
use sync_provider::create_sync_table_provider;
use udf_udaf_udwf::{
    create_ffi_abs_func, create_ffi_random_func, create_ffi_rank_func,
    create_ffi_stddev_func, create_ffi_sum_func, create_ffi_table_func,
};

use crate::catalog_provider::FFI_CatalogProvider;
use crate::catalog_provider_list::FFI_CatalogProviderList;
use crate::config::extension_options::FFI_ExtensionOptions;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::table_provider::FFI_TableProvider;
use crate::table_provider_factory::FFI_TableProviderFactory;
use crate::tests::catalog::create_catalog_provider_list;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udtf::FFI_TableFunction;
use crate::udwf::FFI_WindowUDF;

mod async_provider;
pub mod catalog;
pub mod config;
mod sync_provider;
mod table_provider_factory;
mod udf_udaf_udwf;
pub mod utils;

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = ForeignLibraryModuleRef)))]
/// This struct defines the module interfaces. It is to be shared by
/// both the module loading program and library that implements the
/// module.
pub struct ForeignLibraryModule {
    /// Construct an opinionated catalog provider
    pub create_catalog:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_CatalogProvider,

    /// Construct an opinionated catalog provider list
    pub create_catalog_list:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_CatalogProviderList,

    /// Constructs the table provider
    pub create_table: extern "C" fn(
        synchronous: bool,
        codec: FFI_LogicalExtensionCodec,
    ) -> FFI_TableProvider,

    /// Constructs the table provider factory
    pub create_table_factory:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_TableProviderFactory,

    /// Create a scalar UDF
    pub create_scalar_udf: extern "C" fn() -> FFI_ScalarUDF,

    pub create_nullary_udf: extern "C" fn() -> FFI_ScalarUDF,

    pub create_table_function:
        extern "C" fn(FFI_LogicalExtensionCodec) -> FFI_TableFunction,

    /// Create an aggregate UDAF using sum
    pub create_sum_udaf: extern "C" fn() -> FFI_AggregateUDF,

    /// Create  grouping UDAF using stddev
    pub create_stddev_udaf: extern "C" fn() -> FFI_AggregateUDF,

    pub create_rank_udwf: extern "C" fn() -> FFI_WindowUDF,

    /// Create extension options, for either ConfigOptions or TableOptions
    pub create_extension_options: extern "C" fn() -> FFI_ExtensionOptions,

    pub version: extern "C" fn() -> u64,
}

impl RootModule for ForeignLibraryModuleRef {
    declare_root_module_statics! {ForeignLibraryModuleRef}
    const BASE_NAME: &'static str = "datafusion_ffi";
    const NAME: &'static str = "datafusion_ffi";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    fn initialization(self) -> Result<Self, LibraryError> {
        Ok(self)
    }
}

pub fn create_test_schema() -> Arc<Schema> {
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
extern "C" fn construct_table_provider(
    synchronous: bool,
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProvider {
    match synchronous {
        true => create_sync_table_provider(codec),
        false => create_async_table_provider(codec),
    }
}

/// Here we only wish to create a simple table provider as an example.
/// We create an in-memory table and convert it to it's FFI counterpart.
extern "C" fn construct_table_provider_factory(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProviderFactory {
    table_provider_factory::create(codec)
}

#[export_root_module]
/// This defines the entry point for using the module.
pub fn get_foreign_library_module() -> ForeignLibraryModuleRef {
    ForeignLibraryModule {
        create_catalog: create_catalog_provider,
        create_catalog_list: create_catalog_provider_list,
        create_table: construct_table_provider,
        create_table_factory: construct_table_provider_factory,
        create_scalar_udf: create_ffi_abs_func,
        create_nullary_udf: create_ffi_random_func,
        create_table_function: create_ffi_table_func,
        create_sum_udaf: create_ffi_sum_func,
        create_stddev_udaf: create_ffi_stddev_func,
        create_rank_udwf: create_ffi_rank_func,
        create_extension_options: config::create_extension_options,
        version: super::version,
    }
    .leak_into_prefix()
}
