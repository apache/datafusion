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

use abi_stable::{export_root_module, prefix_type::PrefixTypeTrait};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{common::record_batch, datasource::MemTable};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;
use ffi_module_interface::{TableProviderModule, TableProviderModuleRef};

fn create_record_batch(start_value: i32, num_values: usize) -> RecordBatch {
    let end_value = start_value + num_values as i32;
    let a_vals: Vec<i32> = (start_value..end_value).collect();
    let b_vals: Vec<f64> = a_vals.iter().map(|v| *v as f64).collect();

    record_batch!(("a", Int32, a_vals), ("b", Float64, b_vals)).unwrap()
}

/// Here we only wish to create a simple table provider as an example.
/// We create an in-memory table and convert it to it's FFI counterpart.
extern "C" fn construct_simple_table_provider(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProvider {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Float64, true),
    ]));

    // It is useful to create these as multiple record batches
    // so that we can demonstrate the FFI stream.
    let batches = vec![
        create_record_batch(1, 5),
        create_record_batch(6, 1),
        create_record_batch(7, 5),
    ];

    let table_provider = MemTable::try_new(schema, vec![batches]).unwrap();

    FFI_TableProvider::new_with_ffi_codec(Arc::new(table_provider), true, None, codec)
}

#[export_root_module]
/// This defines the entry point for using the module.
pub fn get_simple_memory_table() -> TableProviderModuleRef {
    TableProviderModule {
        create_table: construct_simple_table_provider,
    }
    .leak_into_prefix()
}
