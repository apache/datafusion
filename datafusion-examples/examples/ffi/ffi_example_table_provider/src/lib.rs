// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{RecordBatch, record_batch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;
use ffi_module_interface::TableProviderModule;

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

#[unsafe(no_mangle)]
/// This defines the entry point for using the module.
pub extern "C" fn ffi_example_get_module() -> TableProviderModule {
    TableProviderModule {
        create_table: construct_simple_table_provider,
    }
}
