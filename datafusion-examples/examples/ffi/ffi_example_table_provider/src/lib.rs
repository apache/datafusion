use std::sync::Arc;

use abi_stable::{export_root_module, prefix_type::PrefixTypeTrait};
use arrow_array::RecordBatch;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::record_batch,
    datasource::MemTable,
};
use datafusion_ffi::table_provider::FFI_TableProvider;
use ffi_module_interface::{TableProviderModule, TableProviderModuleRef};

fn create_record_batch(start_value: i32, num_values: usize) -> RecordBatch {
    let end_value = start_value + num_values as i32;
    let a_vals: Vec<i32> = (start_value..end_value).collect();
    let b_vals: Vec<f64> = a_vals.iter().map(|v| *v as f64).collect();

    record_batch!(("a", Int32, a_vals), ("b", Float64, b_vals)).unwrap()
}

extern "C" fn construct_simple_table_provider() -> FFI_TableProvider {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Float64, true),
    ]));

    let batches = vec![
        create_record_batch(1, 5),
        create_record_batch(6, 1),
        create_record_batch(7, 5),
    ];

    let table_provider = MemTable::try_new(schema, vec![batches]).unwrap();

    FFI_TableProvider::new(Arc::new(table_provider), true)
}

#[export_root_module]
pub fn get_simple_memory_table() -> TableProviderModuleRef {
    TableProviderModule {
        create_table: construct_simple_table_provider,
    }
    .leak_into_prefix()
}
