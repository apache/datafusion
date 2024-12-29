use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion_ffi::table_provider::FFI_TableProvider;

use crate::{create_record_batch, create_test_schema};

pub(crate) fn create_sync_table_provider() -> FFI_TableProvider {
    let schema = create_test_schema();

    // It is useful to create these as multiple record batches
    // so that we can demonstrate the FFI stream.
    let batches = vec![
        create_record_batch(1, 5),
        create_record_batch(6, 1),
        create_record_batch(7, 5),
    ];

    let table_provider = MemTable::try_new(schema, vec![batches]).unwrap();

    FFI_TableProvider::new(Arc::new(table_provider), true, None)
}
