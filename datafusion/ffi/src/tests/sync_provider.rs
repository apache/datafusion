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

use datafusion_catalog::MemTable;

use super::{create_record_batch, create_test_schema};
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::table_provider::FFI_TableProvider;

pub(crate) fn create_sync_table_provider(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProvider {
    let schema = create_test_schema();

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
