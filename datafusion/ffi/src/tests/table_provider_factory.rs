// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_catalog::{MemTable, Session, TableProvider, TableProviderFactory};
use datafusion_common::Result;
use datafusion_expr::CreateExternalTable;

use super::{create_record_batch, create_test_schema};
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::table_provider_factory::FFI_TableProviderFactory;

#[derive(Debug)]
pub struct TestTableProviderFactory {}

#[async_trait]
impl TableProviderFactory for TestTableProviderFactory {
    async fn create(
        &self,
        _session: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema = create_test_schema();

        // It is useful to create these as multiple record batches
        // so that we can demonstrate the FFI stream.
        let batches = vec![
            create_record_batch(1, 5),
            create_record_batch(6, 1),
            create_record_batch(7, 5),
        ];

        let table_provider = MemTable::try_new(schema, vec![batches]).unwrap();

        Ok(Arc::new(table_provider))
    }
}

pub(crate) fn create(codec: FFI_LogicalExtensionCodec) -> FFI_TableProviderFactory {
    let factory = TestTableProviderFactory {};
    FFI_TableProviderFactory::new_with_ffi_codec(Arc::new(factory), None, codec)
}
