// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_execution::TaskContextProvider;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;

// Creates a default SessionContext and FFI Logical Extension Codec
// for use in FFI integration tests.
//
// This helper centralizes setup logic and is kept intentionally
// for upcoming FFI test expansions.
#[cfg_attr(not(feature = "integration-tests"), expect(dead_code))]
pub fn ctx_and_codec() -> (Arc<SessionContext>, FFI_LogicalExtensionCodec) {
    let ctx = Arc::new(SessionContext::default());
    let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
    let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);
    let codec = FFI_LogicalExtensionCodec::new(
        Arc::new(DefaultLogicalExtensionCodec {}),
        None,
        task_ctx_provider,
    );

    (ctx, codec)
}
