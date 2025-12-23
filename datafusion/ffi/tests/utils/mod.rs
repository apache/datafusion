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

use datafusion::prelude::SessionContext;
use datafusion_execution::TaskContextProvider;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;

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
