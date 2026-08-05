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
use datafusion_ffi::proto::extension_codec_bundle::FFI_ExtensionCodecBundle;

// Creates a default SessionContext and an extension codec bundle carrying the
// default logical and physical codecs, for use in FFI integration tests.
//
// This helper centralizes setup logic and is kept intentionally
// for upcoming FFI test expansions.
#[cfg_attr(not(feature = "integration-tests"), expect(dead_code))]
pub fn ctx_and_codecs() -> (Arc<SessionContext>, FFI_ExtensionCodecBundle) {
    let ctx = Arc::new(SessionContext::default());
    let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
    let codecs = FFI_ExtensionCodecBundle::new_default(&task_ctx_provider, None);

    (ctx, codecs)
}
