// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[cfg(feature = "integration-tests")]
pub use datafusion_ffi::tests::{
    datafusion_ffi_get_module, datafusion_ffi_test_create_exec_with_byte_metrics,
};
