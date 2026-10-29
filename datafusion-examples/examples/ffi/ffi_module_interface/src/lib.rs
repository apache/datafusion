// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;

/// This struct defines the module interfaces. It is to be shared by
/// both the module loading program and library that implements the
/// module. It is possible to move this definition into the loading
/// program and reference it in the modules, but this example shows
/// how a user may wish to separate these concerns.
#[repr(C)]
pub struct TableProviderModule {
    /// Constructs the table provider
    pub create_table:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_TableProvider,
}
