// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// This code is generated so we don't want to fix any lint violations manually
#[allow(clippy::allow_attributes)]
#[allow(clippy::all)]
#[allow(clippy::pedantic)]
#[allow(clippy::uninlined_format_args)]
#[allow(unused_qualifications)]
#[rustfmt::skip]
pub mod datafusion_proto_common {
    include!("prost.rs");

    #[cfg(feature = "json")]
    include!("pbjson.rs");
}
