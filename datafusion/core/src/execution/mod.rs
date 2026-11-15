// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Shared state for query planning and execution.

pub mod context;
pub mod session_state;
pub use session_state::{SessionState, SessionStateBuilder};

mod session_state_defaults;

pub use session_state_defaults::SessionStateDefaults;

// backwards compatibility
pub use crate::datasource::file_format::options;
pub use datafusion_execution::*;
