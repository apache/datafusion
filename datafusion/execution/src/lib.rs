// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! DataFusion execution configuration and runtime structures

mod async_stream;
pub mod cache;
pub mod config;
pub mod disk_manager;
pub mod memory_pool;
pub mod object_store;
#[cfg(feature = "parquet_encryption")]
pub mod parquet_encryption;
pub mod runtime_env;
pub mod spill_file;
mod stream;
mod task;

pub mod registry {
    pub use datafusion_expr::registry::{
        FunctionRegistry, MemoryFunctionRegistry, SerializerRegistry,
    };
}

pub use async_stream::{Emitter, TryEmitter, async_stream, async_try_stream};
pub use disk_manager::DiskManager;
pub use registry::FunctionRegistry;
pub use spill_file::{AsyncSpillWriter, SpillFile, SpillWriter, TempFileFactory};
pub use stream::{RecordBatchStream, SendableRecordBatchStream};
pub use task::{TaskContext, TaskContextProvider};
