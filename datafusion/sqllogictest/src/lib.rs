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
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

//! DataFusion sqllogictest driver

mod config_matrix;
mod engines;
mod test_file;

pub use engines::CurrentlyExecutingSqlTracker;
pub use engines::DFColumnType;
pub use engines::DFOutput;
pub use engines::DFSqlLogicTestError;
pub use engines::DataFusion;
#[cfg(feature = "substrait")]
pub use engines::DataFusionSubstraitRoundTrip;
pub use engines::convert_batches;
pub use engines::convert_schema_to_types;

#[cfg(feature = "postgres")]
pub use engines::Postgres;

mod filters;
mod memory_drift;
mod test_context;
mod util;

pub use config_matrix::{TestConfiguration, run_each_configuration, test_configurations};
pub use filters::*;
pub use memory_drift::{
    CountingAllocator, enable_memory_drift_logging, flush_thread_allocations,
    memory_drift_tracker,
};
pub use test_context::TestContext;
pub use test_file::TestFile;
pub use util::*;
