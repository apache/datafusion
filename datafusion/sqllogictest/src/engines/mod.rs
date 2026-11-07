// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Implementation of sqllogictest for datafusion.
mod conversion;
mod currently_executed_sql;
mod datafusion_engine;
#[cfg(feature = "substrait")]
mod datafusion_substrait_roundtrip_engine;
mod output;

pub use datafusion_engine::DFSqlLogicTestError;
pub use datafusion_engine::DataFusion;
pub use datafusion_engine::convert_batches;
pub use datafusion_engine::convert_schema_to_types;
#[cfg(feature = "substrait")]
pub use datafusion_substrait_roundtrip_engine::DataFusionSubstraitRoundTrip;
pub use output::DFColumnType;
pub use output::DFOutput;

pub use currently_executed_sql::CurrentlyExecutingSqlTracker;

#[cfg(feature = "postgres")]
mod postgres_engine;

#[cfg(feature = "postgres")]
pub use postgres_engine::Postgres;
