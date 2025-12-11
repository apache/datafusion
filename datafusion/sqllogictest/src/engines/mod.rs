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

/// Implementation of sqllogictest for datafusion.
mod conversion;
mod currently_executed_sql;
mod datafusion_engine;
mod datafusion_substrait_roundtrip_engine;
mod output;

pub use datafusion_engine::convert_batches;
pub use datafusion_engine::convert_schema_to_types;
pub use datafusion_engine::DFSqlLogicTestError;
pub use datafusion_engine::DataFusion;
pub use datafusion_substrait_roundtrip_engine::DataFusionSubstraitRoundTrip;
pub use output::DFColumnType;
pub use output::DFOutput;

pub use currently_executed_sql::CurrentlyExecutingSqlTracker;

#[cfg(feature = "postgres")]
mod postgres_engine;

#[cfg(feature = "postgres")]
pub use postgres_engine::Postgres;
