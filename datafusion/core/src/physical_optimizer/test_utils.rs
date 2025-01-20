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

//! Collection of testing utility functions that are leveraged by the query optimizer rules

#![allow(missing_docs)]

use std::sync::Arc;

use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
use crate::physical_plan::ExecutionPlan;

use arrow_schema::SchemaRef;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::PhysicalSortExpr;

/// Create a non sorted parquet exec
pub fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
    ParquetExec::builder(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema.clone())
            .with_file(PartitionedFile::new("x".to_string(), 100)),
    )
    .build_arc()
}

// Created a sorted parquet exec
pub fn parquet_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    ParquetExec::builder(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema.clone())
            .with_file(PartitionedFile::new("x".to_string(), 100))
            .with_output_ordering(vec![sort_exprs]),
    )
    .build_arc()
}
