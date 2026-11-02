// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`EmptyTable`] useful for testing.

use std::sync::Arc;

use arrow::datatypes::*;
use async_trait::async_trait;
use datafusion_common::{Result, project_schema};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;

use crate::Session;
use crate::TableProvider;

/// An empty plan that is useful for testing and generating plans
/// without mapping them to actual data.
#[derive(Debug)]
pub struct EmptyTable {
    schema: SchemaRef,
    partitions: usize,
}

impl EmptyTable {
    /// Initialize a new `EmptyTable` from a schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            partitions: 1,
        }
    }

    /// Creates a new EmptyTable with specified partition number.
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }
}

#[async_trait]
impl TableProvider for EmptyTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // even though there is no data, projections apply
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(
            EmptyExec::new(projected_schema).with_partitions(self.partitions),
        ))
    }
}
