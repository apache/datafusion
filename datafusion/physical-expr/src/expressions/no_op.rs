// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! NoOp placeholder for physical operations

use std::hash::Hash;
use std::sync::Arc;

use crate::PhysicalExpr;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ColumnarValue;

/// A place holder expression, can not be evaluated.
///
/// Used in some cases where an `Arc<dyn PhysicalExpr>` is needed, such as `children()`
#[derive(Debug, PartialEq, Eq, Default, Hash)]
pub struct NoOp {}

impl NoOp {
    /// Create a NoOp expression
    pub fn new() -> Self {
        Self {}
    }
}

impl std::fmt::Display for NoOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NoOp")
    }
}

impl PhysicalExpr for NoOp {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("NoOp::evaluate() should not be called")
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
