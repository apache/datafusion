// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::ArrayRef;
use arrow::compute::concat;
use arrow::datatypes::{Schema, SchemaBuilder};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_physical_expr::window::WindowExpr;
use std::ops::Range;
use std::sync::Arc;

pub(crate) fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let capacity = input_schema.fields().len() + window_expr.len();
    let mut builder = SchemaBuilder::with_capacity(capacity);
    builder.extend(input_schema.fields().iter().cloned());
    // append results to the schema
    for expr in window_expr {
        builder.push(expr.field()?);
    }
    Ok(builder
        .finish()
        .with_metadata(input_schema.metadata().clone()))
}

/// Evaluates `window_expr` on each of the whole partitions of `batch` in
/// `partitions`, and returns one array per window expression that covers
/// `partitions` in order.
pub(crate) fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    batch: &RecordBatch,
    partitions: &[Range<usize>],
) -> Result<Vec<ArrayRef>> {
    let mut results = Vec::with_capacity(window_expr.len());
    // The expressions that are evaluated one partition at a time, and their results
    let mut per_partition = vec![];
    for (idx, expr) in window_expr.iter().enumerate() {
        let result = expr.evaluate_partitions(batch, partitions)?;
        if result.is_none() {
            per_partition.push((idx, Vec::with_capacity(partitions.len())));
        }
        results.push(result);
    }
    if !per_partition.is_empty() {
        for range in partitions {
            let partition = batch.slice(range.start, range.len());
            for (idx, arrays) in &mut per_partition {
                arrays.push(window_expr[*idx].evaluate(&partition)?);
            }
        }
    }
    for (idx, arrays) in per_partition {
        let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        results[idx] = Some(concat(&arrays)?);
    }
    Ok(results.into_iter().flatten().collect())
}
