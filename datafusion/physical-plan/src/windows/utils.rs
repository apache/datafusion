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
/// `partitions` in order. An expression that evaluates several partitions in
/// one call gets runs of at most `batch_size` rows, or of one larger
/// partition, so that its frames stay batch-sized.
pub(crate) fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    batch: &RecordBatch,
    partitions: &[Range<usize>],
    batch_size: usize,
) -> Result<Vec<ArrayRef>> {
    // The arrays to concatenate for each expression
    let mut results = vec![vec![]; window_expr.len()];
    // The expressions that are evaluated one partition at a time
    let mut per_partition = vec![];
    for (idx, expr) in window_expr.iter().enumerate() {
        for run in partition_runs(partitions, batch_size) {
            let Some(result) = expr.evaluate_partitions(batch, run)? else {
                results[idx] = Vec::with_capacity(partitions.len());
                per_partition.push(idx);
                break;
            };
            results[idx].push(result);
        }
    }
    if !per_partition.is_empty() {
        for range in partitions {
            let partition = batch.slice(range.start, range.len());
            for &idx in &per_partition {
                results[idx].push(window_expr[idx].evaluate(&partition)?);
            }
        }
    }
    results
        .iter()
        .map(|arrays| {
            let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            Ok(concat(&arrays)?)
        })
        .collect()
}

/// Splits `partitions`, consecutive row ranges, into runs of at most
/// `max_rows` rows, or of one larger partition
fn partition_runs(
    partitions: &[Range<usize>],
    max_rows: usize,
) -> impl Iterator<Item = &[Range<usize>]> {
    let mut rest = partitions;
    std::iter::from_fn(move || {
        let first = rest.first()?;
        let len = 1 + rest[1..]
            .iter()
            .take_while(|p| p.end - first.start <= max_rows)
            .count();
        let (run, tail) = rest.split_at(len);
        rest = tail;
        Some(run)
    })
}
