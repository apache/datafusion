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

//! # MATCH_RECOGNIZE Example
//!
//! This example demonstrates implementing SQL `MATCH_RECOGNIZE` pattern matching
//! using a custom [`RelationPlanner`]. Unlike the [`pivot_unpivot`] example that
//! rewrites SQL to standard operations, this example creates a **custom logical
//! plan node** (`MiniMatchRecognizeNode`) to represent the operation.
//!
//! ## Supported Syntax
//!
//! ```sql
//! SELECT * FROM events
//!   MATCH_RECOGNIZE (
//!     PARTITION BY region
//!     MEASURES SUM(price) AS total, AVG(price) AS average
//!     PATTERN (A B+ C)
//!     DEFINE
//!       A AS price < 100,
//!       B AS price BETWEEN 100 AND 200,
//!       C AS price > 200
//!   ) AS matches
//! ```
//!
//! ## Architecture
//!
//! This example demonstrates **logical planning only**. Physical execution would
//! require implementing an [`ExecutionPlan`] (see the [`table_sample`] example
//! for a complete implementation with physical planning).
//!
//! ```text
//! SQL Query
//!     │
//!     ▼
//! ┌─────────────────────────────────────┐
//! │ MatchRecognizePlanner               │
//! │ (RelationPlanner trait)             │
//! │                                     │
//! │ • Parses MATCH_RECOGNIZE syntax     │
//! │ • Creates MiniMatchRecognizeNode    │
//! │ • Converts SQL exprs to DataFusion  │
//! └─────────────────────────────────────┘
//!     │
//!     ▼
//! ┌─────────────────────────────────────┐
//! │ MiniMatchRecognizeNode              │
//! │ (UserDefinedLogicalNode)            │
//! │                                     │
//! │ • measures: [(alias, expr), ...]    │
//! │ • definitions: [(symbol, expr), ...]│
//! └─────────────────────────────────────┘
//! ```
//!
//! [`pivot_unpivot`]: super::pivot_unpivot
//! [`table_sample`]: super::table_sample
//! [`ExecutionPlan`]: datafusion::physical_plan::ExecutionPlan

use std::{any::Any, cmp::Ordering, hash::Hasher, sync::Arc};

use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{
    Expr, UserDefinedLogicalNode,
    logical_plan::{Extension, InvariantLevel, LogicalPlan},
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
};
use datafusion_sql::sqlparser::ast::TableFactor;
use insta::assert_snapshot;

// ============================================================================
// Example Entry Point
// ============================================================================

/// Runs the MATCH_RECOGNIZE examples demonstrating pattern matching on event streams.
///
/// Note: This example demonstrates **logical planning only**. Physical execution
/// would require additional implementation of an [`ExecutionPlan`].
pub async fn match_recognize() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(MatchRecognizePlanner))?;
    register_sample_data(&ctx)?;

    println!("MATCH_RECOGNIZE Example (Logical Planning Only)");
    println!("================================================\n");

    run_examples(&ctx).await
}

async fn run_examples(ctx: &SessionContext) -> Result<()> {
    // Example 1: Basic MATCH_RECOGNIZE with MEASURES and DEFINE
    // Demonstrates: Aggregate measures over matched rows
    let plan = run_example(
        ctx,
        "Example 1: MATCH_RECOGNIZE with aggregations",
        r#"SELECT * FROM events
           MATCH_RECOGNIZE (
             PARTITION BY 1
             MEASURES SUM(price) AS total_price, AVG(price) AS avg_price
             PATTERN (A)
             DEFINE A AS price > 10
           ) AS matches"#,
    )
    .await?;
    assert_snapshot!(plan, @r"
    Projection: matches.price
      SubqueryAlias: matches
        MiniMatchRecognize measures=[total_price := sum(events.price), avg_price := avg(events.price)] define=[a := events.price > Int64(10)]
          TableScan: events
    ");

    // Example 2: Stock price pattern detection
    // Demonstrates: Real-world use case finding prices above threshold
    let plan = run_example(
        ctx,
        "Example 2: Detect high stock prices",
        r#"SELECT * FROM stock_prices
           MATCH_RECOGNIZE (
             MEASURES
               MIN(price) AS min_price,
               MAX(price) AS max_price,
               AVG(price) AS avg_price
             PATTERN (HIGH)
             DEFINE HIGH AS price > 151.0
           ) AS trends"#,
    )
    .await?;
    assert_snapshot!(plan, @r"
    Projection: trends.symbol, trends.price
      SubqueryAlias: trends
        MiniMatchRecognize measures=[min_price := min(stock_prices.price), max_price := max(stock_prices.price), avg_price := avg(stock_prices.price)] define=[high := stock_prices.price > Float64(151)]
          TableScan: stock_prices
    ");

    Ok(())
}

/// Helper to run a single example query and display the logical plan.
async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<String> {
    println!("{title}:\n{sql}\n");
    let plan = ctx.sql(sql).await?.into_unoptimized_plan();
    let plan_str = plan.display_indent().to_string();
    println!("{plan_str}\n");
    Ok(plan_str)
}

/// Register test data tables.
fn register_sample_data(ctx: &SessionContext) -> Result<()> {
    // events: simple price series
    ctx.register_batch(
        "events",
        RecordBatch::try_from_iter(vec![(
            "price",
            Arc::new(Int32Array::from(vec![5, 12, 8, 15, 20])) as ArrayRef,
        )])?,
    )?;

    // stock_prices: realistic stock data
    ctx.register_batch(
        "stock_prices",
        RecordBatch::try_from_iter(vec![
            (
                "symbol",
                Arc::new(StringArray::from(vec!["DDOG", "DDOG", "DDOG", "DDOG"]))
                    as ArrayRef,
            ),
            (
                "price",
                Arc::new(Float64Array::from(vec![150.0, 155.0, 152.0, 158.0])),
            ),
        ])?,
    )?;

    Ok(())
}

// ============================================================================
// Logical Plan Node: MiniMatchRecognizeNode
// ============================================================================

/// A custom logical plan node representing MATCH_RECOGNIZE operations.
///
/// This is a simplified implementation that captures the essential structure:
/// - `measures`: Aggregate expressions computed over matched rows
/// - `definitions`: Symbol definitions (predicate expressions)
///
/// A production implementation would also include:
/// - Pattern specification (regex-like pattern)
/// - Partition and order by clauses
/// - Output mode (ONE ROW PER MATCH, ALL ROWS PER MATCH)
/// - After match skip strategy
#[derive(Debug)]
struct MiniMatchRecognizeNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
    /// Measures: (alias, aggregate_expr)
    measures: Vec<(String, Expr)>,
    /// Symbol definitions: (symbol_name, predicate_expr)
    definitions: Vec<(String, Expr)>,
}

impl UserDefinedLogicalNode for MiniMatchRecognizeNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "MiniMatchRecognize"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        self.measures
            .iter()
            .chain(&self.definitions)
            .map(|(_, expr)| expr.clone())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MiniMatchRecognize")?;

        if !self.measures.is_empty() {
            write!(f, " measures=[")?;
            for (i, (alias, expr)) in self.measures.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{alias} := {expr}")?;
            }
            write!(f, "]")?;
        }

        if !self.definitions.is_empty() {
            write!(f, " define=[")?;
            for (i, (symbol, expr)) in self.definitions.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{symbol} := {expr}")?;
            }
            write!(f, "]")?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        let expected_len = self.measures.len() + self.definitions.len();
        if exprs.len() != expected_len {
            return Err(datafusion_common::plan_datafusion_err!(
                "MiniMatchRecognize: expected {expected_len} expressions, got {}",
                exprs.len()
            ));
        }

        let input = inputs.into_iter().next().ok_or_else(|| {
            datafusion_common::plan_datafusion_err!(
                "MiniMatchRecognize requires exactly one input"
            )
        })?;

        let (measure_exprs, definition_exprs) = exprs.split_at(self.measures.len());

        let measures = self
            .measures
            .iter()
            .zip(measure_exprs)
            .map(|((alias, _), expr)| (alias.clone(), expr.clone()))
            .collect();

        let definitions = self
            .definitions
            .iter()
            .zip(definition_exprs)
            .map(|((symbol, _), expr)| (symbol.clone(), expr.clone()))
            .collect();

        Ok(Arc::new(Self {
            input: Arc::new(input),
            schema: Arc::clone(&self.schema),
            measures,
            definitions,
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        state.write_usize(Arc::as_ptr(&self.input) as usize);
        state.write_usize(self.measures.len());
        state.write_usize(self.definitions.len());
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|o| {
            Arc::ptr_eq(&self.input, &o.input)
                && self.measures == o.measures
                && self.definitions == o.definitions
        })
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        if self.dyn_eq(other) {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

// ============================================================================
// Relation Planner: MatchRecognizePlanner
// ============================================================================

/// Relation planner that creates `MiniMatchRecognizeNode` for MATCH_RECOGNIZE queries.
#[derive(Debug)]
struct MatchRecognizePlanner;

impl RelationPlanner for MatchRecognizePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        ctx: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let TableFactor::MatchRecognize {
            table,
            measures,
            symbols,
            alias,
            ..
        } = relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        // Plan the input table
        let input = ctx.plan(*table)?;
        let schema = input.schema().clone();

        // Convert MEASURES: SQL expressions → DataFusion expressions
        let planned_measures: Vec<(String, Expr)> = measures
            .iter()
            .map(|m| {
                let alias = ctx.normalize_ident(m.alias.clone());
                let expr = ctx.sql_to_expr(m.expr.clone(), schema.as_ref())?;
                Ok((alias, expr))
            })
            .collect::<Result<_>>()?;

        // Convert DEFINE: symbol definitions → DataFusion expressions
        let planned_definitions: Vec<(String, Expr)> = symbols
            .iter()
            .map(|s| {
                let name = ctx.normalize_ident(s.symbol.clone());
                let expr = ctx.sql_to_expr(s.definition.clone(), schema.as_ref())?;
                Ok((name, expr))
            })
            .collect::<Result<_>>()?;

        // Create the custom node
        let node = MiniMatchRecognizeNode {
            input: Arc::new(input),
            schema,
            measures: planned_measures,
            definitions: planned_definitions,
        };

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });

        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
}
