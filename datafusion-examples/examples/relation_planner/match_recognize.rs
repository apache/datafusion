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

//! This example demonstrates using custom relation planners to implement
//! MATCH_RECOGNIZE-style pattern matching on event streams.
//!
//! MATCH_RECOGNIZE is a SQL extension for pattern matching on ordered data,
//! similar to regular expressions but for relational data. This example shows
//! how to use custom planners to implement new SQL syntax.

use std::{any::Any, cmp::Ordering, hash::Hasher, sync::Arc};

use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{
    logical_plan::{Extension, InvariantLevel, LogicalPlan},
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
    Expr, UserDefinedLogicalNode,
};
use datafusion_sql::sqlparser::ast::TableFactor;

/// This example demonstrates using custom relation planners to implement
/// MATCH_RECOGNIZE-style pattern matching on event streams.
pub async fn match_recognize() -> Result<()> {
    let ctx = SessionContext::new();

    // Register sample data tables
    register_sample_data(&ctx)?;

    // Register custom planner
    ctx.register_relation_planner(Arc::new(MatchRecognizePlanner))?;

    println!("Custom Relation Planner: MATCH_RECOGNIZE Pattern Matching");
    println!("==========================================================\n");

    // Example 1: Basic MATCH_RECOGNIZE with MEASURES and DEFINE clauses
    // Shows: How to use MATCH_RECOGNIZE to find patterns in event data with aggregations
    // Expected: Logical plan showing MiniMatchRecognize node with SUM and AVG measures
    // Note: This demonstrates the logical planning phase - actual execution would require physical implementation
    // Actual (Logical Plan):
    // Projection: t.price
    //   SubqueryAlias: t
    //     MiniMatchRecognize measures=[total_price := sum(price), avg_price := avg(price)] define=[a := price > Int64(10)]
    //       EmptyRelation: rows=0
    run_example(
        &ctx,
        "Example 1: MATCH_RECOGNIZE with measures and definitions",
        r#"SELECT * FROM events 
           MATCH_RECOGNIZE (
             PARTITION BY 1 
             MEASURES SUM(price) AS total_price, AVG(price) AS avg_price 
             PATTERN (A) 
             DEFINE A AS price > 10
           ) AS t"#,
    )
    .await?;

    // Example 2: Stock price pattern detection using MATCH_RECOGNIZE
    // Shows: How to detect patterns in financial data (e.g., stocks above threshold)
    // Expected: Logical plan showing MiniMatchRecognize with MIN, MAX, AVG measures on stock prices
    // Note: Uses real stock data (DDOG prices: 150, 155, 152, 158) to find patterns above 151.0
    // Actual (Logical Plan):
    // Projection: trends.column1, trends.column2
    //   SubqueryAlias: trends
    //     MiniMatchRecognize measures=[min_price := min(column2), max_price := max(column2), avg_price := avg(column2)] define=[high := column2 > Float64(151)]
    //       Values: (Utf8("DDOG"), Float64(150)), (Utf8("DDOG"), Float64(155)), (Utf8("DDOG"), Float64(152)), (Utf8("DDOG"), Float64(158))
    run_example(
        &ctx,
        "Example 2: Detect stocks above threshold using MATCH_RECOGNIZE",
        r#"SELECT * FROM stock_prices 
           MATCH_RECOGNIZE (
             MEASURES MIN(column2) AS min_price, 
                      MAX(column2) AS max_price, 
                      AVG(column2) AS avg_price 
             PATTERN (HIGH) 
             DEFINE HIGH AS column2 > 151.0
           ) AS trends"#,
    )
    .await?;

    Ok(())
}

/// Register sample data tables for the examples
fn register_sample_data(ctx: &SessionContext) -> Result<()> {
    // Create events table with price column
    let price: ArrayRef = Arc::new(Int32Array::from(vec![5, 12, 8, 15, 20]));
    let batch = RecordBatch::try_from_iter(vec![("price", price)])?;
    ctx.register_batch("events", batch)?;

    // Create stock_prices table with symbol and price columns
    let symbol: ArrayRef =
        Arc::new(StringArray::from(vec!["DDOG", "DDOG", "DDOG", "DDOG"]));
    let price: ArrayRef = Arc::new(Float64Array::from(vec![150.0, 155.0, 152.0, 158.0]));
    let batch =
        RecordBatch::try_from_iter(vec![("column1", symbol), ("column2", price)])?;
    ctx.register_batch("stock_prices", batch)?;

    Ok(())
}

async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<()> {
    println!("{title}:\n{sql}\n");
    let plan = ctx.sql(sql).await?.into_unoptimized_plan();
    println!("Logical Plan:");
    println!("{}\n", plan.display_indent());
    Ok(())
}

/// A custom logical plan node representing MATCH_RECOGNIZE operations
#[derive(Debug)]
struct MiniMatchRecognizeNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
    measures: Vec<(String, Expr)>,
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
        vec![self.input.as_ref()]
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
            for (idx, (alias, expr)) in self.measures.iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{alias} := {expr}")?;
            }
            write!(f, "]")?;
        }

        if !self.definitions.is_empty() {
            write!(f, " define=[")?;
            for (idx, (symbol, expr)) in self.definitions.iter().enumerate() {
                if idx > 0 {
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
        if exprs.len() != self.measures.len() + self.definitions.len() {
            return Err(DataFusionError::Internal(
                "MiniMatchRecognize received an unexpected expression count".into(),
            ));
        }

        let (measure_exprs, definition_exprs) = exprs.split_at(self.measures.len());

        let Some(first_input) = inputs.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "MiniMatchRecognize requires a single input".into(),
            ));
        };

        let measures = self
            .measures
            .iter()
            .zip(measure_exprs.iter())
            .map(|((alias, _), expr)| (alias.clone(), expr.clone()))
            .collect();

        let definitions = self
            .definitions
            .iter()
            .zip(definition_exprs.iter())
            .map(|((symbol, _), expr)| (symbol.clone(), expr.clone()))
            .collect();

        Ok(Arc::new(Self {
            input: Arc::new(first_input),
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
        other
            .as_any()
            .downcast_ref::<Self>()
            .map(|o| {
                Arc::ptr_eq(&self.input, &o.input)
                    && self.measures == o.measures
                    && self.definitions == o.definitions
            })
            .unwrap_or(false)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        if self.dyn_eq(other) {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

/// Custom planner that handles MATCH_RECOGNIZE table factor syntax
#[derive(Debug)]
struct MatchRecognizePlanner;

impl RelationPlanner for MatchRecognizePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::MatchRecognize {
            table,
            measures,
            symbols,
            alias,
            ..
        } = relation
        {
            println!("[MatchRecognizePlanner] Processing MATCH_RECOGNIZE clause");

            // DEMONSTRATE context.plan(): Recursively plan the input table
            println!("[MatchRecognizePlanner] Using context.plan() to plan input table");
            let input = context.plan(*table)?;
            let input_schema = input.schema().clone();
            println!(
                "[MatchRecognizePlanner] Input schema has {} fields",
                input_schema.fields().len()
            );

            // DEMONSTRATE normalize_ident() and sql_to_expr(): Process MEASURES
            let planned_measures = measures
                .iter()
                .map(|measure| {
                    // Normalize the measure alias
                    let alias = context.normalize_ident(measure.alias.clone());
                    println!("[MatchRecognizePlanner] Normalized measure alias: {alias}");

                    // Convert SQL expression to DataFusion expression
                    let expr = context
                        .sql_to_expr(measure.expr.clone(), input_schema.as_ref())?;
                    println!(
                        "[MatchRecognizePlanner] Planned measure expression: {expr:?}"
                    );

                    Ok((alias, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            // DEMONSTRATE normalize_ident() and sql_to_expr(): Process DEFINE
            let planned_definitions = symbols
                .iter()
                .map(|symbol| {
                    // Normalize the symbol name
                    let name = context.normalize_ident(symbol.symbol.clone());
                    println!("[MatchRecognizePlanner] Normalized symbol: {name}");

                    // Convert SQL expression to DataFusion expression
                    let expr = context
                        .sql_to_expr(symbol.definition.clone(), input_schema.as_ref())?;
                    println!("[MatchRecognizePlanner] Planned definition: {expr:?}");

                    Ok((name, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            // Create the custom MATCH_RECOGNIZE node
            let node = MiniMatchRecognizeNode {
                schema: Arc::clone(&input_schema),
                input: Arc::new(input),
                measures: planned_measures,
                definitions: planned_definitions,
            };

            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            });

            println!("[MatchRecognizePlanner] Successfully created MATCH_RECOGNIZE plan");
            return Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)));
        }

        Ok(RelationPlanning::Original(relation))
    }
}
