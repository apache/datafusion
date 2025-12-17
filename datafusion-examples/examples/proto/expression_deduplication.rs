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

//! See `main.rs` for how to run it.
//!
//! This example demonstrates how to use the `PhysicalExtensionCodec` trait's
//! interception methods to implement expression deduplication during deserialization.
//!
//! This pattern is inspired by PR #18192, which introduces expression caching
//! to reduce memory usage when deserializing plans with duplicate expressions.
//!
//! The key insight is that identical expressions serialize to identical protobuf bytes.
//! By caching deserialized expressions keyed by their protobuf bytes, we can:
//! 1. Return the same Arc for duplicate expressions
//! 2. Reduce memory allocation during deserialization
//! 3. Enable downstream optimizations that rely on Arc pointer equality
//!
//! This demonstrates the decorator pattern enabled by the `PhysicalExtensionCodec` trait,
//! where all expression serialization/deserialization routes through the codec methods.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::{BinaryExpr, col, lit};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, PhysicalExtensionCodec, default_deserialize_physical_expr,
    default_deserialize_physical_plan, default_serialize_physical_expr,
    default_serialize_physical_plan,
};
use datafusion_proto::protobuf;
use prost::Message;

/// Example showing how to implement expression deduplication using the codec decorator pattern.
///
/// This demonstrates:
/// 1. Creating a CachingCodec that caches expressions by their protobuf bytes
/// 2. Intercepting deserialization to return cached Arcs for duplicate expressions
/// 3. Verifying that duplicate expressions share the same Arc after deserialization
pub async fn expression_deduplication() -> Result<()> {
    println!("=== Expression Deduplication Example ===\n");

    // Create a schema for our test expressions
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // Step 1: Create expressions with duplicates
    println!("Step 1: Creating expressions with duplicates...");

    // Create expression: (a + b)
    let a_plus_b = Arc::new(BinaryExpr::new(
        col("a", &schema)?,
        Operator::Plus,
        col("b", &schema)?,
    )) as Arc<dyn PhysicalExpr>;

    // Create expression: (a + b) > 10 AND (a + b) < 100
    // Note: We create (a + b) twice to simulate duplicate expressions
    let expr1 = Arc::new(BinaryExpr::new(
        Arc::clone(&a_plus_b),
        Operator::Gt,
        lit(10i32),
    )) as Arc<dyn PhysicalExpr>;

    // Create a clone of (a + b) to simulate a duplicate
    let a_plus_b_2 = Arc::clone(&a_plus_b);

    let expr2 = Arc::new(BinaryExpr::new(a_plus_b_2, Operator::Lt, lit(100i32)))
        as Arc<dyn PhysicalExpr>;

    // Combine: expr1 AND expr2
    let combined_expr =
        Arc::new(BinaryExpr::new(expr1, Operator::And, expr2)) as Arc<dyn PhysicalExpr>;

    println!("  Created expression: (a + b) > 10 AND (a + b) < 100");
    println!("  Note: (a + b) appears twice in the expression tree\n");

    // Step 2: Create a filter plan with this expression
    println!("Step 2: Creating physical plan with the expression...");

    let input = Arc::new(PlaceholderRowExec::new(Arc::clone(&schema)));
    let filter_plan: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(combined_expr, input)?);

    println!("  Created FilterExec with duplicate sub-expressions\n");

    // Step 3: Serialize with the caching codec
    println!("Step 3: Serializing plan...");

    let caching_codec = CachingCodec::new();
    let proto = protobuf::PhysicalPlanNode::try_from_physical_plan(
        filter_plan.clone(),
        &caching_codec,
    )?;

    // Serialize to bytes
    let mut bytes = Vec::new();
    proto.encode(&mut bytes).unwrap();
    println!("  Serialized plan to {} bytes\n", bytes.len());

    // Step 4: Deserialize with the caching codec
    println!("Step 4: Deserializing plan with CachingCodec...");

    let ctx = SessionContext::new();
    let _deserialized_plan =
        proto.try_into_physical_plan(&ctx.task_ctx(), &caching_codec)?;

    // Print statistics
    let stats = caching_codec.stats.read().unwrap();
    println!("  Cache statistics:");
    println!("    - Cache hits:   {}", stats.cache_hits);
    println!("    - Cache misses: {}", stats.cache_misses);
    if stats.cache_hits + stats.cache_misses > 0 {
        println!(
            "    - Hit ratio:    {:.1}%\n",
            100.0 * stats.cache_hits as f64
                / (stats.cache_hits + stats.cache_misses) as f64
        );
    }

    // Step 5: Demonstrate cache effectiveness
    println!("Step 5: Demonstrating cache effectiveness...");

    // Show the cache contents
    let cache = caching_codec.expr_cache.read().unwrap();
    println!("  Cached {} unique expression structures", cache.len());

    // Explain the deduplication
    println!("\n  Analysis:");
    println!("    - The expression tree has duplicate sub-expressions");
    println!("    - Column 'a' appears twice, Column 'b' appears twice");
    println!("    - BinaryExpr(a + b) appears twice");
    println!("    - With caching, identical proto bytes map to the same Arc");
    println!("    - Cache hits indicate expressions that were deduplicated");

    println!("\n=== Example Complete! ===");
    println!("Key takeaways:");
    println!("  1. PhysicalExtensionCodec intercepts ALL expression deserialization");
    println!("  2. Caching by protobuf bytes enables automatic deduplication");
    println!("  3. Identical sub-expressions share the same Arc after deserialization");
    println!("  4. This reduces memory usage and enables pointer-equality optimizations");

    Ok(())
}

// ============================================================================
// CachingCodec - Implements expression deduplication
// ============================================================================

/// Statistics for cache performance monitoring
#[derive(Debug, Default)]
struct CacheStats {
    cache_hits: usize,
    cache_misses: usize,
}

/// A codec that caches deserialized expressions to enable deduplication.
///
/// When deserializing, if we've already seen the same protobuf bytes,
/// we return the cached Arc instead of creating a new allocation.
#[derive(Debug, Default)]
struct CachingCodec {
    /// Cache mapping protobuf bytes -> deserialized expression
    expr_cache: RwLock<HashMap<Vec<u8>, Arc<dyn PhysicalExpr>>>,
    /// Statistics for demonstration
    stats: RwLock<CacheStats>,
}

impl CachingCodec {
    fn new() -> Self {
        Self::default()
    }
}

impl PhysicalExtensionCodec for CachingCodec {
    // Required: decode custom extension nodes
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        datafusion::common::not_impl_err!("No custom extension nodes")
    }

    // Required: encode custom execution plans
    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        datafusion::common::not_impl_err!("No custom extension nodes")
    }

    // Delegate plan serialization to default
    fn serialize_physical_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<protobuf::PhysicalPlanNode> {
        default_serialize_physical_plan(plan, self)
    }

    // Delegate plan deserialization to default
    fn deserialize_physical_plan(
        &self,
        proto: &protobuf::PhysicalPlanNode,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        default_deserialize_physical_plan(proto, ctx, self)
    }

    // Delegate expression serialization to default
    fn serialize_physical_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<protobuf::PhysicalExprNode> {
        default_serialize_physical_expr(expr, self)
    }

    // CACHING IMPLEMENTATION: Intercept expression deserialization
    fn deserialize_physical_expr(
        &self,
        proto: &protobuf::PhysicalExprNode,
        ctx: &TaskContext,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // Create cache key from protobuf bytes
        let mut key = Vec::new();
        proto.encode(&mut key).map_err(|e| {
            datafusion::error::DataFusionError::Internal(format!(
                "Failed to encode proto for cache key: {e}"
            ))
        })?;

        // Check cache first
        {
            let cache = self.expr_cache.read().unwrap();
            if let Some(cached) = cache.get(&key) {
                // Cache hit! Update stats and return cached Arc
                let mut stats = self.stats.write().unwrap();
                stats.cache_hits += 1;
                return Ok(Arc::clone(cached));
            }
        }

        // Cache miss - deserialize and store
        let expr = default_deserialize_physical_expr(proto, ctx, input_schema, self)?;

        // Store in cache
        {
            let mut cache = self.expr_cache.write().unwrap();
            cache.insert(key, Arc::clone(&expr));
            let mut stats = self.stats.write().unwrap();
            stats.cache_misses += 1;
        }

        Ok(expr)
    }
}
