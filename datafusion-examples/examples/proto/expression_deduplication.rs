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
use datafusion::physical_plan::expressions::{BinaryExpr, col};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr_with_converter;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use prost::Message;

/// Example showing how to implement expression deduplication using the codec decorator pattern.
///
/// This demonstrates:
/// 1. Creating a CachingCodec that caches expressions by their protobuf bytes
/// 2. Intercepting deserialization to return cached Arcs for duplicate expressions
/// 3. Verifying that duplicate expressions share the same Arc after deserialization
///
/// Deduplication is keyed by the protobuf bytes representing the expression,
/// in reality deduplication could be done based on e.g. the pointer address of the
/// serialized expression in memory, but this is simpler to demonstrate.
///
/// In this case our expression is trivial and just for demonstration purposes.
/// In real scenarios, expressions can be much more complex, e.g. a large InList
/// expression could be megabytes in size, so deduplication can save significant memory
/// in addition to more correctly representing the original plan structure.
pub async fn expression_deduplication() -> Result<()> {
    println!("=== Expression Deduplication Example ===\n");

    // Create a schema for our test expressions
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));

    // Step 1: Create expressions with duplicates
    println!("Step 1: Creating expressions with duplicates...");

    // Create expression: col("a")
    let a = col("a", &schema)?;

    // Create a clone to show duplicates
    let a_clone = Arc::clone(&a);

    // Combine: a OR a_clone
    let combined_expr =
        Arc::new(BinaryExpr::new(a, Operator::Or, a_clone)) as Arc<dyn PhysicalExpr>;
    println!("  Created expression: a OR a with duplicates");
    println!("  Note: a appears twice in the expression tree\n");
    // Step 2: Create a filter plan with this expression
    println!("Step 2: Creating physical plan with the expression...");

    let input = Arc::new(PlaceholderRowExec::new(Arc::clone(&schema)));
    let filter_plan: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(combined_expr, input)?);

    println!("  Created FilterExec with duplicate sub-expressions\n");

    // Step 3: Serialize with the caching codec
    println!("Step 3: Serializing plan...");

    let extension_codec = DefaultPhysicalExtensionCodec {};
    let caching_converter = CachingCodec::new();
    let proto =
        caching_converter.execution_plan_to_proto(&filter_plan, &extension_codec)?;

    // Serialize to bytes
    let mut bytes = Vec::new();
    proto.encode(&mut bytes).unwrap();
    println!("  Serialized plan to {} bytes\n", bytes.len());

    // Step 4: Deserialize with the caching codec
    println!("Step 4: Deserializing plan with CachingCodec...");

    let ctx = SessionContext::new();
    let deserialized_plan = proto.try_into_physical_plan_with_converter(
        &ctx.task_ctx(),
        &extension_codec,
        &caching_converter,
    )?;

    // Step 5: check that we deduplicated expressions
    println!("Step 5: Checking for deduplicated expressions...");
    let Some(filter_exec) = deserialized_plan.as_any().downcast_ref::<FilterExec>()
    else {
        panic!("Deserialized plan is not a FilterExec");
    };
    let predicate = Arc::clone(filter_exec.predicate());
    let binary_expr = predicate
        .as_any()
        .downcast_ref::<BinaryExpr>()
        .expect("Predicate is not a BinaryExpr");
    let left = &binary_expr.left();
    let right = &binary_expr.right();
    // Check if left and right point to the same Arc
    let deduplicated = Arc::ptr_eq(left, right);
    if deduplicated {
        println!("  Success: Duplicate expressions were deduplicated!");
        println!(
            "  Cache Stats: hits={}, misses={}",
            caching_converter.stats.read().unwrap().cache_hits,
            caching_converter.stats.read().unwrap().cache_misses,
        );
    } else {
        println!("  Failure: Duplicate expressions were NOT deduplicated.");
    }

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
}

impl PhysicalProtoConverterExtension for CachingCodec {
    fn proto_to_execution_plan(
        &self,
        ctx: &TaskContext,
        extension_codec: &dyn PhysicalExtensionCodec,
        proto: &PhysicalPlanNode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        proto.try_into_physical_plan_with_converter(ctx, extension_codec, self)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        PhysicalPlanNode::try_from_physical_plan_with_converter(
            Arc::clone(plan),
            extension_codec,
            self,
        )
    }

    // CACHING IMPLEMENTATION: Intercept expression deserialization
    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        ctx: &TaskContext,
        input_schema: &Schema,
        codec: &dyn PhysicalExtensionCodec,
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
        let expr =
            parse_physical_expr_with_converter(proto, ctx, input_schema, codec, self)?;

        // Store in cache
        {
            let mut cache = self.expr_cache.write().unwrap();
            cache.insert(key, Arc::clone(&expr));
            let mut stats = self.stats.write().unwrap();
            stats.cache_misses += 1;
        }

        Ok(expr)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        serialize_physical_expr_with_converter(expr, codec, self)
    }
}
