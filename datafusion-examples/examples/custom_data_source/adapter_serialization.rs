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
//! interception methods (`serialize_physical_plan` and `deserialize_physical_plan`)
//! to implement custom serialization logic.
//!
//! The key insight is that `FileScanConfig::expr_adapter_factory` is NOT serialized by
//! default. This example shows how to:
//! 1. Detect plans with custom adapters during serialization
//! 2. Wrap them as Extension nodes with JSON-serialized adapter metadata
//! 3. Store the inner DataSourceExec (without adapter) as a child in the extension's inputs field
//! 4. Unwrap and restore the adapter during deserialization
//!
//! This demonstrates nested serialization (protobuf outer, JSON inner) and the power
//! of the `PhysicalExtensionCodec` interception pattern. Both plan and expression
//! serialization route through the codec, enabling interception at every node in the tree.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::assert_batches_eq;
use datafusion::common::{Result, not_impl_err};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter,
    physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::from_proto::parse_physical_expr_with_converter;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::{
    PhysicalExprNode, PhysicalExtensionNode, PhysicalPlanNode,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};

/// Example showing how to preserve custom adapter information during plan serialization.
///
/// This demonstrates:
/// 1. Creating a custom PhysicalExprAdapter with metadata
/// 2. Using PhysicalExtensionCodec to intercept serialization
/// 3. Wrapping adapter info as Extension nodes
/// 4. Restoring adapters during deserialization
pub async fn adapter_serialization() -> Result<()> {
    println!("=== PhysicalExprAdapter Serialization Example ===\n");

    // Step 1: Create sample Parquet data in memory
    println!("Step 1: Creating sample Parquet data...");
    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let batch = record_batch!(("id", Int32, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))?;
    let path = Path::from("data.parquet");
    write_parquet(&store, &path, &batch).await?;

    // Step 2: Set up session with custom adapter
    println!("Step 2: Setting up session with custom adapter...");
    let logical_schema =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let mut cfg = SessionConfig::new();
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::clone(&store),
    );

    // Create a table with our custom MetadataAdapterFactory
    let adapter_factory = Arc::new(MetadataAdapterFactory::new("v1"));
    let listing_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///data.parquet")?)
            .infer_options(&ctx.state())
            .await?
            .with_schema(logical_schema)
            .with_expr_adapter_factory(
                Arc::clone(&adapter_factory) as Arc<dyn PhysicalExprAdapterFactory>
            );
    let table = ListingTable::try_new(listing_config)?;
    ctx.register_table("my_table", Arc::new(table))?;

    // Step 3: Create physical plan with filter
    println!("Step 3: Creating physical plan with filter...");
    let df = ctx.sql("SELECT * FROM my_table WHERE id > 5").await?;
    let original_plan = df.create_physical_plan().await?;

    // Verify adapter is present in original plan
    let has_adapter_before = verify_adapter_in_plan(&original_plan, "original");
    println!("  Original plan has adapter: {has_adapter_before}");

    // Step 4: Serialize with our custom codec
    println!("\nStep 4: Serializing plan with AdapterPreservingCodec...");
    let codec = AdapterPreservingCodec;
    let bytes = physical_plan_to_bytes_with_proto_converter(
        Arc::clone(&original_plan),
        &codec,
        &codec,
    )?;
    println!("  Serialized {} bytes", bytes.len());
    println!("  (DataSourceExec with adapter was wrapped as PhysicalExtensionNode)");

    // Step 5: Deserialize with our custom codec
    println!("\nStep 5: Deserializing plan with AdapterPreservingCodec...");
    let task_ctx = ctx.task_ctx();
    let restored_plan =
        physical_plan_from_bytes_with_proto_converter(&bytes, &task_ctx, &codec, &codec)?;

    // Verify adapter is restored
    let has_adapter_after = verify_adapter_in_plan(&restored_plan, "restored");
    println!("  Restored plan has adapter: {has_adapter_after}");

    // Step 6: Execute and compare results
    println!("\nStep 6: Executing plans and comparing results...");
    let original_results =
        datafusion::physical_plan::collect(Arc::clone(&original_plan), task_ctx.clone())
            .await?;
    let restored_results =
        datafusion::physical_plan::collect(restored_plan, task_ctx).await?;

    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| id |",
        "+----+",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "| 10 |",
        "+----+",
    ];

    println!("\n  Original plan results:");
    arrow::util::pretty::print_batches(&original_results)?;
    assert_batches_eq!(expected, &original_results);

    println!("\n  Restored plan results:");
    arrow::util::pretty::print_batches(&restored_results)?;
    assert_batches_eq!(expected, &restored_results);

    println!("\n=== Example Complete! ===");
    println!("Key takeaways:");
    println!(
        "  1. PhysicalExtensionCodec provides serialize_physical_plan/deserialize_physical_plan hooks"
    );
    println!("  2. Custom metadata can be wrapped as PhysicalExtensionNode");
    println!("  3. Nested serialization (protobuf + JSON) works seamlessly");
    println!(
        "  4. Both plans produce identical results despite serialization round-trip"
    );
    println!("  5. Adapters are fully preserved through the serialization round-trip");

    Ok(())
}

// ============================================================================
// MetadataAdapter - A simple custom adapter with a tag
// ============================================================================

/// A custom PhysicalExprAdapter that wraps another adapter.
/// The tag metadata is stored in the factory, not the adapter itself.
#[derive(Debug)]
struct MetadataAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for MetadataAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // Simply delegate to inner adapter
        self.inner.rewrite(expr)
    }
}

// ============================================================================
// MetadataAdapterFactory - Factory for creating MetadataAdapter instances
// ============================================================================

/// Factory for creating MetadataAdapter instances.
/// The tag is stored in the factory and extracted via Debug formatting in `extract_adapter_tag`.
#[derive(Debug)]
struct MetadataAdapterFactory {
    // Note: This field is read via Debug formatting in `extract_adapter_tag`.
    // Rust's dead code analysis doesn't recognize Debug-based field access.
    // In PR #19234, this field is used by `with_partition_values`, but that method
    // doesn't exist in upstream DataFusion's PhysicalExprAdapter trait.
    #[expect(dead_code)]
    tag: String,
}

impl MetadataAdapterFactory {
    fn new(tag: impl Into<String>) -> Self {
        Self { tag: tag.into() }
    }
}

impl PhysicalExprAdapterFactory for MetadataAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let inner = DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, physical_file_schema)?;
        Ok(Arc::new(MetadataAdapter { inner }))
    }
}

// ============================================================================
// AdapterPreservingCodec - Custom codec that preserves adapters
// ============================================================================

/// Extension payload structure for serializing adapter info
#[derive(Serialize, Deserialize)]
struct ExtensionPayload {
    /// Marker to identify this is our custom extension
    marker: String,
    /// JSON-serialized adapter metadata
    adapter_metadata: AdapterMetadata,
}

/// Metadata about the adapter to recreate it during deserialization
#[derive(Serialize, Deserialize)]
struct AdapterMetadata {
    /// The adapter tag (e.g., "v1")
    tag: String,
}

const EXTENSION_MARKER: &str = "adapter_preserving_extension_v1";

/// A codec that intercepts serialization to preserve adapter information.
#[derive(Debug)]
struct AdapterPreservingCodec;

impl PhysicalExtensionCodec for AdapterPreservingCodec {
    // Required method: decode custom extension nodes
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Try to parse as our extension payload
        if let Ok(payload) = serde_json::from_slice::<ExtensionPayload>(buf)
            && payload.marker == EXTENSION_MARKER
        {
            if inputs.len() != 1 {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "Extension node expected exactly 1 child, got {}",
                    inputs.len()
                )));
            }
            let inner_plan = inputs[0].clone();

            // Recreate the adapter factory
            let adapter_factory = create_adapter_factory(&payload.adapter_metadata.tag);

            // Inject adapter into the plan
            return inject_adapter_into_plan(inner_plan, adapter_factory);
        }

        not_impl_err!("Unknown extension type")
    }

    // Required method: encode custom execution plans
    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        // We don't need this for the example - we use serialize_physical_plan instead
        not_impl_err!(
            "try_encode not used - adapter wrapping happens in serialize_physical_plan"
        )
    }
}

impl PhysicalProtoConverterExtension for AdapterPreservingCodec {
    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        // Check if this is a DataSourceExec with adapter
        if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>()
            && let Some(config) =
                exec.data_source().as_any().downcast_ref::<FileScanConfig>()
            && let Some(adapter_factory) = &config.expr_adapter_factory
            && let Some(tag) = extract_adapter_tag(adapter_factory.as_ref())
        {
            // Try to extract our MetadataAdapterFactory's tag
            println!("    [Serialize] Found DataSourceExec with adapter tag: {tag}");

            // 1. Create adapter metadata
            let adapter_metadata = AdapterMetadata { tag };

            // 2. Serialize the inner plan to protobuf
            //    Note that this will drop the custom adapter since the default serialization cannot handle it
            let inner_proto = PhysicalPlanNode::try_from_physical_plan_with_converter(
                Arc::clone(plan),
                extension_codec,
                self,
            )?;

            // 3. Create extension payload to wrap the plan
            //    so that the custom adapter gets re-attached during deserialization
            //    The choice of JSON is arbitrary; other formats could be used.
            let payload = ExtensionPayload {
                marker: EXTENSION_MARKER.to_string(),
                adapter_metadata,
            };
            let payload_bytes = serde_json::to_vec(&payload).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Failed to serialize payload: {e}"
                ))
            })?;

            // 4. Return as PhysicalExtensionNode with child plan in inputs
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Extension(
                    PhysicalExtensionNode {
                        node: payload_bytes,
                        inputs: vec![inner_proto],
                    },
                )),
            });
        }

        // No adapter found, not a DataSourceExec, etc. - use default serialization
        PhysicalPlanNode::try_from_physical_plan_with_converter(
            Arc::clone(plan),
            extension_codec,
            self,
        )
    }

    // Interception point: override deserialization to unwrap adapters
    fn proto_to_execution_plan(
        &self,
        ctx: &TaskContext,
        extension_codec: &dyn PhysicalExtensionCodec,
        proto: &PhysicalPlanNode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check if this is our custom extension wrapper
        if let Some(PhysicalPlanType::Extension(extension)) = &proto.physical_plan_type
            && let Ok(payload) =
                serde_json::from_slice::<ExtensionPayload>(&extension.node)
            && payload.marker == EXTENSION_MARKER
        {
            println!(
                "    [Deserialize] Found adapter extension with tag: {}",
                payload.adapter_metadata.tag
            );

            // Get the inner plan proto from inputs field
            if extension.inputs.is_empty() {
                return Err(datafusion::error::DataFusionError::Plan(
                    "Extension node missing child plan in inputs".to_string(),
                ));
            }
            let inner_proto = &extension.inputs[0];

            // Deserialize the inner plan
            let inner_plan = inner_proto.try_into_physical_plan_with_converter(
                ctx,
                extension_codec,
                self,
            )?;

            // Recreate the adapter factory
            let adapter_factory = create_adapter_factory(&payload.adapter_metadata.tag);

            // Inject adapter into the plan
            return inject_adapter_into_plan(inner_plan, adapter_factory);
        }

        // Not our extension - use default deserialization
        proto.try_into_physical_plan_with_converter(ctx, extension_codec, self)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        ctx: &TaskContext,
        input_schema: &Schema,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        parse_physical_expr_with_converter(proto, ctx, input_schema, codec, self)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        serialize_physical_expr_with_converter(expr, codec, self)
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Write a RecordBatch to Parquet in the object store
async fn write_parquet(
    store: &dyn ObjectStore,
    path: &Path,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    let mut buf = vec![];
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;

    let payload = PutPayload::from_bytes(buf.into());
    store.put(path, payload).await?;
    Ok(())
}

/// Extract the tag from a MetadataAdapterFactory.
///
/// Note: Since `PhysicalExprAdapterFactory` doesn't provide `as_any()` for downcasting,
/// we parse the Debug output. In a production system, you might add a dedicated trait
/// method for metadata extraction.
fn extract_adapter_tag(factory: &dyn PhysicalExprAdapterFactory) -> Option<String> {
    let debug_str = format!("{factory:?}");
    if debug_str.contains("MetadataAdapterFactory") {
        // Extract tag from debug output: MetadataAdapterFactory { tag: "v1" }
        if let Some(start) = debug_str.find("tag: \"") {
            let after_tag = &debug_str[start + 6..];
            if let Some(end) = after_tag.find('"') {
                return Some(after_tag[..end].to_string());
            }
        }
    }
    None
}

/// Create an adapter factory from a tag
fn create_adapter_factory(tag: &str) -> Arc<dyn PhysicalExprAdapterFactory> {
    Arc::new(MetadataAdapterFactory::new(tag))
}

/// Inject an adapter into a plan (assumes plan is a DataSourceExec with FileScanConfig)
fn inject_adapter_into_plan(
    plan: Arc<dyn ExecutionPlan>,
    adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>()
        && let Some(config) = exec.data_source().as_any().downcast_ref::<FileScanConfig>()
    {
        let new_config = FileScanConfigBuilder::from(config.clone())
            .with_expr_adapter(Some(adapter_factory))
            .build();
        return Ok(DataSourceExec::from_data_source(new_config));
    }
    // If not a DataSourceExec with FileScanConfig, return as-is
    Ok(plan)
}

/// Helper to verify if a plan has an adapter (for testing/validation)
fn verify_adapter_in_plan(plan: &Arc<dyn ExecutionPlan>, label: &str) -> bool {
    // Walk the plan tree to find DataSourceExec with adapter
    fn check_plan(plan: &dyn ExecutionPlan) -> bool {
        if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>()
            && let Some(config) =
                exec.data_source().as_any().downcast_ref::<FileScanConfig>()
            && config.expr_adapter_factory.is_some()
        {
            return true;
        }
        // Check children
        for child in plan.children() {
            if check_plan(child.as_ref()) {
                return true;
            }
        }
        false
    }

    let has_adapter = check_plan(plan.as_ref());
    println!("    [Verify] {label} plan adapter check: {has_adapter}");
    has_adapter
}
