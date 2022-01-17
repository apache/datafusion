use crate::error::Result;
use crate::logical_plan::DFSchema;
use crate::physical_plan::ExecutionPlan;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum StreamType {
    Unbounded,
    Bounded,
}

#[async_trait]
pub trait StreamingProvider: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn stream_type(&self) -> StreamType {
        StreamType::Unbounded
    }
    fn schema(&self) -> SchemaRef;
    async fn recv(&self, batch_size: usize) -> Result<Arc<dyn ExecutionPlan>>;
}
