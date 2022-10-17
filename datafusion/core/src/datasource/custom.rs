use crate::datasource::TableProvider;
use crate::execution::context::SessionState;
use crate::physical_plan::ExecutionPlan;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_expr::{Expr, TableType};
use std::any::Any;

use std::collections::HashMap;
use std::sync::Arc;

/// Exists to pass table_type, path, and options from parsing to serialization
pub struct CustomTable {
    /// File/Table type used for factory to create TableProvider
    table_type: String,
    /// Path used for factory to create TableProvider
    path: String,
    /// Options used for factory to create TableProvider
    options: HashMap<String, String>,
    // TableProvider that was created
    provider: Arc<dyn TableProvider>,
}

impl CustomTable {
    pub fn new(
        table_type: &str,
        path: &str,
        options: HashMap<String, String>,
        provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            table_type: table_type.to_string(),
            path: path.to_string(),
            options,
            provider,
        }
    }

    pub fn get_table_type(&self) -> String {
        self.table_type.clone()
    }

    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    pub fn get_options(&self) -> HashMap<String, String> {
        self.options.clone()
    }

}

#[async_trait]
impl TableProvider for CustomTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.provider.schema()
    }

    fn table_type(&self) -> TableType {
        self.provider.table_type()
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.provider.scan(ctx, projection, filters, limit).await
    }
}
