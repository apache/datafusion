use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    error::{DataFusionError, Result},
    execution::{registry::SerializerRegistry, FunctionRegistry, SessionState},
    sql::TableReference,
};

#[async_trait]
pub trait Context: Sync + Send + FunctionRegistry {
    /// Return [SerializerRegistry] for extensions
    fn serializer_registry(&self) -> &Arc<dyn SerializerRegistry>;

    async fn table(
        &self,
        reference: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>>;
}

#[async_trait]
impl Context for SessionState {
    fn serializer_registry(&self) -> &Arc<dyn SerializerRegistry> {
        self.serializer_registry()
    }

    async fn table(
        &self,
        reference: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = reference.table().to_string();
        let schema = self.schema_for_ref(reference.clone())?;
        let table_provider = schema.table(&table).await?;
        Ok(table_provider)
    }
}
