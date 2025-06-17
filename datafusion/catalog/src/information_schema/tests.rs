use std::sync::Arc;

use crate::CatalogProvider;

use super::*;

#[tokio::test]
async fn make_tables_uses_table_type() {
    let config = InformationSchemaConfig {
        catalog_list: Arc::new(Fixture),
    };
    let mut builder = InformationSchemaTablesBuilder {
        catalog_names: StringBuilder::new(),
        schema_names: StringBuilder::new(),
        table_names: StringBuilder::new(),
        table_types: StringBuilder::new(),
        schema: Arc::new(Schema::empty()),
    };

    assert!(config.make_tables(&mut builder).await.is_ok());

    assert_eq!("BASE TABLE", builder.table_types.finish().value(0));
}

#[derive(Debug)]
struct Fixture;

#[async_trait]
impl SchemaProvider for Fixture {
    // InformationSchemaConfig::make_tables should use this.
    async fn table_type(&self, _: &str) -> Result<Option<TableType>> {
        Ok(Some(TableType::Base))
    }

    // InformationSchemaConfig::make_tables used this before `table_type`
    // existed but should not, as it may be expensive.
    async fn table(&self, _: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        panic!("InformationSchemaConfig::make_tables called SchemaProvider::table instead of table_type")
    }

    fn as_any(&self) -> &dyn Any {
        unimplemented!("not required for these tests")
    }

    fn table_names(&self) -> Vec<String> {
        vec!["atable".to_string()]
    }

    fn table_exist(&self, _: &str) -> bool {
        unimplemented!("not required for these tests")
    }
}

impl CatalogProviderList for Fixture {
    fn as_any(&self) -> &dyn Any {
        unimplemented!("not required for these tests")
    }

    fn register_catalog(
        &self,
        _: String,
        _: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unimplemented!("not required for these tests")
    }

    fn catalog_names(&self) -> Vec<String> {
        vec!["acatalog".to_string()]
    }

    fn catalog(&self, _: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(Self))
    }
}

impl CatalogProvider for Fixture {
    fn as_any(&self) -> &dyn Any {
        unimplemented!("not required for these tests")
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["aschema".to_string()]
    }

    fn schema(&self, _: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(Self))
    }
}
