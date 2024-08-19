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

//! Simple example of a catalog/schema implementation.
use async_trait::async_trait;
use datafusion::{
    arrow::util::pretty,
    catalog::{CatalogProvider, CatalogProviderList, SchemaProvider},
    datasource::{
        file_format::{csv::CsvFormat, FileFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    error::Result,
    execution::context::SessionState,
    prelude::SessionContext,
};
use std::sync::RwLock;
use std::{any::Any, collections::HashMap, path::Path, sync::Arc};
use std::{fs::File, io::Write};
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Prepare test directories containing multiple files
    let dir_a = prepare_example_data()?;
    let dir_b = prepare_example_data()?;

    let ctx = SessionContext::new();
    let state = ctx.state();
    let cataloglist = Arc::new(CustomCatalogProviderList::new());

    // use our custom catalog list for context. each context has a single catalog list.
    // context will by default have [`MemoryCatalogProviderList`]
    ctx.register_catalog_list(cataloglist.clone());

    // initialize our catalog and schemas
    let catalog = DirCatalog::new();
    let schema_a = DirSchema::create(
        &state,
        DirSchemaOpts {
            format: Arc::new(CsvFormat::default()),
            dir: dir_a.path(),
            ext: "csv",
        },
    )
    .await?;
    let schema_b = DirSchema::create(
        &state,
        DirSchemaOpts {
            format: Arc::new(CsvFormat::default()),
            dir: dir_b.path(),
            ext: "csv",
        },
    )
    .await?;

    // register schemas into catalog
    catalog.register_schema("schema_a", schema_a.clone())?;
    catalog.register_schema("schema_b", schema_b.clone())?;

    // register our catalog in the context
    ctx.register_catalog("dircat", Arc::new(catalog));
    {
        // catalog was passed down into our custom catalog list since we override the ctx's default
        let catalogs = cataloglist.catalogs.read().unwrap();
        assert!(catalogs.contains_key("dircat"));
    };

    // take the first 3 (arbitrary amount) keys from our schema's hashmap.
    // in our `DirSchema`, the table names are equivalent to their key in the hashmap,
    // so any key in the hashmap will now  be a queryable in our datafusion context.
    let tables = {
        let tables = schema_a.tables.read().unwrap();
        tables.keys().take(3).cloned().collect::<Vec<_>>()
    };
    for table in tables {
        log::info!("querying table {table} from schema_a");
        let df = ctx
            .sql(&format!("select * from dircat.schema_a.\"{table}\" "))
            .await?
            .limit(0, Some(5))?;
        let result = df.collect().await;
        match result {
            Ok(batches) => {
                log::info!("query completed");
                pretty::print_batches(&batches).unwrap();
            }
            Err(e) => {
                log::error!("table '{table}' query failed due to {e}");
            }
        }
    }

    // Select table to drop from registered tables
    let table_to_drop = {
        let tables = schema_a.tables.read().unwrap();
        tables.keys().next().unwrap().to_owned()
    };

    // Execute drop table
    let df: datafusion::prelude::DataFrame = ctx
        .sql(&format!("DROP TABLE dircat.schema_a.\"{table_to_drop}\""))
        .await?;
    df.collect().await?;

    // Ensure that datafusion has deregistered the table from our schema
    // (called our schema's deregister func)
    let tables = schema_a.tables.read().unwrap();
    assert!(!tables.contains_key(&table_to_drop));
    Ok(())
}

struct DirSchemaOpts<'a> {
    ext: &'a str,
    dir: &'a Path,
    format: Arc<dyn FileFormat>,
}
/// Schema where every file with extension `ext` in a given `dir` is a table.
struct DirSchema {
    ext: String,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}
impl DirSchema {
    async fn create(state: &SessionState, opts: DirSchemaOpts<'_>) -> Result<Arc<Self>> {
        let DirSchemaOpts { ext, dir, format } = opts;
        let mut tables = HashMap::new();
        let direntries = std::fs::read_dir(dir).unwrap();
        for res in direntries {
            let entry = res.unwrap();
            let filename = entry.file_name().to_str().unwrap().to_string();
            if !filename.ends_with(ext) {
                continue;
            }

            let table_path = ListingTableUrl::parse(entry.path().to_str().unwrap())?;
            let opts = ListingOptions::new(format.clone());
            let conf = ListingTableConfig::new(table_path)
                .with_listing_options(opts)
                .infer_schema(state)
                .await;

            if let Err(err) = conf {
                log::error!("Error while inferring schema for {filename}: {err}");
                continue;
            }

            let table = ListingTable::try_new(conf?)?;
            tables.insert(filename, Arc::new(table) as Arc<dyn TableProvider>);
        }
        Ok(Arc::new(Self {
            tables: RwLock::new(tables),
            ext: ext.to_string(),
        }))
    }
    #[allow(unused)]
    fn name(&self) -> &str {
        &self.ext
    }
}

#[async_trait]
impl SchemaProvider for DirSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect::<Vec<_>>()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let tables = self.tables.read().unwrap();
        Ok(tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(name)
    }
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().unwrap();
        log::info!("adding table {name}");
        tables.insert(name, table.clone());
        Ok(Some(table))
    }

    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().unwrap();
        log::info!("dropping table {name}");
        Ok(tables.remove(name))
    }
}
/// Catalog holds multiple schemas
struct DirCatalog {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}
impl DirCatalog {
    fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }
}
impl CatalogProvider for DirCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schema_map = self.schemas.write().unwrap();
        schema_map.insert(name.to_owned(), schema.clone());
        Ok(Some(schema))
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read().unwrap();
        let maybe_schema = schemas.get(name);
        if let Some(schema) = maybe_schema {
            let schema = schema.clone() as Arc<dyn SchemaProvider>;
            Some(schema)
        } else {
            None
        }
    }
}
/// Catalog lists holds multiple catalog providers. Each context has a single catalog list.
struct CustomCatalogProviderList {
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}
impl CustomCatalogProviderList {
    fn new() -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
        }
    }
}
impl CatalogProviderList for CustomCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let mut cats = self.catalogs.write().unwrap();
        cats.insert(name, catalog.clone());
        Some(catalog)
    }

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String> {
        let cats = self.catalogs.read().unwrap();
        cats.keys().cloned().collect()
    }

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let cats = self.catalogs.read().unwrap();
        cats.get(name).cloned()
    }
}

fn prepare_example_data() -> Result<TempDir> {
    let dir = tempfile::tempdir()?;
    let path = dir.path();

    let content = r#"key,value
1,foo
2,bar
3,baz"#;

    for i in 0..5 {
        let mut file = File::create(path.join(format!("{}.csv", i)))?;
        file.write_all(content.as_bytes())?;
    }

    Ok(dir)
}
