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
//!
//! Example requires git submodules to be initialized in repo as it uses data from
//! the `parquet-testing` repo.
use async_trait::async_trait;
use datafusion::{
    arrow::util::pretty,
    catalog::{
        schema::SchemaProvider,
        {CatalogList, CatalogProvider},
    },
    datasource::{
        file_format::{csv::CsvFormat, parquet::ParquetFormat, FileFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    error::Result,
    execution::context::SessionState,
    prelude::SessionContext,
};
use std::sync::RwLock;
use std::{
    any::Any,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

#[tokio::main]
async fn main() -> Result<()> {
    let repo_dir = std::fs::canonicalize(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            // parent dir of datafusion-examples = repo root
            .join(".."),
    )
    .unwrap();
    let mut ctx = SessionContext::new();
    let state = ctx.state();
    let catlist = Arc::new(CustomCatalogList::new());
    // use our custom catalog list for context. each context has a single catalog list.
    // context will by default have MemoryCatalogList
    ctx.register_catalog_list(catlist.clone());

    // initialize our catalog and schemas
    let catalog = DirCatalog::new();
    let parquet_schema = DirSchema::create(
        &state,
        DirSchemaOpts {
            format: Arc::new(ParquetFormat::default()),
            dir: &repo_dir.join("parquet-testing").join("data"),
            ext: "parquet",
        },
    )
    .await?;
    let csv_schema = DirSchema::create(
        &state,
        DirSchemaOpts {
            format: Arc::new(CsvFormat::default()),
            dir: &repo_dir.join("testing").join("data").join("csv"),
            ext: "csv",
        },
    )
    .await?;
    // register schemas into catalog
    catalog.register_schema("parquet", parquet_schema.clone())?;
    catalog.register_schema("csv", csv_schema.clone())?;
    // register our catalog in the context
    ctx.register_catalog("dircat", Arc::new(catalog));
    {
        // catalog was passed down into our custom catalog list since we overide the ctx's default
        let catalogs = catlist.catalogs.read().unwrap();
        assert!(catalogs.contains_key("dircat"));
    };
    // take the first 5 (arbitrary amount) keys from our schema's hashmap.
    // in our `DirSchema`, the table names are equivalent to their key in the hashmap,
    // so any key in the hashmap will now  be a queryable in our datafusion context.
    let parquet_tables = {
        let tables = parquet_schema.tables.read().unwrap();
        tables.keys().take(5).cloned().collect::<Vec<_>>()
    };
    for table in parquet_tables {
        println!("querying table {table} from parquet schema");
        let df = ctx
            .sql(&format!("select * from dircat.parquet.\"{table}\" "))
            .await?
            .limit(0, Some(5))?;
        let result = df.collect().await;
        match result {
            Ok(batches) => {
                pretty::print_batches(&batches).unwrap();
            }
            Err(e) => {
                println!("table '{table}' query failed due to {e}");
            }
        }
    }
    let table_to_drop = {
        let parquet_tables = parquet_schema.tables.read().unwrap();
        parquet_tables.keys().next().unwrap().to_owned()
    };
    // DDL example
    let df = ctx
        .sql(&format!("DROP TABLE dircat.parquet.\"{table_to_drop}\""))
        .await?;
    df.collect().await?;
    let parquet_tables = parquet_schema.tables.read().unwrap();
    // datafusion has deregistered the table from our schema
    // (called our schema's deregister func)
    assert!(!parquet_tables.contains_key(&table_to_drop));
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
        let listdir = std::fs::read_dir(dir).unwrap();
        for res in listdir {
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
                .await?;
            let table = ListingTable::try_new(conf)?;
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

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
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
        println!("adding table {name}");
        tables.insert(name, table.clone());
        Ok(Some(table))
    }

    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().unwrap();
        println!("dropping table {name}");
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
/// Catalog lists holds multiple catalogs. Each context has a single catalog list.
struct CustomCatalogList {
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}
impl CustomCatalogList {
    fn new() -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
        }
    }
}
impl CatalogList for CustomCatalogList {
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
