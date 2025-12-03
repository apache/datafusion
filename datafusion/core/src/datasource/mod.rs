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

//! DataFusion data sources: [`TableProvider`] and [`ListingTable`]
//!
//! [`ListingTable`]: crate::datasource::listing::ListingTable

pub mod dynamic_file;
pub mod empty;
pub mod file_format;
pub mod listing;
pub mod listing_table_factory;
mod memory_test;
pub mod physical_plan;
pub mod provider;
mod view_test;

// backwards compatibility
pub use self::default_table_source::{
    provider_as_source, source_as_provider, DefaultTableSource,
};
pub use self::memory::MemTable;
pub use self::view::ViewTable;
pub use crate::catalog::TableProvider;
pub use crate::logical_expr::TableType;
pub use datafusion_catalog::cte_worktable;
pub use datafusion_catalog::default_table_source;
pub use datafusion_catalog::memory;
pub use datafusion_catalog::stream;
pub use datafusion_catalog::view;
pub use datafusion_datasource::schema_adapter;
pub use datafusion_datasource::sink;
pub use datafusion_datasource::source;
pub use datafusion_datasource::table_schema;
pub use datafusion_execution::object_store;
pub use datafusion_physical_expr::create_ordering;

#[cfg(all(test, feature = "parquet"))]
mod tests {
    use arrow::{
        datatypes::{DataType, Field, Schema},
    };
    use datafusion_common::record_batch;
    use datafusion_datasource::{
        schema_adapter::DefaultSchemaAdapterFactory,
    };
    use std::sync::Arc;

    #[test]
    fn default_schema_adapter() {
        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        // file has a subset of the table schema fields and different type
        let file_schema = Schema::new(vec![
            Field::new("c", DataType::Float64, true), // not in table schema
            Field::new("b", DataType::Float64, true),
        ]);

        let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
        let (mapper, indices) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(indices, vec![1]);

        let file_batch = record_batch!(("b", Float64, vec![1.0, 2.0])).unwrap();

        let mapped_batch = mapper.map_batch(file_batch).unwrap();

        // the mapped batch has the correct schema and the "b" column has been cast to Utf8
        let expected_batch = record_batch!(
            ("a", Int32, vec![None, None]), // missing column filled with nulls
            ("b", Utf8, vec!["1.0", "2.0"])  // b was cast to string and order was changed
        )
        .unwrap();
        assert_eq!(mapped_batch, expected_batch);
    }

    #[test]
    fn default_schema_adapter_non_nullable_columns() {
        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false), // "a"" is declared non nullable
            Field::new("b", DataType::Utf8, true),
        ]);
        let file_schema = Schema::new(vec![
            // since file doesn't have "a" it will be filled with nulls
            Field::new("b", DataType::Float64, true),
        ]);

        let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
        let (mapper, indices) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(indices, vec![0]);

        let file_batch = record_batch!(("b", Float64, vec![1.0, 2.0])).unwrap();

        // Mapping fails because it tries to fill in a non-nullable column with nulls
        let err = mapper.map_batch(file_batch).unwrap_err().to_string();
        assert!(err.contains("Invalid argument error: Column 'a' is declared as non-nullable but contains null values"), "{err}");
    }
}
