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

//! Macros for the datafusion-datasource crate

/// Helper macro to generate schema adapter methods for FileSource implementations
///
/// Place this inside *any* `impl FileSource for YourType { … }` to
/// avoid copy-pasting `with_schema_adapter_factory` and
/// `schema_adapter_factory`.
///
/// # Availability
///
/// This macro is exported at the crate root level via `#[macro_export]`, so it can be
/// imported directly from the crate:
///
/// ```rust,no_run
/// use datafusion_datasource::impl_schema_adapter_methods;
/// ```
///
/// # Note on path resolution
/// When this macro is used:
/// - `$crate` expands to `datafusion_datasource` (the crate root)
/// - `$crate::file::FileSource` refers to the FileSource trait from this crate
/// - `$crate::schema_adapter::SchemaAdapterFactory` refers to the SchemaAdapterFactory trait
///
/// # Example Usage
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use datafusion_datasource::file::FileSource;
/// use datafusion_datasource::impl_schema_adapter_methods;
/// use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
///
/// struct MyFileSource {
///     schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
///     // other fields...
/// }
///
/// impl FileSource for MyFileSource {
///     // Implement other required methods...
///     
///     // Use the macro to implement schema adapter methods
///     impl_schema_adapter_methods!();
/// }
/// ```
#[macro_export(local_inner_macros)]
macro_rules! impl_schema_adapter_methods {
    () => {
        fn with_schema_adapter_factory(
            &self,
            schema_adapter_factory: std::sync::Arc<
                dyn $crate::schema_adapter::SchemaAdapterFactory,
            >,
        ) -> std::sync::Arc<dyn $crate::file::FileSource> {
            std::sync::Arc::new(Self {
                schema_adapter_factory: Some(schema_adapter_factory),
                ..self.clone()
            })
        }

        fn schema_adapter_factory(
            &self,
        ) -> Option<std::sync::Arc<dyn $crate::schema_adapter::SchemaAdapterFactory>> {
            self.schema_adapter_factory.clone()
        }
    };
}
