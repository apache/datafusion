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

use std::{collections::HashMap, sync::Arc};

use datafusion_common::{
    Result, ScalarValue,
    config::{ConfigExtension, ConfigNonZeroUsize, ConfigOptions, SpillCompression},
    extensions::Extensions,
};

/// Configuration options for [`SessionContext`].
///
/// Can be passed to [`SessionContext::new_with_config`] to customize the configuration of DataFusion.
///
/// Options can be set using namespaces keys with `.` as the separator, where the
/// namespace determines which configuration struct the value to routed to. All
/// built-in options are under the `datafusion` namespace.
///
/// For example, the key `datafusion.execution.batch_size` will set [ExecutionOptions::batch_size][datafusion_common::config::ExecutionOptions::batch_size],
/// because [ConfigOptions::execution] is [ExecutionOptions][datafusion_common::config::ExecutionOptions]. Similarly, the key
/// `datafusion.execution.parquet.pushdown_filters` will set [ParquetOptions::pushdown_filters][datafusion_common::config::ParquetOptions::pushdown_filters],
/// since [ExecutionOptions::parquet][datafusion_common::config::ExecutionOptions::parquet] is [ParquetOptions][datafusion_common::config::ParquetOptions].
///
/// Some options have convenience methods. For example [SessionConfig::with_batch_size] is
/// shorthand for setting `datafusion.execution.batch_size`.
///
/// ```
/// use datafusion_common::ScalarValue;
/// use datafusion_execution::config::SessionConfig;
///
/// let config = SessionConfig::new()
///     .set(
///         "datafusion.execution.batch_size",
///         &ScalarValue::UInt64(Some(1234)),
///     )
///     .set_bool("datafusion.execution.parquet.pushdown_filters", true);
///
/// assert_eq!(config.batch_size(), 1234);
/// assert_eq!(config.options().execution.batch_size.get(), 1234);
/// assert_eq!(config.options().execution.parquet.pushdown_filters, true);
/// ```
///
/// You can also directly mutate the options via [SessionConfig::options_mut].
/// So the following is equivalent to the above:
///
/// ```
/// # use datafusion_execution::config::SessionConfig;
/// # use datafusion_common::config::ConfigNonZeroUsize;
/// #
/// let mut config = SessionConfig::new();
/// config.options_mut().execution.batch_size = ConfigNonZeroUsize::try_new(1234)?;
/// config.options_mut().execution.parquet.pushdown_filters = true;
/// #
/// # assert_eq!(config.batch_size(), 1234);
/// # assert_eq!(config.options().execution.batch_size.get(), 1234);
/// # assert_eq!(config.options().execution.parquet.pushdown_filters, true);
/// # datafusion_common::Result::<()>::Ok(())
/// ```
///
/// ## Built-in options
///
/// | Namespace | Config struct |
/// | --------- | ------------- |
/// | `datafusion.catalog` | [CatalogOptions][datafusion_common::config::CatalogOptions] |
/// | `datafusion.execution` | [ExecutionOptions][datafusion_common::config::ExecutionOptions] |
/// | `datafusion.execution.parquet` | [ParquetOptions][datafusion_common::config::ParquetOptions] |
/// | `datafusion.optimizer` | [OptimizerOptions][datafusion_common::config::OptimizerOptions] |
/// | `datafusion.sql_parser` | [SqlParserOptions][datafusion_common::config::SqlParserOptions] |
/// | `datafusion.explain` | [ExplainOptions][datafusion_common::config::ExplainOptions] |
///
/// ## Custom configuration
///
/// Configuration options can be extended. See [SessionConfig::with_extension] for details.
///
/// [`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
/// [`SessionContext::new_with_config`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.new_with_config
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Configuration options for the current session.
    ///
    /// A new copy is created on write, if there are other outstanding
    /// references to the same options.
    options: Arc<ConfigOptions>,
    /// Opaque extensions, keyed by concrete Rust type. See
    /// [`with_extension`](Self::with_extension) and
    /// [`get_extension`](Self::get_extension).
    extensions: Extensions,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            options: Arc::new(ConfigOptions::new()),
            extensions: Extensions::new(),
        }
    }
}

impl SessionConfig {
    /// Create an execution config with default setting
    pub fn new() -> Self {
        Default::default()
    }

    /// Create an execution config with config options read from the environment
    ///
    /// See [`ConfigOptions::from_env`] for details on how environment variables
    /// are mapped to config options.
    pub fn from_env() -> Result<Self> {
        Ok(ConfigOptions::from_env()?.into())
    }

    /// Create new ConfigOptions struct, taking values from a string hash map.
    pub fn from_string_hash_map(settings: &HashMap<String, String>) -> Result<Self> {
        Ok(ConfigOptions::from_string_hash_map(settings)?.into())
    }

    /// Return a handle to the configuration options.
    ///
    /// Can be used to read the current configuration.
    ///
    /// ```
    /// use datafusion_execution::config::SessionConfig;
    ///
    /// let config = SessionConfig::new();
    /// assert!(config.options().execution.batch_size.get() > 0);
    /// ```
    pub fn options(&self) -> &Arc<ConfigOptions> {
        &self.options
    }

    /// Return a mutable handle to the configuration options.
    ///
    /// Can be used to set configuration options.
    ///
    /// ```
    /// use datafusion_common::config::ConfigNonZeroUsize;
    /// use datafusion_execution::config::SessionConfig;
    ///
    /// let mut config = SessionConfig::new();
    /// config.options_mut().execution.batch_size = ConfigNonZeroUsize::try_new(1024)?;
    /// assert_eq!(config.options().execution.batch_size.get(), 1024);
    /// # datafusion_common::Result::<()>::Ok(())
    /// ```
    pub fn options_mut(&mut self) -> &mut ConfigOptions {
        Arc::make_mut(&mut self.options)
    }

    /// Set a configuration option
    pub fn set(self, key: &str, value: &ScalarValue) -> Self {
        self.set_str(key, &value.to_string())
    }

    /// Set a boolean configuration option
    pub fn set_bool(self, key: &str, value: bool) -> Self {
        self.set_str(key, &value.to_string())
    }

    /// Set a generic `u64` configuration option
    pub fn set_u64(self, key: &str, value: u64) -> Self {
        self.set_str(key, &value.to_string())
    }

    /// Set a generic `usize` configuration option
    pub fn set_usize(self, key: &str, value: usize) -> Self {
        self.set_str(key, &value.to_string())
    }

    /// Set a generic `str` configuration option
    pub fn set_str(mut self, key: &str, value: &str) -> Self {
        self.options_mut().set(key, value).unwrap();
        self
    }

    /// Set [`batch_size`]
    ///
    /// # Panics
    ///
    /// Panics if `n` is zero
    ///
    /// [`batch_size`]: datafusion_common::config::ExecutionOptions::batch_size
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.options_mut().execution.batch_size =
            ConfigNonZeroUsize::try_new(n).expect("batch size must be greater than zero");
        self
    }

    /// Set [`target_partitions`]
    ///
    /// If `n` is zero, the default value is used instead
    ///
    /// [`target_partitions`]: datafusion_common::config::ExecutionOptions::target_partitions
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        self.options_mut().execution.target_partitions = if n == 0 {
            datafusion_common::config::ExecutionOptions::default().target_partitions
        } else {
            n
        };
        self
    }

    /// Insert new [ConfigExtension]
    pub fn with_option_extension<T: ConfigExtension>(mut self, extension: T) -> Self {
        self.options_mut().extensions.insert(extension);
        self
    }

    /// Get [`target_partitions`]
    ///
    /// [`target_partitions`]: datafusion_common::config::ExecutionOptions::target_partitions
    pub fn target_partitions(&self) -> usize {
        self.options.execution.target_partitions
    }

    /// Get [`information_schema`]
    ///
    /// [`information_schema`]: datafusion_common::config::CatalogOptions::information_schema
    pub fn information_schema(&self) -> bool {
        self.options.catalog.information_schema
    }

    /// Get [`create_default_catalog_and_schema`]
    ///
    /// [`create_default_catalog_and_schema`]: datafusion_common::config::CatalogOptions::create_default_catalog_and_schema
    pub fn create_default_catalog_and_schema(&self) -> bool {
        self.options.catalog.create_default_catalog_and_schema
    }

    /// Get [`repartition_joins`]
    ///
    /// [`repartition_joins`]: datafusion_common::config::OptimizerOptions::repartition_joins
    pub fn repartition_joins(&self) -> bool {
        self.options.optimizer.repartition_joins
    }

    /// Get [`repartition_aggregations`]
    ///
    /// [`repartition_aggregations`]: datafusion_common::config::OptimizerOptions::repartition_aggregations
    pub fn repartition_aggregations(&self) -> bool {
        self.options.optimizer.repartition_aggregations
    }

    /// Get [`repartition_windows`]
    ///
    /// [`repartition_windows`]: datafusion_common::config::OptimizerOptions::repartition_windows
    pub fn repartition_window_functions(&self) -> bool {
        self.options.optimizer.repartition_windows
    }

    /// Get [`repartition_sorts`]
    ///
    /// [`repartition_sorts`]: datafusion_common::config::OptimizerOptions::repartition_sorts
    pub fn repartition_sorts(&self) -> bool {
        self.options.optimizer.repartition_sorts
    }

    /// Get [`prefer_existing_sort`]
    ///
    /// [`prefer_existing_sort`]: datafusion_common::config::OptimizerOptions::prefer_existing_sort
    pub fn prefer_existing_sort(&self) -> bool {
        self.options.optimizer.prefer_existing_sort
    }

    /// Get [`collect_statistics`]
    ///
    /// [`collect_statistics`]: datafusion_common::config::ExecutionOptions::collect_statistics
    pub fn collect_statistics(&self) -> bool {
        self.options.execution.collect_statistics
    }

    /// Get [`spill_compression`]
    ///
    /// [`spill_compression`]: datafusion_common::config::ExecutionOptions::spill_compression
    pub fn spill_compression(&self) -> SpillCompression {
        self.options.execution.spill_compression
    }

    /// Set [`default_catalog`] and [`default_schema`]
    ///
    /// [`default_catalog`]: datafusion_common::config::CatalogOptions::default_catalog
    /// [`default_schema`]: datafusion_common::config::CatalogOptions::default_schema
    pub fn with_default_catalog_and_schema(
        mut self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
    ) -> Self {
        self.options_mut().catalog.default_catalog = catalog.into();
        self.options_mut().catalog.default_schema = schema.into();
        self
    }

    /// Set [`create_default_catalog_and_schema`]
    ///
    /// [`create_default_catalog_and_schema`]: datafusion_common::config::CatalogOptions::create_default_catalog_and_schema
    pub fn with_create_default_catalog_and_schema(mut self, create: bool) -> Self {
        self.options_mut().catalog.create_default_catalog_and_schema = create;
        self
    }

    /// Set [`information_schema`]
    ///
    /// [`information_schema`]: datafusion_common::config::CatalogOptions::information_schema
    pub fn with_information_schema(mut self, enabled: bool) -> Self {
        self.options_mut().catalog.information_schema = enabled;
        self
    }

    /// Set [`repartition_joins`]
    ///
    /// [`repartition_joins`]: datafusion_common::config::OptimizerOptions::repartition_joins
    pub fn with_repartition_joins(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_joins = enabled;
        self
    }

    /// Set [`repartition_aggregations`]
    ///
    /// [`repartition_aggregations`]: datafusion_common::config::OptimizerOptions::repartition_aggregations
    pub fn with_repartition_aggregations(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_aggregations = enabled;
        self
    }

    /// Set [`repartition_file_min_size`]
    ///
    /// [`repartition_file_min_size`]: datafusion_common::config::OptimizerOptions::repartition_file_min_size
    pub fn with_repartition_file_min_size(mut self, size: usize) -> Self {
        self.options_mut().optimizer.repartition_file_min_size = size;
        self
    }

    /// Set [`allow_symmetric_joins_without_pruning`]
    ///
    /// [`allow_symmetric_joins_without_pruning`]: datafusion_common::config::OptimizerOptions::allow_symmetric_joins_without_pruning
    pub fn with_allow_symmetric_joins_without_pruning(mut self, enabled: bool) -> Self {
        self.options_mut()
            .optimizer
            .allow_symmetric_joins_without_pruning = enabled;
        self
    }

    /// Set [`repartition_file_scans`]
    ///
    /// [`repartition_file_scans`]: datafusion_common::config::OptimizerOptions::repartition_file_scans
    pub fn with_repartition_file_scans(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_file_scans = enabled;
        self
    }

    /// Set [`repartition_windows`]
    ///
    /// [`repartition_windows`]: datafusion_common::config::OptimizerOptions::repartition_windows
    pub fn with_repartition_windows(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_windows = enabled;
        self
    }

    /// Set [`repartition_sorts`]
    ///
    /// [`repartition_sorts`]: datafusion_common::config::OptimizerOptions::repartition_sorts
    pub fn with_repartition_sorts(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_sorts = enabled;
        self
    }

    /// Set [`prefer_existing_sort`]
    ///
    /// [`prefer_existing_sort`]: datafusion_common::config::OptimizerOptions::prefer_existing_sort
    pub fn with_prefer_existing_sort(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.prefer_existing_sort = enabled;
        self
    }

    /// Set [`prefer_existing_union`]
    ///
    /// [`prefer_existing_union`]: datafusion_common::config::OptimizerOptions::prefer_existing_union
    pub fn with_prefer_existing_union(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.prefer_existing_union = enabled;
        self
    }

    /// Set [`pruning`]
    ///
    /// [`pruning`]: datafusion_common::config::ParquetOptions::pruning
    pub fn with_parquet_pruning(mut self, enabled: bool) -> Self {
        self.options_mut().execution.parquet.pruning = enabled;
        self
    }

    /// Get [`pruning`]
    ///
    /// [`pruning`]: datafusion_common::config::ParquetOptions::pruning
    pub fn parquet_pruning(&self) -> bool {
        self.options.execution.parquet.pruning
    }

    /// Get [`bloom_filter_on_read`]
    ///
    /// [`bloom_filter_on_read`]: datafusion_common::config::ParquetOptions::bloom_filter_on_read
    pub fn parquet_bloom_filter_pruning(&self) -> bool {
        self.options.execution.parquet.bloom_filter_on_read
    }

    /// Set [`bloom_filter_on_read`]
    ///
    /// [`bloom_filter_on_read`]: datafusion_common::config::ParquetOptions::bloom_filter_on_read
    pub fn with_parquet_bloom_filter_pruning(mut self, enabled: bool) -> Self {
        self.options_mut().execution.parquet.bloom_filter_on_read = enabled;
        self
    }

    /// Get [`enable_page_index`]
    ///
    /// [`enable_page_index`]: datafusion_common::config::ParquetOptions::enable_page_index
    pub fn parquet_page_index_pruning(&self) -> bool {
        self.options.execution.parquet.enable_page_index
    }

    /// Set [`enable_page_index`]
    ///
    /// [`enable_page_index`]: datafusion_common::config::ParquetOptions::enable_page_index
    pub fn with_parquet_page_index_pruning(mut self, enabled: bool) -> Self {
        self.options_mut().execution.parquet.enable_page_index = enabled;
        self
    }

    /// Set [`collect_statistics`]
    ///
    /// [`collect_statistics`]: datafusion_common::config::ExecutionOptions::collect_statistics
    pub fn with_collect_statistics(mut self, enabled: bool) -> Self {
        self.options_mut().execution.collect_statistics = enabled;
        self
    }

    /// Get [`batch_size`]
    ///
    /// [`batch_size`]: datafusion_common::config::ExecutionOptions::batch_size
    pub fn batch_size(&self) -> usize {
        self.options.execution.batch_size.get()
    }

    /// Set [`coalesce_batches`]
    ///
    /// [`coalesce_batches`]: datafusion_common::config::ExecutionOptions::coalesce_batches
    pub fn with_coalesce_batches(mut self, enabled: bool) -> Self {
        self.options_mut().execution.coalesce_batches = enabled;
        self
    }

    /// Get [`coalesce_batches`]
    ///
    /// [`coalesce_batches`]: datafusion_common::config::ExecutionOptions::coalesce_batches
    pub fn coalesce_batches(&self) -> bool {
        self.options.execution.coalesce_batches
    }

    /// Set [`enable_round_robin_repartition`]
    ///
    /// [`enable_round_robin_repartition`]: datafusion_common::config::OptimizerOptions::enable_round_robin_repartition
    pub fn with_round_robin_repartition(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.enable_round_robin_repartition = enabled;
        self
    }

    /// Get [`enable_round_robin_repartition`]
    ///
    /// [`enable_round_robin_repartition`]: datafusion_common::config::OptimizerOptions::enable_round_robin_repartition
    pub fn round_robin_repartition(&self) -> bool {
        self.options.optimizer.enable_round_robin_repartition
    }

    /// Set [`enable_sort_pushdown`]
    ///
    /// [`enable_sort_pushdown`]: datafusion_common::config::OptimizerOptions::enable_sort_pushdown
    pub fn with_enable_sort_pushdown(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.enable_sort_pushdown = enabled;
        self
    }

    /// Set [`enable_subquery_sort_elimination`]
    ///
    /// [`enable_subquery_sort_elimination`]: datafusion_common::config::SqlParserOptions::enable_subquery_sort_elimination
    pub fn with_enable_subquery_sort_elimination(mut self, enabled: bool) -> Self {
        self.options_mut()
            .sql_parser
            .enable_subquery_sort_elimination = enabled;
        self
    }

    /// Set [`sort_spill_reservation_bytes`]
    ///
    /// [`sort_spill_reservation_bytes`]: datafusion_common::config::ExecutionOptions::sort_spill_reservation_bytes
    pub fn with_sort_spill_reservation_bytes(
        mut self,
        sort_spill_reservation_bytes: usize,
    ) -> Self {
        self.options_mut().execution.sort_spill_reservation_bytes =
            sort_spill_reservation_bytes;
        self
    }

    /// Set [`spill_compression`]
    ///
    /// [`spill_compression`]: datafusion_common::config::ExecutionOptions::spill_compression
    pub fn with_spill_compression(mut self, spill_compression: SpillCompression) -> Self {
        self.options_mut().execution.spill_compression = spill_compression;
        self
    }

    /// Set [`sort_in_place_threshold_bytes`]
    ///
    /// [`sort_in_place_threshold_bytes`]: datafusion_common::config::ExecutionOptions::sort_in_place_threshold_bytes
    pub fn with_sort_in_place_threshold_bytes(
        mut self,
        sort_in_place_threshold_bytes: usize,
    ) -> Self {
        self.options_mut().execution.sort_in_place_threshold_bytes =
            sort_in_place_threshold_bytes;
        self
    }

    /// Set [`enforce_batch_size_in_joins`]
    ///
    /// [`enforce_batch_size_in_joins`]: datafusion_common::config::ExecutionOptions::enforce_batch_size_in_joins
    pub fn with_enforce_batch_size_in_joins(
        mut self,
        enforce_batch_size_in_joins: bool,
    ) -> Self {
        self.options_mut().execution.enforce_batch_size_in_joins =
            enforce_batch_size_in_joins;
        self
    }

    /// Get [`enforce_batch_size_in_joins`]
    ///
    /// [`enforce_batch_size_in_joins`]: datafusion_common::config::ExecutionOptions::enforce_batch_size_in_joins
    pub fn enforce_batch_size_in_joins(&self) -> bool {
        self.options.execution.enforce_batch_size_in_joins
    }

    /// Set [`enable_ansi_mode`]
    ///
    /// [`enable_ansi_mode`]: datafusion_common::config::ExecutionOptions::enable_ansi_mode
    pub fn with_enable_ansi_mode(mut self, enable_ansi_mode: bool) -> Self {
        self.options_mut().execution.enable_ansi_mode = enable_ansi_mode;
        self
    }

    /// Convert configuration options to name-value pairs with values
    /// converted to strings.
    ///
    /// Note that this method will eventually be deprecated and
    /// replaced by [`options`].
    ///
    /// [`options`]: Self::options
    pub fn to_props(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        // copy configs from config_options
        for entry in self.options.entries() {
            map.insert(entry.key, entry.value.unwrap_or_default());
        }

        map
    }

    /// Add extensions.
    ///
    /// Extensions can be used to attach extra data to the session config -- e.g. tracing information or caches.
    /// Extensions are opaque and the types are unknown to DataFusion itself, which makes them extremely flexible. [^1]
    ///
    /// Extensions are stored within an [`Arc`] so they do NOT require [`Clone`]. The are immutable. If you need to
    /// modify their state over their lifetime -- e.g. for caches -- you need to establish some form of interior mutability.
    ///
    /// Extensions are indexed by their type `T`. If multiple values of the same type are provided, only the last one
    /// will be kept.
    ///
    /// You may use [`get_extension`](Self::get_extension) to retrieve extensions.
    ///
    /// # Example
    /// ```
    /// use datafusion_execution::config::SessionConfig;
    /// use std::sync::Arc;
    ///
    /// // application-specific extension types
    /// struct Ext1(u8);
    /// struct Ext2(u8);
    /// struct Ext3(u8);
    ///
    /// let ext1a = Arc::new(Ext1(10));
    /// let ext1b = Arc::new(Ext1(11));
    /// let ext2 = Arc::new(Ext2(2));
    ///
    /// let cfg = SessionConfig::default()
    ///     // will only remember the last Ext1
    ///     .with_extension(Arc::clone(&ext1a))
    ///     .with_extension(Arc::clone(&ext1b))
    ///     .with_extension(Arc::clone(&ext2));
    ///
    /// let ext1_received = cfg.get_extension::<Ext1>().unwrap();
    /// assert!(!Arc::ptr_eq(&ext1_received, &ext1a));
    /// assert!(Arc::ptr_eq(&ext1_received, &ext1b));
    ///
    /// let ext2_received = cfg.get_extension::<Ext2>().unwrap();
    /// assert!(Arc::ptr_eq(&ext2_received, &ext2));
    ///
    /// assert!(cfg.get_extension::<Ext3>().is_none());
    /// ```
    ///
    /// [^1]: Compare that to [`ConfigOptions`] which only supports [`ScalarValue`] payloads.
    pub fn with_extension<T>(mut self, ext: Arc<T>) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.set_extension(ext);
        self
    }

    /// Set extension. Pretty much the same as [`with_extension`](Self::with_extension), but take
    /// mutable reference instead of owning it. Useful if you want to add another extension after
    /// the [`SessionConfig`] is created.
    ///
    /// # Example
    /// ```
    /// use datafusion_execution::config::SessionConfig;
    /// use std::sync::Arc;
    ///
    /// // application-specific extension types
    /// struct Ext1(u8);
    /// struct Ext2(u8);
    /// struct Ext3(u8);
    ///
    /// let ext1a = Arc::new(Ext1(10));
    /// let ext1b = Arc::new(Ext1(11));
    /// let ext2 = Arc::new(Ext2(2));
    ///
    /// let mut cfg = SessionConfig::default();
    ///
    /// // will only remember the last Ext1
    /// cfg.set_extension(Arc::clone(&ext1a));
    /// cfg.set_extension(Arc::clone(&ext1b));
    /// cfg.set_extension(Arc::clone(&ext2));
    ///
    /// let ext1_received = cfg.get_extension::<Ext1>().unwrap();
    /// assert!(!Arc::ptr_eq(&ext1_received, &ext1a));
    /// assert!(Arc::ptr_eq(&ext1_received, &ext1b));
    ///
    /// let ext2_received = cfg.get_extension::<Ext2>().unwrap();
    /// assert!(Arc::ptr_eq(&ext2_received, &ext2));
    ///
    /// assert!(cfg.get_extension::<Ext3>().is_none());
    /// ```
    pub fn set_extension<T>(&mut self, ext: Arc<T>)
    where
        T: Send + Sync + 'static,
    {
        self.extensions.insert_arc(ext);
    }

    /// Get extension, if any for the specified type `T` exists.
    ///
    /// See [`with_extension`](Self::with_extension) on how to add attach extensions.
    pub fn get_extension<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.extensions.get_arc::<T>()
    }
}

impl From<ConfigOptions> for SessionConfig {
    fn from(options: ConfigOptions) -> Self {
        let options = Arc::new(options);
        Self {
            options,
            ..Default::default()
        }
    }
}
