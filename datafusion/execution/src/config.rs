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

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::Arc,
};

use datafusion_common::{
    config::{ConfigExtension, ConfigOptions, SpillCompression},
    Result, ScalarValue,
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
/// assert_eq!(config.options().execution.batch_size, 1234);
/// assert_eq!(config.options().execution.parquet.pushdown_filters, true);
/// ```
///
/// You can also directly mutate the options via [SessionConfig::options_mut].
/// So the following is equivalent to the above:
///
/// ```
/// # use datafusion_execution::config::SessionConfig;
/// # use datafusion_common::ScalarValue;
/// #
/// let mut config = SessionConfig::new();
/// config.options_mut().execution.batch_size = 1234;
/// config.options_mut().execution.parquet.pushdown_filters = true;
/// #
/// # assert_eq!(config.batch_size(), 1234);
/// # assert_eq!(config.options().execution.batch_size, 1234);
/// # assert_eq!(config.options().execution.parquet.pushdown_filters, true);
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
    /// Opaque extensions.
    extensions: Arc<Extensions>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            options: Arc::new(ConfigOptions::new()),
            // Assume no extensions by default.
            extensions: Arc::new(Extensions::default()),
        }
    }
}

/// A type map for storing extensions.
///
/// Extensions are indexed by their type `T`. If multiple values of the same type are provided, only the last one
/// will be kept.
///
/// Extensions are opaque objects that are unknown to DataFusion itself but can be downcast by optimizer rules,
/// execution plans, or other components that have access to the session config.
/// They provide a flexible way to attach extra data or behavior to the session config.
#[derive(Clone, Debug)]
pub struct Extensions {
    inner: AnyMap,
}

impl Default for Extensions {
    fn default() -> Self {
        Self {
            inner: HashMap::with_capacity_and_hasher(0, BuildHasherDefault::default()),
        }
    }
}

impl Extensions {
    pub fn insert<T>(&mut self, value: Arc<T>)
    where
        T: Send + Sync + 'static,
    {
        let id = TypeId::of::<T>();
        self.inner.insert(id, value);
    }

    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let id = TypeId::of::<T>();
        self.inner
            .get(&id)
            .cloned()
            .map(|arc_any| Arc::downcast(arc_any).expect("TypeId unique"))
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
    /// assert!(config.options().execution.batch_size > 0);
    /// ```
    pub fn options(&self) -> &Arc<ConfigOptions> {
        &self.options
    }

    /// Return a mutable handle to the configuration options.
    ///
    /// Can be used to set configuration options.
    ///
    /// ```
    /// use datafusion_execution::config::SessionConfig;
    ///
    /// let mut config = SessionConfig::new();
    /// config.options_mut().execution.batch_size = 1024;
    /// assert_eq!(config.options().execution.batch_size, 1024);
    /// ```
    pub fn options_mut(&mut self) -> &mut ConfigOptions {
        Arc::make_mut(&mut self.options)
    }

    /// Return a handle to the extensions.
    ///
    /// Can be used to read the current extensions.
    pub fn extensions(&self) -> &Arc<Extensions> {
        &self.extensions
    }

    /// Return a mutable handle to the extensions.
    ///
    /// Can be used to set extensions.
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        Arc::make_mut(&mut self.extensions)
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

    /// Customize batch size
    pub fn with_batch_size(mut self, n: usize) -> Self {
        // batch size must be greater than zero
        assert!(n > 0);
        self.options_mut().execution.batch_size = n;
        self
    }

    /// Customize [`target_partitions`]
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

    /// Is the information schema enabled?
    pub fn information_schema(&self) -> bool {
        self.options.catalog.information_schema
    }

    /// Should the context create the default catalog and schema?
    pub fn create_default_catalog_and_schema(&self) -> bool {
        self.options.catalog.create_default_catalog_and_schema
    }

    /// Are joins repartitioned during execution?
    pub fn repartition_joins(&self) -> bool {
        self.options.optimizer.repartition_joins
    }

    /// Are aggregates repartitioned during execution?
    pub fn repartition_aggregations(&self) -> bool {
        self.options.optimizer.repartition_aggregations
    }

    /// Are window functions repartitioned during execution?
    pub fn repartition_window_functions(&self) -> bool {
        self.options.optimizer.repartition_windows
    }

    /// Do we execute sorts in a per-partition fashion and merge afterwards,
    /// or do we coalesce partitions first and sort globally?
    pub fn repartition_sorts(&self) -> bool {
        self.options.optimizer.repartition_sorts
    }

    /// Prefer existing sort (true) or maximize parallelism (false). See
    /// [prefer_existing_sort] for more details
    ///
    /// [prefer_existing_sort]: datafusion_common::config::OptimizerOptions::prefer_existing_sort
    pub fn prefer_existing_sort(&self) -> bool {
        self.options.optimizer.prefer_existing_sort
    }

    /// Are statistics collected during execution?
    pub fn collect_statistics(&self) -> bool {
        self.options.execution.collect_statistics
    }

    /// Compression codec for spill file
    pub fn spill_compression(&self) -> SpillCompression {
        self.options.execution.spill_compression
    }

    /// Selects a name for the default catalog and schema
    pub fn with_default_catalog_and_schema(
        mut self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
    ) -> Self {
        self.options_mut().catalog.default_catalog = catalog.into();
        self.options_mut().catalog.default_schema = schema.into();
        self
    }

    /// Controls whether the default catalog and schema will be automatically created
    pub fn with_create_default_catalog_and_schema(mut self, create: bool) -> Self {
        self.options_mut().catalog.create_default_catalog_and_schema = create;
        self
    }

    /// Enables or disables the inclusion of `information_schema` virtual tables
    pub fn with_information_schema(mut self, enabled: bool) -> Self {
        self.options_mut().catalog.information_schema = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for joins to improve parallelism
    pub fn with_repartition_joins(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_joins = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for aggregations to improve parallelism
    pub fn with_repartition_aggregations(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_aggregations = enabled;
        self
    }

    /// Sets minimum file range size for repartitioning scans
    pub fn with_repartition_file_min_size(mut self, size: usize) -> Self {
        self.options_mut().optimizer.repartition_file_min_size = size;
        self
    }

    /// Enables or disables the allowing unordered symmetric hash join
    pub fn with_allow_symmetric_joins_without_pruning(mut self, enabled: bool) -> Self {
        self.options_mut()
            .optimizer
            .allow_symmetric_joins_without_pruning = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for file scans
    pub fn with_repartition_file_scans(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_file_scans = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for window functions to improve parallelism
    pub fn with_repartition_windows(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_windows = enabled;
        self
    }

    /// Enables or disables the use of per-partition sorting to improve parallelism
    pub fn with_repartition_sorts(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.repartition_sorts = enabled;
        self
    }

    /// Prefer existing sort (true) or maximize parallelism (false). See
    /// [prefer_existing_sort] for more details
    ///
    /// [prefer_existing_sort]: datafusion_common::config::OptimizerOptions::prefer_existing_sort
    pub fn with_prefer_existing_sort(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.prefer_existing_sort = enabled;
        self
    }

    /// Prefer existing union (true). See [prefer_existing_union] for more details
    ///
    /// [prefer_existing_union]: datafusion_common::config::OptimizerOptions::prefer_existing_union
    pub fn with_prefer_existing_union(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.prefer_existing_union = enabled;
        self
    }

    /// Enables or disables the use of pruning predicate for parquet readers to skip row groups
    pub fn with_parquet_pruning(mut self, enabled: bool) -> Self {
        self.options_mut().execution.parquet.pruning = enabled;
        self
    }

    /// Returns true if pruning predicate should be used to skip parquet row groups
    pub fn parquet_pruning(&self) -> bool {
        self.options.execution.parquet.pruning
    }

    /// Returns true if bloom filter should be used to skip parquet row groups
    pub fn parquet_bloom_filter_pruning(&self) -> bool {
        self.options.execution.parquet.bloom_filter_on_read
    }

    /// Enables or disables the use of bloom filter for parquet readers to skip row groups
    pub fn with_parquet_bloom_filter_pruning(mut self, enabled: bool) -> Self {
        self.options_mut().execution.parquet.bloom_filter_on_read = enabled;
        self
    }

    /// Returns true if page index should be used to skip parquet data pages
    pub fn parquet_page_index_pruning(&self) -> bool {
        self.options.execution.parquet.enable_page_index
    }

    /// Enables or disables the use of page index for parquet readers to skip parquet data pages
    pub fn with_parquet_page_index_pruning(mut self, enabled: bool) -> Self {
        self.options_mut().execution.parquet.enable_page_index = enabled;
        self
    }

    /// Enables or disables the collection of statistics after listing files
    pub fn with_collect_statistics(mut self, enabled: bool) -> Self {
        self.options_mut().execution.collect_statistics = enabled;
        self
    }

    /// Get the currently configured batch size
    pub fn batch_size(&self) -> usize {
        self.options.execution.batch_size
    }

    /// Enables or disables the coalescence of small batches into larger batches
    pub fn with_coalesce_batches(mut self, enabled: bool) -> Self {
        self.options_mut().execution.coalesce_batches = enabled;
        self
    }

    /// Returns true if record batches will be examined between each operator
    /// and small batches will be coalesced into larger batches.
    pub fn coalesce_batches(&self) -> bool {
        self.options.execution.coalesce_batches
    }

    /// Enables or disables the round robin repartition for increasing parallelism
    pub fn with_round_robin_repartition(mut self, enabled: bool) -> Self {
        self.options_mut().optimizer.enable_round_robin_repartition = enabled;
        self
    }

    /// Returns true if the physical plan optimizer will try to
    /// add round robin repartition to increase parallelism to leverage more CPU cores.
    pub fn round_robin_repartition(&self) -> bool {
        self.options.optimizer.enable_round_robin_repartition
    }

    /// Set the size of [`sort_spill_reservation_bytes`] to control
    /// memory pre-reservation
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

    /// Set the compression codec [`spill_compression`] used when spilling data to disk.
    ///
    /// [`spill_compression`]: datafusion_common::config::ExecutionOptions::spill_compression
    pub fn with_spill_compression(mut self, spill_compression: SpillCompression) -> Self {
        self.options_mut().execution.spill_compression = spill_compression;
        self
    }

    /// Set the size of [`sort_in_place_threshold_bytes`] to control
    /// how sort does things.
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

    /// Enables or disables the enforcement of batch size in joins
    pub fn with_enforce_batch_size_in_joins(
        mut self,
        enforce_batch_size_in_joins: bool,
    ) -> Self {
        self.options_mut().execution.enforce_batch_size_in_joins =
            enforce_batch_size_in_joins;
        self
    }

    /// Returns true if the joins will be enforced to output batches of the configured size
    pub fn enforce_batch_size_in_joins(&self) -> bool {
        self.options.execution.enforce_batch_size_in_joins
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
    /// modify their state over their lifetime -- e.g. for caches -- you need to establish some for of interior mutability.
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
        self.extensions_mut().insert::<T>(ext);
    }

    /// Get extension, if any for the specified type `T` exists.
    ///
    /// See [`with_extension`](Self::with_extension) on how to add attach extensions.
    pub fn get_extension<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.extensions.get::<T>()
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

/// Map that holds opaque objects indexed by their type.
///
/// Data is wrapped into an [`Arc`] to enable [`Clone`] while still being [object safe].
///
/// [object safe]: https://doc.rust-lang.org/reference/items/traits.html#object-safety
type AnyMap =
    HashMap<TypeId, Arc<dyn Any + Send + Sync + 'static>, BuildHasherDefault<IdHasher>>;

/// Hasher for [`AnyMap`].
///
/// With [`TypeId`]s as keys, there's no need to hash them. They are already hashes themselves, coming from the compiler.
/// The [`IdHasher`] just holds the [`u64`] of the [`TypeId`], and then returns it, instead of doing any bit fiddling.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn write(&mut self, _: &[u8]) {
        unreachable!("TypeId calls write_u64");
    }

    #[inline]
    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}
