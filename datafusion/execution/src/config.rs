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

use datafusion_common::{config::ConfigOptions, Result, ScalarValue};

/// Configuration options for Execution context
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Configuration options
    options: ConfigOptions,
    /// Opaque extensions.
    extensions: AnyMap,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            options: ConfigOptions::new(),
            // Assume no extensions by default.
            extensions: HashMap::with_capacity_and_hasher(
                0,
                BuildHasherDefault::default(),
            ),
        }
    }
}

impl SessionConfig {
    /// Create an execution config with default setting
    pub fn new() -> Self {
        Default::default()
    }

    /// Create an execution config with config options read from the environment
    pub fn from_env() -> Result<Self> {
        Ok(ConfigOptions::from_env()?.into())
    }

    /// Create new ConfigOptions struct, taking values from a string hash map.
    pub fn from_string_hash_map(settings: HashMap<String, String>) -> Result<Self> {
        Ok(ConfigOptions::from_string_hash_map(settings)?.into())
    }

    /// Set a configuration option
    pub fn set(mut self, key: &str, value: ScalarValue) -> Self {
        self.options.set(key, &value.to_string()).unwrap();
        self
    }

    /// Set a boolean configuration option
    pub fn set_bool(self, key: &str, value: bool) -> Self {
        self.set(key, ScalarValue::Boolean(Some(value)))
    }

    /// Set a generic `u64` configuration option
    pub fn set_u64(self, key: &str, value: u64) -> Self {
        self.set(key, ScalarValue::UInt64(Some(value)))
    }

    /// Set a generic `usize` configuration option
    pub fn set_usize(self, key: &str, value: usize) -> Self {
        let value: u64 = value.try_into().expect("convert usize to u64");
        self.set(key, ScalarValue::UInt64(Some(value)))
    }

    /// Set a generic `str` configuration option
    pub fn set_str(self, key: &str, value: &str) -> Self {
        self.set(key, ScalarValue::from(value))
    }

    /// Customize batch size
    pub fn with_batch_size(mut self, n: usize) -> Self {
        // batch size must be greater than zero
        assert!(n > 0);
        self.options.execution.batch_size = n;
        self
    }

    /// Customize [`target_partitions`]
    ///
    /// [`target_partitions`]: datafusion_common::config::ExecutionOptions::target_partitions
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        // partition count must be greater than zero
        assert!(n > 0);
        self.options.execution.target_partitions = n;
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

    /// Selects a name for the default catalog and schema
    pub fn with_default_catalog_and_schema(
        mut self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
    ) -> Self {
        self.options.catalog.default_catalog = catalog.into();
        self.options.catalog.default_schema = schema.into();
        self
    }

    /// Controls whether the default catalog and schema will be automatically created
    pub fn with_create_default_catalog_and_schema(mut self, create: bool) -> Self {
        self.options.catalog.create_default_catalog_and_schema = create;
        self
    }

    /// Enables or disables the inclusion of `information_schema` virtual tables
    pub fn with_information_schema(mut self, enabled: bool) -> Self {
        self.options.catalog.information_schema = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for joins to improve parallelism
    pub fn with_repartition_joins(mut self, enabled: bool) -> Self {
        self.options.optimizer.repartition_joins = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for aggregations to improve parallelism
    pub fn with_repartition_aggregations(mut self, enabled: bool) -> Self {
        self.options.optimizer.repartition_aggregations = enabled;
        self
    }

    /// Sets minimum file range size for repartitioning scans
    pub fn with_repartition_file_min_size(mut self, size: usize) -> Self {
        self.options.optimizer.repartition_file_min_size = size;
        self
    }

    /// Enables or disables the allowing unordered symmetric hash join
    pub fn with_allow_symmetric_joins_without_pruning(mut self, enabled: bool) -> Self {
        self.options.optimizer.allow_symmetric_joins_without_pruning = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for file scans
    pub fn with_repartition_file_scans(mut self, enabled: bool) -> Self {
        self.options.optimizer.repartition_file_scans = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for window functions to improve parallelism
    pub fn with_repartition_windows(mut self, enabled: bool) -> Self {
        self.options.optimizer.repartition_windows = enabled;
        self
    }

    /// Enables or disables the use of per-partition sorting to improve parallelism
    pub fn with_repartition_sorts(mut self, enabled: bool) -> Self {
        self.options.optimizer.repartition_sorts = enabled;
        self
    }

    /// Prefer existing sort (true) or maximize parallelism (false). See
    /// [prefer_existing_sort] for more details
    ///
    /// [prefer_existing_sort]: datafusion_common::config::OptimizerOptions::prefer_existing_sort
    pub fn with_prefer_existing_sort(mut self, enabled: bool) -> Self {
        self.options.optimizer.prefer_existing_sort = enabled;
        self
    }

    /// Enables or disables the use of pruning predicate for parquet readers to skip row groups
    pub fn with_parquet_pruning(mut self, enabled: bool) -> Self {
        self.options.execution.parquet.pruning = enabled;
        self
    }

    /// Returns true if pruning predicate should be used to skip parquet row groups
    pub fn parquet_pruning(&self) -> bool {
        self.options.execution.parquet.pruning
    }

    /// Enables or disables the collection of statistics after listing files
    pub fn with_collect_statistics(mut self, enabled: bool) -> Self {
        self.options.execution.collect_statistics = enabled;
        self
    }

    /// Get the currently configured batch size
    pub fn batch_size(&self) -> usize {
        self.options.execution.batch_size
    }

    /// Get the currently configured scalar_update_factor for aggregate
    pub fn agg_scalar_update_factor(&self) -> usize {
        self.options.execution.aggregate.scalar_update_factor
    }

    /// Customize scalar_update_factor for aggregate
    pub fn with_agg_scalar_update_factor(mut self, n: usize) -> Self {
        // scalar update factor must be greater than zero
        assert!(n > 0);
        self.options.execution.aggregate.scalar_update_factor = n;
        self
    }

    /// Enables or disables the coalescence of small batches into larger batches
    pub fn with_coalesce_batches(mut self, enabled: bool) -> Self {
        self.options.execution.coalesce_batches = enabled;
        self
    }

    /// Returns true if record batches will be examined between each operator
    /// and small batches will be coalesced into larger batches.
    pub fn coalesce_batches(&self) -> bool {
        self.options.execution.coalesce_batches
    }

    /// Enables or disables the round robin repartition for increasing parallelism
    pub fn with_round_robin_repartition(mut self, enabled: bool) -> Self {
        self.options.optimizer.enable_round_robin_repartition = enabled;
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
        self.options.execution.sort_spill_reservation_bytes =
            sort_spill_reservation_bytes;
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
        self.options.execution.sort_in_place_threshold_bytes =
            sort_in_place_threshold_bytes;
        self
    }

    /// Convert configuration options to name-value pairs with values
    /// converted to strings.
    ///
    /// Note that this method will eventually be deprecated and
    /// replaced by [`config_options`].
    ///
    /// [`config_options`]: Self::config_options
    pub fn to_props(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        // copy configs from config_options
        for entry in self.options.entries() {
            map.insert(entry.key, entry.value.unwrap_or_default());
        }

        map
    }

    /// Return a handle to the configuration options.
    #[deprecated(since = "21.0.0", note = "use options() instead")]
    pub fn config_options(&self) -> &ConfigOptions {
        &self.options
    }

    /// Return a mutable handle to the configuration options.
    #[deprecated(since = "21.0.0", note = "use options_mut() instead")]
    pub fn config_options_mut(&mut self) -> &mut ConfigOptions {
        &mut self.options
    }

    /// Return a handle to the configuration options.
    pub fn options(&self) -> &ConfigOptions {
        &self.options
    }

    /// Return a mutable handle to the configuration options.
    pub fn options_mut(&mut self) -> &mut ConfigOptions {
        &mut self.options
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
    /// use std::sync::Arc;
    /// use datafusion_execution::config::SessionConfig;
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
        let ext = ext as Arc<dyn Any + Send + Sync + 'static>;
        let id = TypeId::of::<T>();
        self.extensions.insert(id, ext);
        self
    }

    /// Get extension, if any for the specified type `T` exists.
    ///
    /// See [`with_extension`](Self::with_extension) on how to add attach extensions.
    pub fn get_extension<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let id = TypeId::of::<T>();
        self.extensions
            .get(&id)
            .cloned()
            .map(|ext| Arc::downcast(ext).expect("TypeId unique"))
    }
}

impl From<ConfigOptions> for SessionConfig {
    fn from(options: ConfigOptions) -> Self {
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
