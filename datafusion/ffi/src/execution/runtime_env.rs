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

//! FFI support for [`RuntimeEnv`].
//!
//! # How the environment crosses
//!
//! Each component is carried across on its own terms:
//!
//! | Component | How it crosses |
//! |---|---|
//! | [`ObjectStoreRegistry`] | Shared: it is a trait object, wrapped by [`FFI_ObjectStoreRegistry`] |
//! | [`MemoryPool`] | Shared: it is a trait object, wrapped by [`FFI_MemoryPool`] |
//! | [`DiskManager`] | Configuration copied; each side keeps its own instance |
//! | [`CacheManager`] | Configuration copied; each side keeps its own instance |
//!
//! The first two are the ones that matter for correctness. Sharing the registry
//! lets a table provider and its session agree on object stores, and sharing
//! the memory pool makes the session's memory limit apply to a foreign plan.
//!
//! [`RuntimeEnv`] is a plain struct rather than a trait, so it is tempting to
//! pass an `Arc<RuntimeEnv>` across as an opaque pointer instead. That is
//! unsound: the struct is not `repr(C)`, and its field list depends on enabled
//! features (`parquet_encryption` adds one). Two libraries built against the
//! same DataFusion version but different feature sets disagree about the
//! layout, so reading through such a pointer is undefined behaviour rather than
//! merely version-fragile.
//!
//! # What the configuration copy does and does not give you
//!
//! [`DiskManager`] and [`CacheManager`] are concrete structs and cannot be
//! wrapped as trait objects, so only their configuration is copied and each
//! side builds its own. Two consequences are worth knowing:
//!
//! * Spill limits are enforced *per side*. Both disk managers honour the same
//!   `max_temp_directory_size`, but they count independently, so total on-disk
//!   usage can reach twice the configured limit.
//! * Caches are not shared, so file statistics and listings fetched by one side
//!   are not reused by the other.
//!
//! The host's temp directory *paths* are deliberately not propagated.
//! [`DiskManager::temp_dir_paths`] returns directories the host has already
//! created and owns, and it deletes them when it is dropped. Pointing a second
//! disk manager at them would risk writing into a directory that has been
//! removed underneath it, so the foreign side gets its own OS temporary
//! directory instead. Whether spilling is enabled at all *is* propagated.
//!
//! # The local fast path
//!
//! When both sides are the same library, [`FFI_RuntimeEnv::as_local`] returns
//! the original `Arc<RuntimeEnv>` and none of the above applies: the two sides
//! share one runtime environment exactly, including its disk manager and
//! caches.
//!
//! [`DiskManager`]: datafusion_execution::disk_manager::DiskManager
//! [`DiskManager::temp_dir_paths`]: datafusion_execution::disk_manager::DiskManager::temp_dir_paths
//! [`CacheManager`]: datafusion_execution::cache::cache_manager::CacheManager
//! [`ObjectStoreRegistry`]: datafusion_execution::object_store::ObjectStoreRegistry
//! [`MemoryPool`]: datafusion_execution::memory_pool::MemoryPool

use std::ffi::c_void;
use std::sync::Arc;
use std::time::Duration;

use datafusion_common::{DataFusionError, Result};
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::memory_pool::MemoryPool;
use datafusion_execution::object_store::ObjectStoreRegistry;
use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

use crate::util::FFI_Option;

use super::memory_pool::FFI_MemoryPool;
use super::object_store::FFI_ObjectStoreRegistry;

/// The parts of [`RuntimeEnv`] that are copied rather than shared.
///
/// See the module documentation for why these are copied and what that costs.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_RuntimeConfig {
    /// Whether the providing side permits spilling to disk at all, so a
    /// session that has disabled spilling keeps it disabled across the
    /// boundary.
    pub tmp_files_enabled: bool,

    /// Maximum temporary directory size in bytes.
    pub max_temp_directory_size: u64,

    /// Maximum number of spill files opened by one merge pass. Zero means
    /// unlimited.
    pub max_spill_merge_fan_in: u64,

    pub metadata_cache_limit: u64,
    pub list_files_cache_limit: u64,
    pub list_files_cache_ttl_secs: FFI_Option<u64>,
    pub file_statistics_cache_limit: u64,
}

impl From<&RuntimeEnv> for FFI_RuntimeConfig {
    fn from(runtime_env: &RuntimeEnv) -> Self {
        Self {
            tmp_files_enabled: runtime_env.disk_manager.tmp_files_enabled(),
            max_temp_directory_size: runtime_env.disk_manager.max_temp_directory_size(),
            max_spill_merge_fan_in: runtime_env.disk_manager.max_spill_merge_fan_in()
                as u64,
            metadata_cache_limit: runtime_env.cache_manager.get_metadata_cache_limit()
                as u64,
            list_files_cache_limit: runtime_env.cache_manager.get_list_files_cache_limit()
                as u64,
            list_files_cache_ttl_secs: runtime_env
                .cache_manager
                .get_list_files_cache_ttl()
                .map(|ttl| ttl.as_secs())
                .into(),
            file_statistics_cache_limit: runtime_env
                .cache_manager
                .get_file_statistic_cache_limit()
                as u64,
        }
    }
}

impl FFI_RuntimeConfig {
    /// Apply this configuration to a [`RuntimeEnvBuilder`].
    fn apply(self, builder: RuntimeEnvBuilder) -> RuntimeEnvBuilder {
        let disk_manager = DiskManagerBuilder::default()
            .with_mode(if self.tmp_files_enabled {
                DiskManagerMode::OsTmpDirectory
            } else {
                DiskManagerMode::Disabled
            })
            .with_max_temp_directory_size(self.max_temp_directory_size)
            .with_max_spill_merge_fan_in(self.max_spill_merge_fan_in as usize);

        builder
            .with_disk_manager_builder(disk_manager)
            .with_metadata_cache_limit(self.metadata_cache_limit as usize)
            .with_object_list_cache_limit(self.list_files_cache_limit as usize)
            .with_object_list_cache_ttl(
                self.list_files_cache_ttl_secs
                    .into_option()
                    .map(Duration::from_secs),
            )
            .with_file_statistics_cache_limit(self.file_statistics_cache_limit as usize)
    }
}

/// A stable struct for sharing [`RuntimeEnv`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_RuntimeEnv {
    /// Return the object store registry. Shared, not copied.
    pub object_store_registry:
        unsafe extern "C" fn(runtime_env: &Self) -> FFI_ObjectStoreRegistry,

    /// Return the memory pool. Shared, not copied.
    pub memory_pool: unsafe extern "C" fn(runtime_env: &Self) -> FFI_MemoryPool,

    /// Return the configuration of the components that are copied.
    pub config: unsafe extern "C" fn(runtime_env: &Self) -> FFI_RuntimeConfig,

    /// Used to create a clone on the provider of the environment. This should
    /// only need to be called by the receiver of the environment.
    pub clone: unsafe extern "C" fn(runtime_env: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the
    /// environment. The foreign library should never attempt to access this
    /// data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_RuntimeEnv {}
unsafe impl Sync for FFI_RuntimeEnv {}

struct RuntimeEnvPrivateData {
    runtime_env: Arc<RuntimeEnv>,
    runtime: Option<tokio::runtime::Handle>,
}

impl FFI_RuntimeEnv {
    fn private_data(&self) -> &RuntimeEnvPrivateData {
        unsafe { &*(self.private_data as *const RuntimeEnvPrivateData) }
    }

    fn inner(&self) -> &Arc<RuntimeEnv> {
        &self.private_data().runtime_env
    }

    /// Create a new [`FFI_RuntimeEnv`] from a local runtime environment.
    ///
    /// `runtime` is the tokio runtime handle of the providing library, attached
    /// to object stores handed out by the registry so that stores which spawn
    /// tasks work when driven by a foreign executor.
    pub fn new(
        runtime_env: Arc<RuntimeEnv>,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        Self {
            object_store_registry: object_store_registry_fn_wrapper,
            memory_pool: memory_pool_fn_wrapper,
            config: config_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(RuntimeEnvPrivateData {
                runtime_env,
                runtime,
            })) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }

    /// If this environment originated in the current library, return the
    /// underlying [`RuntimeEnv`] directly.
    ///
    /// This is an exact identity match, so the two sides share the disk manager
    /// and caches as well as the registry and memory pool.
    pub fn as_local(&self) -> Option<Arc<RuntimeEnv>> {
        ((self.library_marker_id)() == crate::get_library_marker_id())
            .then(|| Arc::clone(self.inner()))
    }
}

unsafe extern "C" fn object_store_registry_fn_wrapper(
    runtime_env: &FFI_RuntimeEnv,
) -> FFI_ObjectStoreRegistry {
    let private_data = runtime_env.private_data();
    FFI_ObjectStoreRegistry::new(
        Arc::clone(&private_data.runtime_env.object_store_registry),
        private_data.runtime.clone(),
    )
}

unsafe extern "C" fn memory_pool_fn_wrapper(
    runtime_env: &FFI_RuntimeEnv,
) -> FFI_MemoryPool {
    FFI_MemoryPool::new(Arc::clone(&runtime_env.inner().memory_pool))
}

unsafe extern "C" fn config_fn_wrapper(
    runtime_env: &FFI_RuntimeEnv,
) -> FFI_RuntimeConfig {
    FFI_RuntimeConfig::from(runtime_env.inner().as_ref())
}

unsafe extern "C" fn clone_fn_wrapper(runtime_env: &FFI_RuntimeEnv) -> FFI_RuntimeEnv {
    let private_data = runtime_env.private_data();
    FFI_RuntimeEnv::new(
        Arc::clone(&private_data.runtime_env),
        private_data.runtime.clone(),
    )
}

unsafe extern "C" fn release_fn_wrapper(runtime_env: &mut FFI_RuntimeEnv) {
    unsafe {
        debug_assert!(!runtime_env.private_data.is_null());
        drop(Box::from_raw(
            runtime_env.private_data as *mut RuntimeEnvPrivateData,
        ));
        runtime_env.private_data = std::ptr::null_mut();
    }
}

impl Clone for FFI_RuntimeEnv {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_RuntimeEnv {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl TryFrom<&FFI_RuntimeEnv> for Arc<RuntimeEnv> {
    type Error = DataFusionError;

    fn try_from(runtime_env: &FFI_RuntimeEnv) -> Result<Self> {
        if let Some(local) = runtime_env.as_local() {
            return Ok(local);
        }

        let registry: Arc<dyn ObjectStoreRegistry> =
            unsafe { (runtime_env.object_store_registry)(runtime_env) }.into();
        let memory_pool: Arc<dyn MemoryPool> =
            unsafe { (runtime_env.memory_pool)(runtime_env) }.into();
        let config = unsafe { (runtime_env.config)(runtime_env) };

        config
            .apply(
                RuntimeEnvBuilder::new()
                    .with_object_store_registry(registry)
                    .with_memory_pool(memory_pool),
            )
            .build_arc()
    }
}

#[cfg(test)]
mod tests {
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryLimit,
    };
    use datafusion_execution::object_store::ObjectStoreUrl;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;

    /// `RuntimeEnv::object_store` takes an `ObjectStoreUrl` rather than a raw
    /// `Url`.
    fn store_url(url: &Url) -> ObjectStoreUrl {
        ObjectStoreUrl::parse(url.as_str()).expect("valid object store url")
    }

    fn foreign_runtime_env(runtime_env: Arc<RuntimeEnv>) -> Arc<RuntimeEnv> {
        let mut ffi = FFI_RuntimeEnv::new(runtime_env, None);
        ffi.library_marker_id = crate::mock_foreign_marker_id;
        Arc::<RuntimeEnv>::try_from(&ffi).expect("build foreign runtime env")
    }

    /// The bug this whole module exists to fix: a store registered on one side
    /// must be visible from the other.
    #[test]
    fn object_store_registered_on_host_is_visible() -> Result<()> {
        let host = RuntimeEnvBuilder::new().build_arc()?;
        let url = Url::parse("s3://bucket").unwrap();
        host.register_object_store(&url, Arc::new(InMemory::new()));

        let foreign = foreign_runtime_env(Arc::clone(&host));
        assert!(foreign.object_store(store_url(&url)).is_ok());

        Ok(())
    }

    /// A store registered by a table provider on the session it is handed
    /// during planning must be visible to the host at execution time.
    #[test]
    fn object_store_registered_on_foreign_is_visible_to_host() -> Result<()> {
        let host = RuntimeEnvBuilder::new().build_arc()?;
        let foreign = foreign_runtime_env(Arc::clone(&host));

        let url = Url::parse("s3://provider-registered").unwrap();
        foreign.register_object_store(&url, Arc::new(InMemory::new()));

        assert!(
            host.object_store(store_url(&url)).is_ok(),
            "store registered through the foreign runtime env should reach the host"
        );

        Ok(())
    }

    /// A store that a provider registers and then reads back must come back as
    /// its own local store, so reads do not cross the boundary.
    #[test]
    fn round_tripped_store_is_recovered_locally() -> Result<()> {
        let host = RuntimeEnvBuilder::new().build_arc()?;
        let foreign = foreign_runtime_env(Arc::clone(&host));

        let url = Url::parse("s3://mine").unwrap();
        let original = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        foreign.register_object_store(&url, Arc::clone(&original));

        let recovered = foreign.object_store(store_url(&url))?;
        assert!(
            Arc::ptr_eq(&original, &recovered),
            "a provider's own store should not be wrapped when read back"
        );

        Ok(())
    }

    #[test]
    fn memory_limit_crosses_the_boundary() -> Result<()> {
        let host = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(1_000)))
            .build_arc()?;
        let foreign = foreign_runtime_env(Arc::clone(&host));

        assert!(matches!(
            foreign.memory_pool.memory_limit(),
            MemoryLimit::Finite(1_000)
        ));

        let reservation = MemoryConsumer::new("foreign").register(&foreign.memory_pool);
        reservation.grow(600);
        assert_eq!(
            host.memory_pool.reserved(),
            600,
            "foreign allocations must count against the host pool"
        );

        Ok(())
    }

    #[test]
    fn disk_manager_config_is_copied() -> Result<()> {
        let host = RuntimeEnvBuilder::new()
            .with_max_temp_directory_size(4_096)
            .with_max_spill_merge_fan_in(7)
            .build_arc()?;
        let foreign = foreign_runtime_env(host);

        assert_eq!(foreign.disk_manager.max_temp_directory_size(), 4_096);
        assert_eq!(foreign.disk_manager.max_spill_merge_fan_in(), 7);
        assert!(foreign.disk_manager.tmp_files_enabled());

        Ok(())
    }

    /// A host that disabled spilling must not have a foreign plan spill behind
    /// its back.
    #[test]
    fn disabled_spilling_is_propagated() -> Result<()> {
        let host = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        assert!(!host.disk_manager.tmp_files_enabled());

        let foreign = foreign_runtime_env(host);
        assert!(
            !foreign.disk_manager.tmp_files_enabled(),
            "spilling must stay disabled across the boundary"
        );

        Ok(())
    }

    #[test]
    fn cache_config_is_copied() -> Result<()> {
        let host = RuntimeEnvBuilder::new()
            .with_metadata_cache_limit(1_234)
            .with_object_list_cache_limit(2_345)
            .with_object_list_cache_ttl(Some(Duration::from_mins(1)))
            .with_file_statistics_cache_limit(3_456)
            .build_arc()?;
        let foreign = foreign_runtime_env(host);

        assert_eq!(foreign.cache_manager.get_metadata_cache_limit(), 1_234);
        assert_eq!(foreign.cache_manager.get_list_files_cache_limit(), 2_345);
        assert_eq!(
            foreign.cache_manager.get_list_files_cache_ttl(),
            Some(Duration::from_mins(1))
        );
        assert_eq!(
            foreign.cache_manager.get_file_statistic_cache_limit(),
            3_456
        );

        Ok(())
    }

    /// Within one library the runtime environment is shared exactly, so the
    /// config copy never runs.
    #[test]
    fn local_runtime_env_is_shared_exactly() -> Result<()> {
        let original = RuntimeEnvBuilder::new().build_arc()?;
        let ffi = FFI_RuntimeEnv::new(Arc::clone(&original), None);

        let recovered = Arc::<RuntimeEnv>::try_from(&ffi)?;
        assert!(Arc::ptr_eq(&original, &recovered));

        Ok(())
    }
}
