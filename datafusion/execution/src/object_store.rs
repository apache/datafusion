// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! ObjectStoreRegistry holds all the object stores at Runtime with a scheme for each store.
//! This allows the user to extend DataFusion with different storage systems such as S3 or HDFS
//! and query data inside these systems.

use dashmap::DashMap;
use datafusion_common::{
    DataFusionError, Result, exec_err, internal_datafusion_err, not_impl_err,
};
use object_store::ObjectStore;
#[cfg(not(target_arch = "wasm32"))]
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use url::Url;

/// A parsed URL identifying a particular [`ObjectStore`] instance
///
/// For example:
/// * `file://` for local file system
/// * `s3://bucket` for AWS S3 bucket
/// * `oss://bucket` for Aliyun OSS bucket
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectStoreUrl {
    url: Url,
}

impl ObjectStoreUrl {
    /// Parse an [`ObjectStoreUrl`] from a string
    ///
    /// # Example
    /// ```
    /// # use url::Url;
    /// # use datafusion_execution::object_store::ObjectStoreUrl;
    /// let object_store_url = ObjectStoreUrl::parse("s3://bucket").unwrap();
    /// assert_eq!(object_store_url.as_str(), "s3://bucket/");
    /// // can also access the underlying `Url`
    /// let url: &Url = object_store_url.as_ref();
    /// assert_eq!(url.scheme(), "s3");
    /// assert_eq!(url.host_str(), Some("bucket"));
    /// assert_eq!(url.path(), "/");
    /// ```
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        let mut parsed =
            Url::parse(s.as_ref()).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let remaining = &parsed[url::Position::BeforePath..];
        if !remaining.is_empty() && remaining != "/" {
            return exec_err!(
                "ObjectStoreUrl must only contain scheme and authority, got: {remaining}"
            );
        }

        // Always set path for consistency
        parsed.set_path("/");
        Ok(Self { url: parsed })
    }

    /// An [`ObjectStoreUrl`] for the local filesystem (`file://`)
    ///
    /// # Example
    /// ```
    /// # use datafusion_execution::object_store::ObjectStoreUrl;
    /// let local_fs = ObjectStoreUrl::parse("file://").unwrap();
    /// assert_eq!(local_fs, ObjectStoreUrl::local_filesystem())
    /// ```
    pub fn local_filesystem() -> Self {
        Self::parse("file://").unwrap()
    }

    /// Returns this [`ObjectStoreUrl`] as a string
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Returns a copy of this [`ObjectStoreUrl`] with any username and
    /// password removed, mirroring how [`DefaultObjectStoreRegistry`] ignores
    /// credentials when selecting a store.
    pub(crate) fn without_credentials(&self) -> Self {
        let mut url = self.url.clone();
        // These only fail for URLs that cannot have credentials, in which
        // case there is nothing to remove.
        let _ = url.set_username("");
        let _ = url.set_password(None);
        Self { url }
    }
}

impl AsRef<str> for ObjectStoreUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl AsRef<Url> for ObjectStoreUrl {
    fn as_ref(&self) -> &Url {
        &self.url
    }
}

impl std::fmt::Display for ObjectStoreUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

/// [`ObjectStoreRegistry`] maps a URL to an [`ObjectStore`] instance,
/// and allows DataFusion to read from different [`ObjectStore`]
/// instances. For example DataFusion might be configured so that
///
/// 1. `s3://my_bucket/lineitem/` mapped to the `/lineitem` path on an
///    AWS S3 object store bound to `my_bucket`
///
/// 2. `s3://my_other_bucket/lineitem/` mapped to the (same)
///    `/lineitem` path on a *different* AWS S3 object store bound to
///    `my_other_bucket`
///
/// When given a [`ListingTableUrl`], DataFusion tries to find an
/// appropriate [`ObjectStore`]. For example
///
/// ```sql
/// create external table unicorns stored as parquet location 's3://my_bucket/lineitem/';
/// ```
///
/// In this particular case, the url `s3://my_bucket/lineitem/` will be provided to
/// [`ObjectStoreRegistry::get_store`] and one of three things will happen:
///
/// - If an [`ObjectStore`] has been registered with [`ObjectStoreRegistry::register_store`] with
///   `s3://my_bucket`, that [`ObjectStore`] will be returned
///
/// - If an AWS S3 object store can be ad-hoc discovered by the url `s3://my_bucket/lineitem/`, this
///   object store will be registered with key `s3://my_bucket` and returned.
///
/// - Otherwise an error will be returned, indicating that no suitable [`ObjectStore`] could
///   be found
///
/// This allows for two different use-cases:
///
/// 1. Systems where object store buckets are explicitly created using DDL, can register these
///    buckets using [`ObjectStoreRegistry::register_store`]
///
/// 2. Systems relying on ad-hoc discovery, without corresponding DDL, can create [`ObjectStore`]
///    lazily by providing a custom implementation of [`ObjectStoreRegistry`]
///
/// <!-- is in a different crate so normal rustdoc links don't work -->
/// [`ListingTableUrl`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTableUrl.html
/// [`ObjectStore`]: object_store::ObjectStore
pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static {
    /// If a store with the same key existed before, it is replaced and returned
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>>;

    /// Deregister the store previously registered with the same key. Returns the
    /// deregistered store if it existed.
    #[expect(unused_variables)]
    fn deregister_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        not_impl_err!(
            "ObjectStoreRegistry::deregister_store is not implemented for this ObjectStoreRegistry"
        )
    }

    /// Get a suitable store for the provided URL. For example:
    ///
    /// - URL with scheme `file:///` or no scheme will return the default LocalFS store
    /// - URL with scheme `s3://bucket/` will return the S3 store
    /// - URL with scheme `hdfs://hostname:port/` will return the hdfs store
    ///
    /// If no [`ObjectStore`] found for the `url`, ad-hoc discovery may be executed depending on
    /// the `url` and [`ObjectStoreRegistry`] implementation. An [`ObjectStore`] may be lazily
    /// created and registered.
    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>>;

    /// Returns the [`ObjectStoreUrl`] under which `store` is registered, if
    /// it is known.
    ///
    /// This is the reverse of [`Self::get_store`]. It lets code that is only
    /// handed an [`ObjectStore`], such as `FileFormat::infer_schema`, find the
    /// identity of that store, for example to key the file metadata cache.
    /// Returning `None` is always correct, and makes such callers skip work
    /// that needs the store's identity.
    ///
    /// The default implementation returns `None`.
    #[expect(unused_variables)]
    fn get_store_url(&self, store: &Arc<dyn ObjectStore>) -> Option<ObjectStoreUrl> {
        None
    }
}

/// The default [`ObjectStoreRegistry`]
pub struct DefaultObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    object_stores: DashMap<String, Arc<dyn ObjectStore>>,
}

impl std::fmt::Debug for DefaultObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DefaultObjectStoreRegistry")
            .field(
                "schemes",
                &self
                    .object_stores
                    .iter()
                    .map(|o| o.key().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for DefaultObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultObjectStoreRegistry {
    /// This will register [`LocalFileSystem`] to handle `file://` paths
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new() -> Self {
        let object_stores: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        object_stores.insert("file://".to_string(), Arc::new(LocalFileSystem::new()));
        Self { object_stores }
    }

    /// Default without any backend registered.
    #[cfg(target_arch = "wasm32")]
    pub fn new() -> Self {
        let object_stores: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        Self { object_stores }
    }
}

///
/// Stores are registered based on the scheme, host and port of the provided URL
/// with a [`LocalFileSystem::new`] automatically registered for `file://` (if the
/// target arch is not `wasm32`).
///
/// For example:
///
/// - `file:///my_path` will return the default LocalFS store
/// - `s3://bucket/path` will return a store registered with `s3://bucket` if any
/// - `hdfs://host:port/path` will return a store registered with `hdfs://host:port` if any
impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let s = get_url_key(url);
        self.object_stores.insert(s, store)
    }

    fn deregister_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let s = get_url_key(url);
        let (_, object_store) = self.object_stores
            .remove(&s)
            .ok_or_else(|| {
                internal_datafusion_err!("Failed to deregister object store. No suitable object store found for {url}. See `RuntimeEnv::register_object_store`")
            })?;

        Ok(object_store)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let s = get_url_key(url);
        self.object_stores
            .get(&s)
            .map(|o| Arc::clone(o.value()))
            .ok_or_else(|| {
                internal_datafusion_err!("No suitable object store found for {url}. See `RuntimeEnv::register_object_store`")
            })
    }

    /// Finds the registration of the same `Arc` as `store`. If it is
    /// registered under several URLs, the smallest one is returned so the
    /// answer is deterministic.
    fn get_store_url(&self, store: &Arc<dyn ObjectStore>) -> Option<ObjectStoreUrl> {
        let target = Arc::as_ptr(store).cast::<()>();
        self.object_stores
            .iter()
            .filter(|entry| Arc::as_ptr(entry.value()).cast::<()>() == target)
            .map(|entry| entry.key().clone())
            .min()
            .and_then(|key| ObjectStoreUrl::parse(key).ok())
    }
}

/// Get the key of a url for object store registration.
/// The credential info will be removed
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn test_object_store_url() {
        let file = ObjectStoreUrl::parse("file://").unwrap();
        assert_eq!(file.as_str(), "file:///");

        let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
        assert_eq!(url.as_str(), "s3://bucket/");

        let url = ObjectStoreUrl::parse("s3://username:password@host:123").unwrap();
        assert_eq!(url.as_str(), "s3://username:password@host:123/");

        let err = ObjectStoreUrl::parse("s3://bucket:invalid").unwrap_err();
        assert_eq!(err.strip_backtrace(), "External error: invalid port number");

        let err = ObjectStoreUrl::parse("s3://bucket?").unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: ObjectStoreUrl must only contain scheme and authority, got: ?"
        );

        let err = ObjectStoreUrl::parse("s3://bucket?foo=bar").unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: ObjectStoreUrl must only contain scheme and authority, got: ?foo=bar"
        );

        let err = ObjectStoreUrl::parse("s3://host:123/foo").unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: ObjectStoreUrl must only contain scheme and authority, got: /foo"
        );

        let err =
            ObjectStoreUrl::parse("s3://username:password@host:123/foo").unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: ObjectStoreUrl must only contain scheme and authority, got: /foo"
        );
    }

    #[test]
    fn test_get_store_url() {
        let registry = DefaultObjectStoreRegistry::new();
        let store_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let unregistered: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let url_a = ObjectStoreUrl::parse("memory://a").unwrap();
        let url_b = ObjectStoreUrl::parse("memory://b").unwrap();
        registry.register_store(url_a.as_ref(), Arc::clone(&store_a));
        registry.register_store(url_b.as_ref(), Arc::clone(&store_b));

        assert_eq!(registry.get_store_url(&store_a), Some(url_a));
        assert_eq!(registry.get_store_url(&store_b), Some(url_b.clone()));
        assert_eq!(registry.get_store_url(&unregistered), None);

        // The store returned by `get_store` maps back to its URL
        let fetched = registry.get_store(url_b.as_ref()).unwrap();
        assert_eq!(registry.get_store_url(&fetched), Some(url_b));
    }

    #[test]
    fn test_object_store_url_without_credentials() {
        let url = ObjectStoreUrl::parse("s3://username:password@host:123").unwrap();
        assert_eq!(url.without_credentials().as_str(), "s3://host:123/");
        let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
        assert_eq!(url.without_credentials(), url);
    }

    #[test]
    fn test_get_url_key() {
        let file = ObjectStoreUrl::parse("file://").unwrap();
        let key = get_url_key(&file.url);
        assert_eq!(key.as_str(), "file://");

        let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
        let key = get_url_key(&url.url);
        assert_eq!(key.as_str(), "s3://bucket");

        let url = ObjectStoreUrl::parse("s3://username:password@host:123").unwrap();
        let key = get_url_key(&url.url);
        assert_eq!(key.as_str(), "s3://host:123");
    }
}
