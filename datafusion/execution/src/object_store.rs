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

//! ObjectStoreRegistry holds all the object stores at Runtime with a scheme for each store.
//! This allows the user to extend DataFusion with different storage systems such as S3 or HDFS
//! and query data inside these systems.

use datafusion_common::{
    DataFusionError, Result, exec_err, internal_datafusion_err, not_impl_err,
};
use object_store::ObjectStore;
#[cfg(not(target_arch = "wasm32"))]
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::registry::ObjectStoreRegistry as UpstreamObjectStoreRegistry;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
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

        // An ObjectStoreUrl identifies a store, optionally including a registered
        // path prefix (e.g. `hf://bucket/user/repo`). Query strings and fragments
        // are not part of a store key and are rejected.
        if parsed.query().is_some() || parsed.fragment().is_some() {
            let remaining = &parsed[url::Position::AfterPath..];
            return exec_err!(
                "ObjectStoreUrl must not contain a query or fragment, got: {remaining}"
            );
        }

        // Normalize an empty path to "/" for consistency.
        if parsed.path().is_empty() {
            parsed.set_path("/");
        }
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

    /// Resolve `url` to an [`ObjectStore`] together with the object [`Path`]
    /// *relative to that store*.
    ///
    /// Unlike [`Self::get_store`], this allows a store to be registered under a
    /// path prefix (e.g. `hf://bucket/user/repo`). When resolving a longer URL
    /// (e.g. `hf://bucket/user/repo/data/f.parquet`) the registered prefix is
    /// stripped from the returned path (`data/f.parquet`), so a store rooted at
    /// that prefix is not prefixed twice. The store with the longest matching path
    /// prefix is returned. The store identity (scheme + authority + registered
    /// prefix) can be reconstructed from `url` and the returned path.
    ///
    /// The default implementation preserves the legacy behavior of matching on
    /// scheme + authority only, returning the full URL path unchanged.
    fn resolve(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, Path)> {
        let store = self.get_store(url)?;
        let path = Path::from_url_path(url.path())
            .map_err(|e| internal_datafusion_err!("Invalid object store path: {e}"))?;
        Ok((store, path))
    }
}

/// The default [`ObjectStoreRegistry`].
///
/// This is a thin adapter over [`object_store::registry::DefaultObjectStoreRegistry`],
/// which performs path-segment based longest-prefix matching. This means stores can
/// be registered under a path prefix and coexist under a single scheme + authority:
///
/// - `file:///my_path` returns the local filesystem store
/// - `s3://bucket/path` returns a store registered with `s3://bucket` (authority only)
/// - `hf://bucket/user/repo/data/f.parquet` returns a store registered with
///   `hf://bucket/user/repo`, and [`ObjectStoreRegistry::resolve`] strips the
///   registered prefix from the returned path (`data/f.parquet`)
///
/// Only stores that have been explicitly registered (plus the default
/// `file://` store) are resolved: a URL with no matching registration returns an
/// error rather than lazily creating a store from the environment. This keeps
/// object store configuration explicit and avoids silently picking up ambient
/// (e.g. cloud) credentials.
#[derive(Debug)]
pub struct DefaultObjectStoreRegistry {
    inner: object_store::registry::DefaultObjectStoreRegistry,
    /// Scheme+authority and path segments of every explicitly registered store.
    ///
    /// The inner registry would otherwise lazily create a store from the
    /// environment for any known scheme; tracking registrations lets `resolve`
    /// reject unregistered URLs before that fallback runs.
    ///
    /// `has_registered_match` scans this set linearly on every `resolve`/
    /// `get_store` call. That's intentional: registration counts are expected
    /// to stay in the range of a handful to low hundreds of stores (one per
    /// mounted bucket/prefix, not per query or per file), so a linear scan is
    /// cheaper and simpler than a trie/prefix-map. Revisit only if profiling
    /// shows registration cardinality growing past that range.
    registered: RwLock<HashSet<(String, Vec<String>)>>,
}

impl Default for DefaultObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns `url` with any embedded userinfo (username/password) removed.
///
/// Stores are matched on scheme + host + port only: credentials are not part
/// of a store's identity, so a store registered at a credentialed URL must
/// still resolve for a credential-less lookup on the same host, and vice versa.
fn without_credentials(url: &Url) -> Url {
    let mut stripped = url.clone();
    // These only fail for schemes that cannot have a host (e.g. `data:`),
    // which never applies to object store URLs.
    let _ = stripped.set_username("");
    let _ = stripped.set_password(None);
    stripped
}

/// Splits `url` into its scheme+authority and non-empty path segments, the key
/// under which a store is registered / matched. `url` must already have had
/// its credentials stripped by [`without_credentials`].
fn registration_key(url: &Url) -> (String, Vec<String>) {
    let authority = url[..url::Position::AfterPort].to_string();
    let segments = url
        .path()
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    (authority, segments)
}

/// Returns `true` if some entry in `registered` is a path-segment prefix of
/// `url`'s (under the same scheme+authority), i.e. `url` resolves to a
/// registered store rather than requiring lazy creation. `url` must already
/// have had its credentials stripped by [`without_credentials`].
fn has_registered_match(registered: &HashSet<(String, Vec<String>)>, url: &Url) -> bool {
    let (authority, segments) = registration_key(url);
    registered.iter().any(|(reg_authority, reg_segments)| {
        *reg_authority == authority
            && reg_segments.len() <= segments.len()
            && reg_segments.iter().zip(&segments).all(|(a, b)| a == b)
    })
}

impl DefaultObjectStoreRegistry {
    /// This will register [`LocalFileSystem`] to handle `file://` paths
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new() -> Self {
        let this = Self {
            inner: object_store::registry::DefaultObjectStoreRegistry::new(),
            registered: RwLock::new(HashSet::new()),
        };
        this.register_store(
            &Url::parse("file://").unwrap(),
            Arc::new(LocalFileSystem::new()),
        );
        this
    }

    /// Default without any backend registered.
    #[cfg(target_arch = "wasm32")]
    pub fn new() -> Self {
        Self {
            inner: object_store::registry::DefaultObjectStoreRegistry::new(),
            registered: RwLock::new(HashSet::new()),
        }
    }
}

impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let url = without_credentials(url);
        // Hold the write lock across both the inner registration and the
        // bookkeeping update: otherwise a concurrent `resolve` could observe
        // `url` as "registered" (via `registered`) before the store is
        // actually resolvable in `inner`, and fall through to `inner`'s
        // ambient-environment fallback instead of finding the real store.
        let mut registered = self.registered.write().unwrap();
        let previous = self.inner.register(url.clone(), store);
        registered.insert(registration_key(&url));
        previous
    }

    fn deregister_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let url = without_credentials(url);
        // See `register_store`: hold the write lock across both mutations so
        // a concurrent `resolve` can't observe `url` as still registered after
        // it has actually been removed from `inner`.
        let mut registered = self.registered.write().unwrap();
        let store = self.inner.deregister(&url).ok_or_else(|| {
            internal_datafusion_err!("Failed to deregister object store. No suitable object store found for {url}. See `RuntimeEnv::register_object_store`")
        })?;
        registered.remove(&registration_key(&url));
        Ok(store)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.resolve(url).map(|(store, _)| store)
    }

    fn resolve(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, Path)> {
        let url = without_credentials(url);
        // Hold the read lock across the registration check *and* the inner
        // resolve call: otherwise a concurrent `register_store` could land in
        // between, e.g. letting this call see "not yet registered" and fall
        // through to `inner`'s ambient-environment fallback right as another
        // thread finishes registering the real store, or a concurrent
        // `deregister_store` could let this call see "still registered" for a
        // store another thread just removed.
        let registered = self.registered.read().unwrap();
        if !has_registered_match(&registered, &url) {
            return Err(internal_datafusion_err!(
                "No suitable object store found for {url}. See `RuntimeEnv::register_object_store`"
            ));
        }
        self.inner
            .resolve(&url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::prefix::PrefixStore;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

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

        // A path prefix is now allowed: it identifies a store registered under
        // that prefix (e.g. an OpenDAL store rooted at a specific repo).
        let url = ObjectStoreUrl::parse("s3://host:123/foo").unwrap();
        assert_eq!(url.as_str(), "s3://host:123/foo");

        let url =
            ObjectStoreUrl::parse("s3://username:password@host:123/foo/bar").unwrap();
        assert_eq!(url.as_str(), "s3://username:password@host:123/foo/bar");

        // Query strings and fragments are still rejected.
        let err = ObjectStoreUrl::parse("s3://bucket?").unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: ObjectStoreUrl must not contain a query or fragment, got: ?"
        );

        let err = ObjectStoreUrl::parse("s3://bucket?foo=bar").unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: ObjectStoreUrl must not contain a query or fragment, got: ?foo=bar"
        );
    }

    #[test]
    fn test_registry_resolves_prefixed_store() {
        let registry = DefaultObjectStoreRegistry::new();

        // Register two stores under the SAME authority at different prefixes.
        let repo1 = Arc::new(PrefixStore::new(InMemory::new(), "userA/repo1"))
            as Arc<dyn ObjectStore>;
        let repo2 = Arc::new(PrefixStore::new(InMemory::new(), "userB/repo2"))
            as Arc<dyn ObjectStore>;
        registry.register_store(
            &Url::parse("hf://bucket/userA/repo1").unwrap(),
            Arc::clone(&repo1),
        );
        registry.register_store(
            &Url::parse("hf://bucket/userB/repo2").unwrap(),
            Arc::clone(&repo2),
        );

        // Each query resolves to its own store, with the registered prefix
        // stripped from the returned path.
        let (store, path) = registry
            .resolve(&Url::parse("hf://bucket/userA/repo1/data/f.parquet").unwrap())
            .unwrap();
        assert_eq!(path.as_ref(), "data/f.parquet");
        assert!(Arc::ptr_eq(&store, &repo1));

        let (store, path) = registry
            .resolve(&Url::parse("hf://bucket/userB/repo2/x/y.parquet").unwrap())
            .unwrap();
        assert_eq!(path.as_ref(), "x/y.parquet");
        assert!(Arc::ptr_eq(&store, &repo2));

        // Deregister repo1 and confirm it is gone while repo2 survives.
        registry
            .deregister_store(&Url::parse("hf://bucket/userA/repo1").unwrap())
            .unwrap();
        assert!(
            registry
                .resolve(&Url::parse("hf://bucket/userA/repo1/data/f.parquet").unwrap())
                .is_err()
        );
        let (store, _) = registry
            .resolve(&Url::parse("hf://bucket/userB/repo2/x/y.parquet").unwrap())
            .unwrap();
        assert!(Arc::ptr_eq(&store, &repo2));

        // Deregistering a URL that was never registered is an error.
        assert!(
            registry
                .deregister_store(&Url::parse("hf://bucket/nope").unwrap())
                .is_err()
        );
    }

    #[test]
    fn test_registry_authority_only_store() {
        // A store registered by scheme+authority only serves the whole authority;
        // the full path is returned unchanged (the legacy behavior).
        let registry = DefaultObjectStoreRegistry::new();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&Url::parse("s3://bucket").unwrap(), Arc::clone(&store));

        let (resolved, path) = registry
            .resolve(&Url::parse("s3://bucket/a/b/c.parquet").unwrap())
            .unwrap();
        assert_eq!(path.as_ref(), "a/b/c.parquet");
        assert!(Arc::ptr_eq(&resolved, &store));
    }

    #[test]
    fn test_registry_rejects_unregistered_store() {
        let registry = DefaultObjectStoreRegistry::new();

        // An unregistered scheme+authority is a strict error: the registry must
        // not lazily create a store from the environment.
        let err = registry
            .resolve(&Url::parse("s3://bucket/data/f.parquet").unwrap())
            .unwrap_err();
        assert!(
            err.strip_backtrace()
                .contains("No suitable object store found"),
            "unexpected error: {}",
            err.strip_backtrace()
        );
        assert!(
            registry
                .get_store(&Url::parse("s3://bucket/x").unwrap())
                .is_err()
        );

        // The default `file://` store is registered and still resolves.
        assert!(
            registry
                .get_store(&Url::parse("file:///tmp/x").unwrap())
                .is_ok()
        );

        // Once registered explicitly, the store resolves.
        registry.register_store(
            &Url::parse("s3://bucket").unwrap(),
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
        );
        let (_, path) = registry
            .resolve(&Url::parse("s3://bucket/data/f.parquet").unwrap())
            .unwrap();
        assert_eq!(path.as_ref(), "data/f.parquet");

        // Deregistering restores the strict error.
        registry
            .deregister_store(&Url::parse("s3://bucket").unwrap())
            .unwrap();
        assert!(
            registry
                .resolve(&Url::parse("s3://bucket/data/f.parquet").unwrap())
                .is_err()
        );
    }

    #[test]
    fn test_registry_ignores_credentials() {
        // Credentials are not part of a store's identity: a store registered
        // at a credentialed URL must resolve for a credential-less lookup on
        // the same host, and a store registered without credentials must
        // resolve for a lookup that includes them.
        let registry = DefaultObjectStoreRegistry::new();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(
            &Url::parse("s3://username:password@host:123").unwrap(),
            Arc::clone(&store),
        );

        let (resolved, _) = registry
            .resolve(&Url::parse("s3://host:123/data/f.parquet").unwrap())
            .unwrap();
        assert!(Arc::ptr_eq(&resolved, &store));

        let (resolved, _) = registry
            .resolve(&Url::parse("s3://other:secret@host:123/data/f.parquet").unwrap())
            .unwrap();
        assert!(Arc::ptr_eq(&resolved, &store));

        // Deregistering without credentials removes the store that was
        // registered with them.
        registry
            .deregister_store(&Url::parse("s3://host:123").unwrap())
            .unwrap();
        assert!(
            registry
                .resolve(&Url::parse("s3://host:123/data/f.parquet").unwrap())
                .is_err()
        );
    }

    /// Regression test: `register_store`/`deregister_store` must update the
    /// bookkeeping `registered` set and the inner registry as a single atomic
    /// step. Before the fix, `register_store` marked a URL "registered" before
    /// actually inserting the store into the inner registry, so a `resolve`
    /// racing that window could see "registered" yet find no match in `inner`
    /// and fall through to `inner`'s ambient-environment `parse_url_opts`
    /// fallback, silently returning an unrelated, freshly-created store instead
    /// of erroring or returning the real one.
    ///
    /// Uses the `memory://` scheme (host-less), which `parse_url_opts` can
    /// synthesize with no credentials/config -- exactly the case that would
    /// otherwise mask this race as a "success" rather than an error.
    #[test]
    fn test_registry_concurrent_resolve_never_observes_partial_registration() {
        let registry = Arc::new(DefaultObjectStoreRegistry::new());
        let url = Url::parse("memory:///tenant").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let mismatches = Arc::new(AtomicUsize::new(0));

        const ITERATIONS: usize = 5_000;
        const READERS: usize = 4;

        // Repeatedly registers and deregisters `store` at `url`.
        let writer = {
            let registry = Arc::clone(&registry);
            let url = url.clone();
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    registry.register_store(&url, Arc::clone(&store));
                    let _ = registry.deregister_store(&url);
                }
                // Leave it registered so the final sanity check below is stable.
                registry.register_store(&url, store);
            })
        };

        // Concurrently resolves `url`. Whenever a resolve succeeds, it must
        // return exactly `store` -- never a different (e.g. environment
        // fallback) store.
        let readers: Vec<_> = (0..READERS)
            .map(|_| {
                let registry = Arc::clone(&registry);
                let url = url.clone();
                let expected = Arc::clone(&store);
                let mismatches = Arc::clone(&mismatches);
                thread::spawn(move || {
                    for _ in 0..ITERATIONS {
                        if let Ok((resolved, _)) = registry.resolve(&url)
                            && !Arc::ptr_eq(&resolved, &expected)
                        {
                            mismatches.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for reader in readers {
            reader.join().unwrap();
        }

        assert_eq!(
            mismatches.load(Ordering::Relaxed),
            0,
            "resolve() returned a store other than the one explicitly \
             registered: register_store/resolve are not atomic with respect \
             to each other"
        );
        // Sanity: the registry converged to a consistent, resolvable state.
        let (resolved, _) = registry.resolve(&url).unwrap();
        assert!(Arc::ptr_eq(&resolved, &store));
    }
}
