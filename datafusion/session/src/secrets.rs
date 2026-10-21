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

//! Session-scoped storage for named secrets.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::secret::Secret;
use datafusion_common::{Result, exec_err};
use parking_lot::RwLock;

/// How registering a secret handles an existing name.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SecretCreateMode {
    /// Return an error without changing the existing secret.
    ErrorIfExists,
    /// Keep the existing secret and return success.
    IgnoreIfExists,
    /// Replace the existing secret.
    Replace,
}

/// Storage for named secrets used by a session.
///
/// Names passed to this API are exact and case-sensitive. Implementations must
/// apply duplicate-name checks and updates atomically, and must not include
/// secret option names or values in diagnostics.
///
/// Access to this interface permits access to plaintext secrets. It does not
/// perform authorization; applications must restrict access to the session and
/// its store. The asynchronous interface also allows implementations backed by
/// an external secrets service.
#[async_trait]
pub trait SecretsStore: Debug + Send + Sync {
    /// Return a secret if its exact name exists.
    ///
    /// The returned value is a snapshot. Replacing or removing the stored secret
    /// does not invalidate values already returned to callers.
    async fn get(&self, name: &str) -> Result<Option<Arc<Secret>>>;

    /// Register a secret, applying `mode` atomically if its name already exists.
    async fn register(
        &self,
        name: &str,
        secret: Secret,
        mode: SecretCreateMode,
    ) -> Result<()>;

    /// Remove a secret by its exact name.
    ///
    /// If it does not exist, return success when `if_exists` is true, and an
    /// error otherwise.
    async fn remove(&self, name: &str, if_exists: bool) -> Result<()>;
}

/// An in-memory [`SecretsStore`].
///
/// Each instance is independent. The store has no persistence, encryption, or
/// zeroization; secret strings remain in process memory and may be retained by
/// callers holding an [`Arc<Secret>`] after a removal or replacement.
/// Its [`Debug`] representation omits the stored names and values.
#[derive(Default)]
pub struct MemorySecretsStore {
    secrets: RwLock<HashMap<String, Arc<Secret>>>,
}

impl MemorySecretsStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Debug for MemorySecretsStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySecretsStore").finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretsStore for MemorySecretsStore {
    async fn get(&self, name: &str) -> Result<Option<Arc<Secret>>> {
        Ok(self.secrets.read().get(name).map(Arc::clone))
    }

    async fn register(
        &self,
        name: &str,
        secret: Secret,
        mode: SecretCreateMode,
    ) -> Result<()> {
        match self.secrets.write().entry(name.to_string()) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(secret));
            }
            Entry::Occupied(mut entry) => match mode {
                SecretCreateMode::ErrorIfExists => {
                    return exec_err!("Secret '{name}' already exists");
                }
                SecretCreateMode::IgnoreIfExists => {}
                SecretCreateMode::Replace => {
                    entry.insert(Arc::new(secret));
                }
            },
        }
        Ok(())
    }

    async fn remove(&self, name: &str, if_exists: bool) -> Result<()> {
        if self.secrets.write().remove(name).is_none() && !if_exists {
            return exec_err!("Secret '{name}' does not exist");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Barrier;
    use std::thread;

    use futures::executor::block_on;

    use super::*;

    fn secret(value: &str) -> Secret {
        Secret::new(BTreeMap::from([(
            "private-option-canary".to_string(),
            value.to_string(),
        )]))
    }

    #[test]
    fn create_modes_and_snapshot_lifetime() -> Result<()> {
        block_on(async {
            let store = MemorySecretsStore::new();
            assert!(store.get("connection").await?.is_none());
            store
                .register(
                    "connection",
                    secret("original"),
                    SecretCreateMode::ErrorIfExists,
                )
                .await?;
            let original = store.get("connection").await?.unwrap();
            let error = store
                .register(
                    "connection",
                    secret("duplicate-value-canary"),
                    SecretCreateMode::ErrorIfExists,
                )
                .await
                .unwrap_err();
            assert!(error.to_string().contains("already exists"));
            assert!(!format!("{error:?}").contains("duplicate-value-canary"));

            store
                .register(
                    "connection",
                    secret("ignored"),
                    SecretCreateMode::IgnoreIfExists,
                )
                .await?;
            assert_eq!(store.get("connection").await?, Some(Arc::clone(&original)));

            store
                .register(
                    "connection",
                    secret("replacement"),
                    SecretCreateMode::Replace,
                )
                .await?;
            assert_eq!(
                store
                    .get("connection")
                    .await?
                    .unwrap()
                    .get("private-option-canary"),
                Some("replacement")
            );
            assert_eq!(original.get("private-option-canary"), Some("original"));

            store.remove("connection", false).await?;
            assert!(store.get("connection").await?.is_none());
            assert_eq!(original.get("private-option-canary"), Some("original"));
            store.remove("connection", true).await?;
            let error = store.remove("connection", false).await.unwrap_err();
            assert!(error.to_string().contains("does not exist"));
            Ok(())
        })
    }

    #[test]
    fn missing_secret_is_created_for_each_mode() -> Result<()> {
        block_on(async {
            for mode in [
                SecretCreateMode::ErrorIfExists,
                SecretCreateMode::IgnoreIfExists,
                SecretCreateMode::Replace,
            ] {
                let store = MemorySecretsStore::new();
                store.register("connection", secret("value"), mode).await?;
                assert_eq!(
                    store
                        .get("connection")
                        .await?
                        .unwrap()
                        .get("private-option-canary"),
                    Some("value")
                );
            }
            Ok(())
        })
    }

    #[test]
    fn stores_are_independent_and_names_are_case_sensitive() -> Result<()> {
        block_on(async {
            let store = MemorySecretsStore::new();
            let other = MemorySecretsStore::new();
            store
                .register(
                    "connection",
                    secret("lower"),
                    SecretCreateMode::ErrorIfExists,
                )
                .await?;
            store
                .register(
                    "CONNECTION",
                    secret("upper"),
                    SecretCreateMode::ErrorIfExists,
                )
                .await?;
            assert_eq!(
                store
                    .get("connection")
                    .await?
                    .unwrap()
                    .get("private-option-canary"),
                Some("lower")
            );
            assert_eq!(
                store
                    .get("CONNECTION")
                    .await?
                    .unwrap()
                    .get("private-option-canary"),
                Some("upper")
            );
            assert!(store.get("Connection").await?.is_none());
            assert!(other.get("connection").await?.is_none());
            Ok(())
        })
    }

    #[test]
    fn debug_does_not_expose_stored_names_or_options() -> Result<()> {
        block_on(async {
            let store = MemorySecretsStore::new();
            store
                .register(
                    "private-name-canary",
                    secret("private-value-canary"),
                    SecretCreateMode::ErrorIfExists,
                )
                .await?;
            for debug in [format!("{store:?}"), format!("{store:#?}")] {
                assert!(!debug.contains("private-name-canary"));
                assert!(!debug.contains("private-option-canary"));
                assert!(!debug.contains("private-value-canary"));
            }
            Ok(())
        })
    }

    #[test]
    fn concurrent_creates_have_one_winner() {
        let store = Arc::new(MemorySecretsStore::new());
        let barrier = Arc::new(Barrier::new(8));
        let threads = (0..8)
            .map(|index| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let result = block_on(store.register(
                        "connection",
                        secret(&format!("candidate-{index}")),
                        SecretCreateMode::ErrorIfExists,
                    ));
                    (index, result)
                })
            })
            .collect::<Vec<_>>();

        let winners = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter_map(|(index, result)| result.ok().map(|()| index))
            .collect::<Vec<_>>();
        assert_eq!(winners.len(), 1);
        assert_eq!(
            block_on(store.get("connection"))
                .unwrap()
                .unwrap()
                .get("private-option-canary"),
            Some(format!("candidate-{}", winners[0]).as_str())
        );
    }

    #[test]
    fn concurrent_removes_have_one_winner() -> Result<()> {
        let store = Arc::new(MemorySecretsStore::new());
        block_on(store.register(
            "connection",
            secret("value"),
            SecretCreateMode::ErrorIfExists,
        ))?;
        let barrier = Arc::new(Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    block_on(store.remove("connection", false))
                })
            })
            .collect::<Vec<_>>();

        let successes = threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .count();
        assert_eq!(successes, 1);
        assert!(block_on(store.get("connection"))?.is_none());
        Ok(())
    }
}
