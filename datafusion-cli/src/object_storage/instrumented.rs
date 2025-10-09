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

use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::ObjectStore;
use url::Url;

/// Provides access to wrapped [`ObjectStore`] instances that record requests for reporting
#[derive(Debug)]
pub struct InstrumentedObjectStoreRegistry {
    inner: Arc<dyn ObjectStoreRegistry>,
}

impl InstrumentedObjectStoreRegistry {
    /// Returns a new [`InstrumentedObjectStoreRegistry`] that wraps the provided
    /// [`ObjectStoreRegistry`]
    pub fn new(registry: Arc<dyn ObjectStoreRegistry>) -> Self {
        Self { inner: registry }
    }
}

impl ObjectStoreRegistry for InstrumentedObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn deregister_store(
        &self,
        url: &Url,
    ) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        self.inner.deregister_store(url)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;

    use super::*;

    #[test]
    fn instrumented_registry() {
        let reg = Arc::new(InstrumentedObjectStoreRegistry::new(Arc::new(
            DefaultObjectStoreRegistry::new(),
        )));
        let store = object_store::memory::InMemory::new();

        let url = "mem://test".parse().unwrap();
        let registered = reg.register_store(&url, Arc::new(store));
        assert!(registered.is_none());

        let fetched = reg.get_store(&url);
        assert!(fetched.is_ok())
    }
}
