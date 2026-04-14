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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

pub mod boundary_stream;
pub mod file_format;
pub mod source;
pub mod utils;

pub use file_format::*;

#[cfg(test)]
pub(crate) mod test_utils {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::chunked::ChunkedStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    /// Chunk sizes exercised by every parameterised test.
    ///
    /// `usize::MAX` is intentionally included: `ChunkedStore` treats it as
    /// "one chunk containing everything", giving the single-chunk fast path.
    pub const CHUNK_SIZES: &[usize] = &[1, 2, 3, 4, 5, 7, 8, 11, 13, 16, usize::MAX];

    /// Seed a fresh `InMemory` store with `data` and wrap it in a
    /// [`ChunkedStore`] that splits every GET response into `chunk_size`-byte
    /// pieces.
    pub async fn make_chunked_store(
        data: &[u8],
        chunk_size: usize,
    ) -> (Arc<dyn ObjectStore>, Path) {
        let inner = Arc::new(InMemory::new());
        let path = Path::from("test");
        inner
            .put(&path, PutPayload::from(Bytes::copy_from_slice(data)))
            .await
            .unwrap();
        (Arc::new(ChunkedStore::new(inner, chunk_size)), path)
    }
}
