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

//! [`SharedFileState`]: open state shared by the scan pieces of one file.

use std::any::Any;
use std::sync::{Arc, OnceLock};

/// Format-specific "opened file" state shared by all byte-range pieces of
/// one file within a single execution.
///
/// Opening a file involves per-file work that is identical for every piece —
/// for parquet: parsing metadata, evaluating pruning predicates, loading the
/// page index and bloom filters. When a file is split into many small pieces
/// so sibling streams can balance decode work, repeating that work per piece
/// dominates the scan. This cell lets the first piece that finishes opening
/// publish its result; pieces that start later skip straight to decoding.
///
/// Sharing is optimistic: pieces that begin before any result is published
/// simply do the full open themselves (concurrently, exactly as without
/// sharing) — there is no waiting or coordination. The value is type-erased
/// so this crate stays independent of any file format.
#[derive(Debug, Default)]
pub struct SharedFileState {
    cell: OnceLock<Arc<dyn Any + Send + Sync>>,
}

impl SharedFileState {
    /// The published open state, if a piece has finished opening the file.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::clone(self.cell.get()?).downcast::<T>().ok()
    }

    /// Publish the open state. If another piece already published (or a
    /// value of a different type is stored), this call has no effect.
    pub fn set<T: Any + Send + Sync>(&self, value: Arc<T>) {
        let _ = self.cell.set(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_publish_wins() {
        let state = SharedFileState::default();
        assert_eq!(state.get::<String>(), None);

        state.set(Arc::new("first".to_string()));
        state.set(Arc::new("second".to_string()));
        assert_eq!(state.get::<String>().unwrap().as_str(), "first");

        // A different type is not returned
        assert_eq!(state.get::<u64>(), None);
    }
}
