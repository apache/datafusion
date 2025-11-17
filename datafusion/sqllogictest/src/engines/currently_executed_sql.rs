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

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

/// Hold the currently executed SQL statements.
/// This is used to save the currently running SQLs in case of a crash.
#[derive(Clone)]
pub struct CurrentlyExecutingSqlTracker {
    /// The index of the SQL statement.
    /// Used to uniquely identify each SQL statement even if they are the same.
    sql_index: Arc<AtomicUsize>,
    /// Lock to store the currently executed SQL statement.
    /// It DOES NOT hold the lock for the duration of query execution and only execute the lock
    /// when updating the currently executed SQL statement to allow for saving the last executed SQL
    /// in case of a crash.
    currently_executed_sqls: Arc<Mutex<HashMap<usize, String>>>,
}

impl Default for CurrentlyExecutingSqlTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentlyExecutingSqlTracker {
    pub fn new() -> Self {
        Self {
            sql_index: Arc::new(AtomicUsize::new(0)),
            currently_executed_sqls: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set the currently executed SQL statement.
    ///
    /// Returns a key to use to remove the SQL statement when done.
    ///
    /// We are not returning a guard that will automatically remove the SQL statement when dropped.
    /// as on panic the drop can be called, and it will remove the SQL statement before we can log it.
    #[must_use = "The returned index must be used to remove the SQL statement when done."]
    pub fn set_sql(&self, sql: impl Into<String>) -> usize {
        let index = self
            .sql_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut lock = self.currently_executed_sqls.lock().unwrap();
        lock.insert(index, sql.into());
        drop(lock);
        index
    }

    /// Remove the currently executed SQL statement by the provided key that was returned by [`Self::set_sql`].
    pub fn remove_sql(&self, index: usize) {
        let mut lock = self.currently_executed_sqls.lock().unwrap();
        lock.remove(&index);
    }

    /// Get the currently executed SQL statements.
    pub fn get_currently_running_sqls(&self) -> Vec<String> {
        let lock = self.currently_executed_sqls.lock().unwrap();
        lock.values().cloned().collect()
    }
}
