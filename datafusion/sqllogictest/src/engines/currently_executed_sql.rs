use std::sync::{Arc, Mutex};

/// Hold the currently executed SQL statement.
/// This is used to save the last executed SQL statement in case of a crash.
///
/// This can only hold one SQL statement at a time.
#[derive(Clone)]
pub struct CurrentlyExecutedSqlTracker {
    /// Lock to store the currently executed SQL statement.
    /// It DOES NOT hold the lock for the duration of query execution and only execute the lock
    /// when updating the currently executed SQL statement to allow for saving the last executed SQL
    /// in case of a crash.
    currently_executed_sql: Arc<Mutex<Option<String>>>,
}

impl Default for CurrentlyExecutedSqlTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentlyExecutedSqlTracker {
    pub fn new() -> Self {
        Self {
            currently_executed_sql: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the currently executed SQL statement.
    pub fn set_sql(&self, sql: impl Into<String>) {
        let mut lock = self.currently_executed_sql.lock().unwrap();
        *lock = Some(sql.into());
    }

    /// Get the currently executed SQL statement.
    pub fn get_sql(&self) -> Option<String> {
        let lock = self.currently_executed_sql.lock().unwrap();
        lock.clone()
    }

    /// Clear the currently executed SQL statement only if it matches the provided SQL.
    pub fn clear_sql_if_same(&self, sql: impl Into<String>) {
        let mut lock = self.currently_executed_sql.lock().unwrap();
        if lock.as_ref() != Some(&sql.into()) {
            return;
        }
        *lock = None;
    }
}
