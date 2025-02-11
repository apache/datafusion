use crate::error::_internal_err;
use crate::scalar::LogicalScalar;
use crate::types::LogicalTypeRef;
use crate::HashMap;
use crate::Result;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

/// TODO logical-types
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogicalMap {
    values: HashMap<LogicalScalar, LogicalScalar>,
}

impl LogicalMap {
    /// Tries to create a new [LogicalMap].
    ///
    /// # Errors
    ///
    /// Returns an error if a key in the map is null. See [MapArray] for details.
    pub fn try_new(map: HashMap<LogicalScalar, LogicalScalar>) -> Result<Self> {
        if map.keys().any(|k| k.is_null()) {
            return _internal_err!("LogicalMap keys must not be null");
        }
        Ok(Self { values: map })
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        todo!("logical-types: What is the field in MapType?")
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the [LogicalMap] has any entries.
    pub fn is_empty(&self) -> bool {
        self.values.len() == 0
    }
}

impl Hash for LogicalMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (k, v) in &self.values {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl PartialOrd for LogicalMap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

impl Display for LogicalMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for (idx, (key, value)) in self.values.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", key, value)?;
        }
        write!(f, "}}")
    }
}
