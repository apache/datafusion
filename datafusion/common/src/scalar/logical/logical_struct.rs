use crate::error::_internal_err;
use crate::scalar::LogicalScalar;
use crate::types::{logical_struct, LogicalFields, LogicalTypeRef};
use crate::{HashMap, Result};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Represents a logical struct.
///
/// A struct consists of fields, each of which holding a child [LogicalScalar].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogicalStruct {
    fields: LogicalFields,
    values: HashMap<String, LogicalScalar>,
}

impl LogicalStruct {
    /// Tries to create a new [LogicalStruct].
    ///
    /// # Errors
    ///
    /// Returns an error in the following cases:
    /// - If `fields.len()` is not equal to `values.len()`
    /// - The field names do not pair with the keys in `values`
    /// - One of the values is incompatible with the corresponding field. See
    ///   [LogicalScalar::is_compatible_with_field] for details.
    pub fn try_new(
        fields: LogicalFields,
        values: HashMap<String, LogicalScalar>,
    ) -> Result<Self> {
        if fields.len() != values.len() {
            return _internal_err!("Number of fields does not equal number of values");
        }

        let all_fields_compatible = fields.iter().all(|f| {
            values
                .get(&f.name)
                .map(|v| v.is_compatible_with_field(f))
                .unwrap_or(false) // Incompatible if we don't find a matching value
        });
        if !all_fields_compatible {
            return _internal_err!(
                "Given values are incompatible with the given fields."
            );
        }

        Ok(Self { fields, values })
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_struct(self.fields.clone())
    }
}

impl Hash for LogicalStruct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.values.iter().for_each(|v| v.hash(state))
    }
}

impl PartialOrd for LogicalStruct {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
    }
}
