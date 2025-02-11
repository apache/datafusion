use crate::error::_internal_err;
use crate::scalar::LogicalScalar;
use crate::types::{logical_struct, LogicalFields, LogicalTypeRef};
use crate::{HashMap, Result};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
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
        if self.fields.len() != self.fields.len() {
            return None;
        }

        if self.logical_type() != other.logical_type() {
            return None;
        }

        for col_index in 0..self.fields.len() {
            let field = self.fields.get(col_index).unwrap();
            let v1 = self
                .values
                .get(field.name())
                .expect("Field resolved via self");
            let v2 = other
                .values
                .get(field.name())
                .expect("Logical types are equal");

            let result = v1.partial_cmp(v2)?;
            if result != Ordering::Equal {
                return Some(result);
            }
        }

        Some(Ordering::Equal)
    }
}

impl Display for LogicalStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for (idx, field) in self.fields.iter().enumerate() {
            let value = &self
                .values
                .get(field.name())
                .expect("Field resolved via self");
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", field.name(), value)?;
        }
        write!(f, "}}")
    }
}
