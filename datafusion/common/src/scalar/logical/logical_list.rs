use std::cmp::Ordering;
use crate::error::_internal_err;
use crate::scalar::LogicalScalar;
use crate::types::{logical_list, LogicalFieldRef, LogicalTypeRef};
use crate::Result;

/// TODO logical-types
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalList {
    /// The logical type and nullability of all list elements.
    element_field: LogicalFieldRef,
    /// The list of contained logical scalars.
    values: Vec<LogicalScalar>,
}

impl LogicalList {
    /// Creates a new [`LogicalList`].
    ///
    /// The logical types of the elements in [`value`] must be equal to [`element_type`]. If
    /// [`nullable`] is true, [`LogicalValue::Null`] is also allowed.
    ///
    /// # Errors
    ///
    /// Returns an error if the elements of [`value`] do not match [`element_type`].
    pub fn try_new(
        element_field: LogicalFieldRef,
        value: Vec<LogicalScalar>,
    ) -> Result<Self> {
        for element in &value {
            let has_matching_type =
                &element.logical_type() == &element_field.logical_type;
            let is_allowed_null =
                element_field.nullable && element.logical_type().native().is_null();

            if !has_matching_type && !is_allowed_null {
                return _internal_err!(
                    "Incompatible element type creating logical list."
                );
            }
        }

        Ok(Self {
            element_field,
            values: value,
        })
    }

    /// Returns a reference to the field of this list.
    pub fn element_field(&self) -> LogicalFieldRef {
        self.element_field.clone()
    }

    /// Returns the logical type of this list.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_list(self.element_field.clone())
    }

    /// Returns the length of this list.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns the logical values of this value.
    pub fn values(&self) -> &[LogicalScalar] {
        self.values.as_slice()
    }
}

impl PartialOrd for LogicalList {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if &self.element_field.logical_type != &other.element_field.logical_type {
            return None;
        }

        if self.len() != other.len() {
            return None;
        }

        for (self_element, other_element) in self.values.iter().zip(other.values.iter()) {
            let result = self_element.partial_cmp(other_element)?;
            if result != Ordering::Equal {
                return Some(result);
            }
        }

        Some(Ordering::Equal)
    }
}