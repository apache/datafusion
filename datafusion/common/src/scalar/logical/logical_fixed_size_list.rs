use crate::error::_internal_err;
use crate::scalar::logical::logical_list::LogicalList;
use crate::scalar::LogicalScalar;
use crate::types::{logical_fixed_size_list, LogicalTypeRef};
use crate::DataFusionError;
use bigdecimal::ToPrimitive;

#[derive(Clone, PartialEq, Eq, Hash, Debug, PartialOrd)]
pub struct LogicalFixedSizeList {
    /// The inner list with a fixed size.
    inner: LogicalList,
}

impl LogicalFixedSizeList {
    /// Tries to create a new [`LogicalFixedSizeList`].
    ///
    /// # Errors
    ///
    /// An error is returned if `list` is too big.
    pub fn try_new(inner: LogicalList) -> crate::Result<Self> {
        if inner.len().to_i32().is_none() {
            return _internal_err!("List too big for fixed-size list.");
        }

        Ok(Self { inner })
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        // LogicalFixedSizeList::new guarantees that the cast succeeds
        logical_fixed_size_list(self.inner.element_field().clone(), self.inner.len() as i32)
    }

    /// Returns the logical values of this list.
    pub fn values(&self) -> &[LogicalScalar] {
        self.inner.values()
    }
}

/// A [LogicalList] may be turned into a [LogicalFixedSizeList].
///
/// # Errors
///
/// Returns an error if the [LogicalList] is too big.
impl TryFrom<LogicalList> for LogicalFixedSizeList {
    type Error = DataFusionError;

    fn try_from(value: LogicalList) -> Result<Self, Self::Error> {
        LogicalFixedSizeList::try_new(value)
    }
}
