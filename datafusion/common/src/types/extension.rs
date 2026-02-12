use crate::error::Result;
use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use std::fmt::Debug;
use std::sync::Arc;

/// A cheaply cloneable pointer to a [`DFExtensionType`].
pub type DFExtensionTypeRef = Arc<dyn DFExtensionType>;

/// Represents an implementation of a DataFusion extension type, allowing users to customize the
/// behavior of DataFusion for custom extension types.
///
/// Extension types may change the semantics of a column. For example, adding two values of
/// [`DataType::Int64`] is a sensible thing to do. However, if the same data type is annotated with
/// an extension type like `custom.id`, the correct interpretation of a column changes. For example,
/// adding together two `custom.id` values (represented as a 64-bit integer) may no longer make
/// sense.
///
/// Note that while helping users to navigate the semantic gap between the data type and extension
/// types is a goal of this trait, DataFusion's extension type support is still evolving and does
/// not cover all use cases. Currently, the following capabilities can be customized:
/// - Pretty-printing values in record batches
///
/// # Relation to Arrow's `ExtensionType`
///
/// The purpose of Arrow's `ExtensionType` trait, for the time being, is to provide a way to handle
/// metadata of an extension type in a type-safe manner. The trait does not provide any
/// customization options such that users can customize the behavior of any kernels (e.g.,
/// [`DFExtensionType::create_array_formatter`] for formatting record batches). Therefore,
/// downstream users (such as DataFusion) have the flexibility to implement the extension type
/// mechanism according to their needs. [`DFExtensionType`] is DataFusion's implementation of this
/// extension type mechanism.
///
/// Furthermore, Arrow's current trait is not dyn-compatible which we need for implementing
/// extension type registries. In the future, the two implementations may increasingly converge.
///
/// # Example
///
///
pub trait DFExtensionType: Debug + Send + Sync {
    /// Returns an [`ArrayFormatter`] that can format values of this type.
    ///
    /// If `Ok(None)` is returned, the default implementation will be used.
    /// If an error is returned, there was an error creating the formatter.
    fn create_array_formatter<'fmt>(
        &self,
        _array: &'fmt dyn Array,
        _options: &FormatOptions<'fmt>,
    ) -> Result<Option<ArrayFormatter<'fmt>>> {
        Ok(None)
    }
}
