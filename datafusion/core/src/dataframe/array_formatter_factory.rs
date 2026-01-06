use arrow::array::Array;
use arrow::util::display::{ArrayFormatter, ArrayFormatterFactory, FormatOptions};
use arrow_schema::{ArrowError, Field};
use datafusion_expr::registry::ExtensionTypeRegistry;
use std::sync::Arc;

/// A factory for creating [`ArrayFormatter`]s that checks whether a registered extension type can
/// format a given array based on its metadata.
#[derive(Debug)]
pub struct DFArrayFormatterFactory {
    /// The extension type registry
    registry: Arc<dyn ExtensionTypeRegistry>,
}

impl DFArrayFormatterFactory {
    /// Creates a new [`DFArrayFormatterFactory`].
    pub fn new(registry: Arc<dyn ExtensionTypeRegistry>) -> Self {
        Self { registry }
    }
}

impl ArrayFormatterFactory for DFArrayFormatterFactory {
    fn create_array_formatter<'formatter>(
        &self,
        array: &'formatter dyn Array,
        options: &FormatOptions<'formatter>,
        field: Option<&'formatter Field>,
    ) -> Result<Option<ArrayFormatter<'formatter>>, ArrowError> {
        let Some(field) = field else {
            return Ok(None);
        };

        let Some(extension_type_name) = field.extension_type_name() else {
            return Ok(None);
        };

        // TODO: I am not too please about the error handling here

        let Some(registration) = self.registry.extension_type(extension_type_name).ok()
        else {
            // If the extension type is not registered, we fall back to the default formatter
            return Ok(None);
        };
        let extension_type = registration
            .create_logical_type(extension_type_name, field.extension_type_metadata())?;

        extension_type
            .create_array_formatter(array, options)
            .map_err(ArrowError::from)
    }
}
