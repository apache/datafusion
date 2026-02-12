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

use arrow::array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow::util::display::{ArrayFormatter, DisplayIndex, FormatOptions, FormatResult};
use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_common::internal_err;
use datafusion_common::types::DFExtensionType;
use datafusion_expr::registry::{
    DefaultExtensionTypeRegistration, ExtensionTypeRegistry, MemoryExtensionTypeRegistry,
};
use std::fmt::Write;
use std::sync::Arc;

/// This example demonstrates using DataFusion's extension type API to create a custom identifier
/// type [`EventIdExtensionType`].
///
/// The following use cases are demonstrated:
/// - Use a custom implementation for pretty-printing data frames.
pub async fn event_id_example() -> Result<()> {
    let ctx = create_session_context()?;
    register_events_table(&ctx).await?;

    // Print the example table with the custom pretty-printer.
    ctx.table("example").await?.show().await
}

/// Creates the DataFusion session context with the custom extension type implementation.
fn create_session_context() -> Result<SessionContext> {
    // Create a registry with a reference to the custom extension type implementation.
    let registry = MemoryExtensionTypeRegistry::new();
    let event_id_registration = DefaultExtensionTypeRegistration::new_arc(|metadata| {
        Ok(EventIdExtensionType(metadata))
    });
    registry.add_extension_type_registration(event_id_registration)?;

    // Set the extension type registry in the session state so that DataFusion can use it.
    let state = SessionStateBuilder::default()
        .with_extension_type_registry(Arc::new(registry))
        .build();
    Ok(SessionContext::new_with_state(state))
}

/// Registers the example table and returns the data frame.
async fn register_events_table(ctx: &SessionContext) -> Result<DataFrame> {
    let schema = example_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from(vec![
                2001000000, 2001000001, 2103000000, 2103000001, 2103000002,
            ])),
            Arc::new(UInt32Array::from(vec![
                2020010000, 2020010001, 2021030000, 2021030001, 2021030002,
            ])),
            Arc::new(StringArray::from(vec![
                "First Event Jan 2020",
                "Second Event Jan 2020",
                "First Event Mar 2021",
                "Second Event Mar 2021",
                "Third Event Mar 2021",
            ])),
        ],
    )?;

    // Register the table and return the data frame.
    ctx.register_batch("example", batch)?;
    ctx.table("example").await
}

/// The schema of the example table.
fn example_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_id_short", DataType::UInt32, false)
            .with_extension_type(EventIdExtensionType(IdYearMode::Short)),
        Field::new("event_id_long", DataType::UInt32, false)
            .with_extension_type(EventIdExtensionType(IdYearMode::Long)),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Represents a 32-bit custom identifier that represents a single event. Using this format is
/// probably not a good idea in practice, but it is useful for demonstrating the API usage.
///
/// An event is constructed of three parts:
/// - The year
/// - The month
/// - An auto-incrementing counter within the month
///
/// For example, the event id `2024-01-0000` represents the first event in 2024.
///
/// # Year Mode
///
/// In addition, each event id can be represented in two modes. A short year mode `24-01-000000` and
/// a long year mode `2024-01-0000`. This showcases how extension types can be parameterized using
/// metadata.
#[derive(Debug)]
pub struct EventIdExtensionType(IdYearMode);

/// Represents whether the id uses the short or long format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdYearMode {
    /// The short year format (e.g., `24-01-000000`). Allows for more events per month.
    Short,
    /// The long year format (e.g., `2024-01-0000`). Allows distinguishing between centuries.
    Long,
}

/// Implementation of [`ExtensionType`] for [`EventIdExtensionType`].
///
/// This is for the arrow-rs side of the API usage. The [`ExtensionType::Metadata`] type provides
/// static guarantees on the deserialized metadata for the extension type. We will use this
/// implementation to read and write the type metadata to arrow [`Field`]s.
///
/// This trait does allow users to customize the behavior of DataFusion for this extension type.
/// This is done in [`DFExtensionType`].
impl ExtensionType for EventIdExtensionType {
    const NAME: &'static str = "custom.event_id";
    type Metadata = IdYearMode;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        // Arrow extension type metadata is encoded as a string. We simply use the lowercase name.
        // In a more involved scenario, more complex serialization formats such as JSON are
        // appropriate.
        Some(format!("{:?}", self.0).to_lowercase())
    }

    fn deserialize_metadata(
        metadata: Option<&str>,
    ) -> std::result::Result<Self::Metadata, ArrowError> {
        match metadata {
            None => Err(ArrowError::InvalidArgumentError(
                "Event id type requires metadata".to_owned(),
            )),
            Some(metadata) => match metadata {
                "short" => Ok(IdYearMode::Short),
                "long" => Ok(IdYearMode::Long),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Invalid metadata for event id type: {metadata}"
                ))),
            },
        }
    }

    fn supports_data_type(
        &self,
        data_type: &DataType,
    ) -> std::result::Result<(), ArrowError> {
        match data_type {
            DataType::UInt32 => Ok(()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid data type: {data_type} for event id type",
            ))),
        }
    }

    fn try_new(
        data_type: &DataType,
        metadata: Self::Metadata,
    ) -> std::result::Result<Self, ArrowError> {
        let instance = Self(metadata);
        instance.supports_data_type(data_type)?; // Check that the data type is supported.
        Ok(instance)
    }
}

/// Implementation of [`ExtensionType`] for [`EventIdExtensionType`].
///
/// This is for the DataFusion side of the API usage. Here users can override the default behavior
/// of DataFusion for supported extension points.
impl DFExtensionType for EventIdExtensionType {
    fn create_array_formatter<'fmt>(
        &self,
        array: &'fmt dyn Array,
        options: &FormatOptions<'fmt>,
    ) -> Result<Option<ArrayFormatter<'fmt>>> {
        if array.data_type() != &DataType::UInt32 {
            return internal_err!("Wrong array type for Event Id");
        }

        // Create the formatter and pass in the year formatting mode of the type
        let display_index = EventIdDisplayIndex {
            array: array.as_any().downcast_ref().unwrap(),
            null_str: options.null(),
            mode: self.0,
        };
        Ok(Some(ArrayFormatter::new(
            Box::new(display_index),
            options.safe(),
        )))
    }
}

/// Pretty printer for event ids.
#[derive(Debug)]
struct EventIdDisplayIndex<'a> {
    array: &'a UInt32Array,
    null_str: &'a str,
    mode: IdYearMode,
}

/// This implements the arrow-rs API for printing individual values of a column. DataFusion will
/// automatically pass in the reference to this implementation if a column is annotated with the
/// extension type metadata.
impl DisplayIndex for EventIdDisplayIndex<'_> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        // Handle nulls first
        if self.array.is_null(idx) {
            write!(f, "{}", self.null_str)?;
            return Ok(());
        }

        let value = self.array.value(idx);

        match self.mode {
            IdYearMode::Short => {
                // Format: YY-MM-CCCCCC
                // Logic:
                //  - The last 6 digits are the counter.
                //  - The next 2 digits are the month.
                //  - The remaining digits are the year.
                let counter = value % 1_000_000;
                let rest = value / 1_000_000;
                let month = rest % 100;
                let year = rest / 100;

                write!(f, "{year:02}-{month:02}-{counter:06}")?;
            }
            IdYearMode::Long => {
                // Format: YYYY-MM-CCCC
                // Logic:
                //  - The last 4 digits are the counter.
                //  - The next 2 digits are the month.
                //  - The remaining digits are the year.
                let counter = value % 10_000;
                let rest = value / 10_000;
                let month = rest % 100;
                let year = rest / 100;

                write!(f, "{year:04}-{month:02}-{counter:04}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn test_print_example_table() -> Result<()> {
        let ctx = create_session_context()?;
        let table = register_events_table(&ctx).await?;

        assert_snapshot!(
            table.to_string().await?,
            @r"
        +----------------+---------------+-----------------------+
        | event_id_short | event_id_long | name                  |
        +----------------+---------------+-----------------------+
        | 20-01-000000   | 2020-01-0000  | First Event Jan 2020  |
        | 20-01-000001   | 2020-01-0001  | Second Event Jan 2020 |
        | 21-03-000000   | 2021-03-0000  | First Event Mar 2021  |
        | 21-03-000001   | 2021-03-0001  | Second Event Mar 2021 |
        | 21-03-000002   | 2021-03-0002  | Third Event Mar 2021  |
        +----------------+---------------+-----------------------+
        "
        );

        Ok(())
    }
}
