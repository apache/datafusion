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

use arrow::array::{
    Array, ArrowPrimitiveType, AsArray, Float32Array, Float64Array, PrimitiveArray,
    RecordBatch, StringArray,
};
use arrow::datatypes::{Float32Type, Float64Type};
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
use std::fmt::{Display, Write};
use std::sync::Arc;

/// This example demonstrates using DataFusion's extension type API to create a custom
/// extension type [`TemperatureExtensionType`] for representing different temperature units.
pub async fn temperature_example() -> Result<()> {
    let ctx = create_session_context()?;
    register_temperature_table(&ctx).await?;

    // Print the example table with the custom pretty-printer.
    ctx.table("example").await?.show().await
}

/// Creates the DataFusion session context with the custom extension type implementation.
fn create_session_context() -> Result<SessionContext> {
    let registry = MemoryExtensionTypeRegistry::new_empty();

    // The registration creates a new instance of the extension type with the deserialized metadata.
    let temp_registration =
        DefaultExtensionTypeRegistration::new_arc(|storage_type, metadata| {
            Ok(TemperatureExtensionType::new(
                storage_type.clone(),
                metadata,
            ))
        });
    registry.add_extension_type_registration(temp_registration)?;

    let state = SessionStateBuilder::default()
        .with_extension_type_registry(Arc::new(registry))
        .build();
    Ok(SessionContext::new_with_state(state))
}

/// Registers the example table and returns the data frame.
async fn register_temperature_table(ctx: &SessionContext) -> Result<DataFrame> {
    let schema = example_schema();

    let city_names = Arc::new(StringArray::from(vec![
        "Vienna", "Tokyo", "New York", "Sydney",
    ]));

    // The temperature readings in different units
    let celsius_temps = vec![15.1, 22.5, 18.98, 25.0];
    let fahrenheit_temps = vec![59.18, 72.5, 66.164, 77.0];
    let kelvin_temps = vec![288.25, 295.65, 292.13, 298.15];

    let batch = RecordBatch::try_new(
        schema,
        vec![
            city_names,
            Arc::new(Float64Array::from(celsius_temps)),
            Arc::new(Float64Array::from(fahrenheit_temps)),
            Arc::new(Float32Array::from(kelvin_temps)), // Demonstrate use of different storage type
        ],
    )?;

    ctx.register_batch("example", batch)?;
    ctx.table("example").await
}

/// The schema of the example table.
fn example_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("celsius", DataType::Float64, false).with_extension_type(
            TemperatureExtensionType::new(DataType::Float64, TemperatureUnit::Celsius),
        ),
        Field::new("fahrenheit", DataType::Float64, false).with_extension_type(
            TemperatureExtensionType::new(DataType::Float64, TemperatureUnit::Fahrenheit),
        ),
        Field::new("kelvin", DataType::Float32, false).with_extension_type(
            TemperatureExtensionType::new(DataType::Float32, TemperatureUnit::Kelvin),
        ),
    ]))
}

/// Represents the unit of a temperature reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemperatureUnit {
    Celsius,
    Fahrenheit,
    Kelvin,
}

/// Represents a float that semantically represents a temperature. The temperature can be one of
/// the supported [`TemperatureUnit`]s.
///
/// The unit is realized as an additional extension type metadata and is stored alongside the
/// extension type name in the Arrow field metadata. This metadata can also be stored within files,
/// allowing DataFusion to read temperature data from, for example, Parquet files.
///
/// The field metadata for a Celsius temperature field will look like this (serialized as JSON):
/// ```json
/// {
///     "ARROW:extension:name": "custom.temperature",
///     "ARROW:extension:metadata": "celsius"
/// }
/// ```
///
/// See [the official Arrow documentation](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
/// for more details on the extension type mechanism.
#[derive(Debug)]
pub struct TemperatureExtensionType {
    /// Extension type instances are always for a specific storage type and metadata pairing.
    /// Therefore, we store the storage type.
    storage_type: DataType,
    /// The unit of the temperature.
    temperature_unit: TemperatureUnit,
}

impl TemperatureExtensionType {
    /// Creates a new [`TemperatureExtensionType`].
    pub fn new(storage_type: DataType, temperature_unit: TemperatureUnit) -> Self {
        Self {
            storage_type,
            temperature_unit,
        }
    }
}

/// Implementation of [`ExtensionType`] for [`TemperatureExtensionType`].
///
/// This implements the arrow-rs trait for reading, writing, and validating extension types.
impl ExtensionType for TemperatureExtensionType {
    /// Arrow extension type name that is stored in the `ARROW:extension:name` field.
    const NAME: &'static str = "custom.temperature";
    type Metadata = TemperatureUnit;

    fn metadata(&self) -> &Self::Metadata {
        &self.temperature_unit
    }

    /// Arrow extension type metadata is encoded as a string and stored using the
    /// `ARROW:extension:metadata` key. As we only store the name of the unit, a simple string
    /// suffices. Extension types can store more complex metadata using serialization formats like
    /// JSON.
    fn serialize_metadata(&self) -> Option<String> {
        let s = match self.temperature_unit {
            TemperatureUnit::Celsius => "celsius",
            TemperatureUnit::Fahrenheit => "fahrenheit",
            TemperatureUnit::Kelvin => "kelvin",
        };
        Some(s.to_string())
    }

    /// Inverse operation of [`Self::serialize_metadata`]. This creates the [`TemperatureUnit`]
    /// value from the serialized string.
    fn deserialize_metadata(
        metadata: Option<&str>,
    ) -> std::result::Result<Self::Metadata, ArrowError> {
        match metadata {
            Some("celsius") => Ok(TemperatureUnit::Celsius),
            Some("fahrenheit") => Ok(TemperatureUnit::Fahrenheit),
            Some("kelvin") => Ok(TemperatureUnit::Kelvin),
            Some(other) => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid metadata for temperature type: {other}"
            ))),
            None => Err(ArrowError::InvalidArgumentError(
                "Temperature type requires metadata (unit)".to_owned(),
            )),
        }
    }

    /// Checks that the extension type supports a given [`DataType`].
    fn supports_data_type(
        &self,
        data_type: &DataType,
    ) -> std::result::Result<(), ArrowError> {
        match data_type {
            DataType::Float32 | DataType::Float64 => Ok(()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid data type: {data_type} for temperature type, expected Float32 or Float64",
            ))),
        }
    }

    fn try_new(
        data_type: &DataType,
        metadata: Self::Metadata,
    ) -> std::result::Result<Self, ArrowError> {
        let instance = Self::new(data_type.clone(), metadata);
        instance.supports_data_type(data_type)?;
        Ok(instance)
    }
}

/// Implementation of [`DFExtensionType`] for [`TemperatureExtensionType`].
///
/// This implements the trait for customizing DataFusion.
impl DFExtensionType for TemperatureExtensionType {
    fn storage_type(&self) -> DataType {
        self.storage_type.clone()
    }

    fn serialize_metadata(&self) -> Option<String> {
        ExtensionType::serialize_metadata(self)
    }

    fn create_array_formatter<'fmt>(
        &self,
        array: &'fmt dyn Array,
        options: &FormatOptions<'fmt>,
    ) -> Result<Option<ArrayFormatter<'fmt>>> {
        match self.storage_type {
            DataType::Float32 => {
                let display_index = TemperatureDisplayIndex {
                    array: array.as_primitive::<Float32Type>(),
                    null_str: options.null(),
                    unit: self.temperature_unit,
                };
                Ok(Some(ArrayFormatter::new(
                    Box::new(display_index),
                    options.safe(),
                )))
            }
            DataType::Float64 => {
                let display_index = TemperatureDisplayIndex {
                    array: array.as_primitive::<Float64Type>(),
                    null_str: options.null(),
                    unit: self.temperature_unit,
                };
                Ok(Some(ArrayFormatter::new(
                    Box::new(display_index),
                    options.safe(),
                )))
            }
            _ => internal_err!("Wrong array type for Temperature"),
        }
    }
}

/// Pretty printer for temperatures.
#[derive(Debug)]
struct TemperatureDisplayIndex<'a, TNative: ArrowPrimitiveType<Native: Display>> {
    array: &'a PrimitiveArray<TNative>,
    null_str: &'a str,
    unit: TemperatureUnit,
}

/// Implements the custom display logic.
impl<TNative: ArrowPrimitiveType<Native: Display>> DisplayIndex
    for TemperatureDisplayIndex<'_, TNative>
{
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        if self.array.is_null(idx) {
            write!(f, "{}", self.null_str)?;
            return Ok(());
        }

        let value = self.array.value(idx);
        let suffix = match self.unit {
            TemperatureUnit::Celsius => "°C",
            TemperatureUnit::Fahrenheit => "°F",
            TemperatureUnit::Kelvin => "K",
        };

        write!(f, "{value:.2} {suffix}")?;

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
        let table = register_temperature_table(&ctx).await?;

        assert_snapshot!(
            table.to_string().await?,
            @r"
        +----------+----------+------------+----------+
        | city     | celsius  | fahrenheit | kelvin   |
        +----------+----------+------------+----------+
        | Vienna   | 15.10 °C | 59.18 °F   | 288.25 K |
        | Tokyo    | 22.50 °C | 72.50 °F   | 295.65 K |
        | New York | 18.98 °C | 66.16 °F   | 292.13 K |
        | Sydney   | 25.00 °C | 77.00 °F   | 298.15 K |
        +----------+----------+------------+----------+
        "
        );

        Ok(())
    }
}
