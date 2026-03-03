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

/// Add an additional module here for convenience to scope this to only
/// when the feature integration-tests is built
#[cfg(feature = "integration-tests")]
mod tests {
    use datafusion::error::{DataFusionError, Result};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::{ConfigOptions, TableOptions};
    use datafusion_execution::config::SessionConfig;
    use datafusion_ffi::config::ExtensionOptionsFFIProvider;
    use datafusion_ffi::tests::config::ExternalConfig;
    use datafusion_ffi::tests::utils::get_module;

    #[test]
    fn test_ffi_config_options_extension() -> Result<()> {
        let module = get_module()?;

        let extension_options =
            module
                .create_extension_options()
                .ok_or(DataFusionError::NotImplemented(
                    "External test library failed to implement create_extension_options"
                        .to_string(),
                ))?();

        let mut config = ConfigOptions::new();
        config.extensions.insert(extension_options);

        // Verify default values are as expected
        let returned_config: ExternalConfig = config
            .local_or_ffi_extension()
            .expect("should have external config extension");
        assert_eq!(returned_config, ExternalConfig::default());

        config.set("external_config.is_enabled", "false")?;
        let returned_config: ExternalConfig = config
            .local_or_ffi_extension()
            .expect("should have external config extension");
        assert!(!returned_config.is_enabled);

        Ok(())
    }

    #[test]
    fn test_ffi_table_options_extension() -> Result<()> {
        let module = get_module()?;

        let extension_options =
            module
                .create_extension_options()
                .ok_or(DataFusionError::NotImplemented(
                    "External test library failed to implement create_extension_options"
                        .to_string(),
                ))?();

        let mut table_options = TableOptions::new();
        table_options.extensions.insert(extension_options);

        // Verify default values are as expected
        let returned_options: ExternalConfig = table_options
            .local_or_ffi_extension()
            .expect("should have external config extension");

        assert_eq!(returned_options, ExternalConfig::default());

        table_options.set("external_config.is_enabled", "false")?;
        let returned_options: ExternalConfig = table_options
            .local_or_ffi_extension()
            .expect("should have external config extension");
        assert!(!returned_options.is_enabled);

        Ok(())
    }

    #[test]
    fn test_ffi_session_config_options_extension() -> Result<()> {
        let module = get_module()?;

        let extension_options =
            module
                .create_extension_options()
                .ok_or(DataFusionError::NotImplemented(
                    "External test library failed to implement create_extension_options"
                        .to_string(),
                ))?();

        let mut config = SessionConfig::new().with_option_extension(extension_options);

        // Verify default values are as expected
        let returned_config: ExternalConfig = config
            .options()
            .local_or_ffi_extension()
            .expect("should have external config extension");
        assert_eq!(returned_config, ExternalConfig::default());

        config = config.set(
            "external_config.is_enabled",
            &ScalarValue::Boolean(Some(false)),
        );
        let returned_config: ExternalConfig = config
            .options()
            .local_or_ffi_extension()
            .expect("should have external config extension");
        assert!(!returned_config.is_enabled);

        Ok(())
    }
}
