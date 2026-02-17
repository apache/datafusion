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
    use datafusion_ffi::config::extension_options::FFI_ExtensionOptions;
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

        fn extract_config(config: &ConfigOptions) -> ExternalConfig {
            // For our use case of this test, we do not need to check
            // using .get::<ExternalConfig>() but it is left here as an
            // example to users of this crate.
            config
                .extensions
                .get::<ExternalConfig>()
                .map(|v| v.to_owned())
                .or_else(|| {
                    config
                        .extensions
                        .get::<FFI_ExtensionOptions>()
                        .and_then(|ext| ext.to_extension().ok())
                })
                .expect("Should be able to get ExternalConfig")
        }

        // Verify default values are as expected
        let returned_config = extract_config(&config);

        assert_eq!(returned_config, ExternalConfig::default());

        config.set("external_config.is_enabled", "false")?;
        let returned_config = extract_config(&config);
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

        fn extract_options(options: &TableOptions) -> ExternalConfig {
            // For our use case of this test, we do not need to check
            // using .get::<ExternalConfig>() but it is left here as an
            // example to users of this crate.
            options
                .extensions
                .get::<ExternalConfig>()
                .map(|v| v.to_owned())
                .or_else(|| {
                    options
                        .extensions
                        .get::<FFI_ExtensionOptions>()
                        .and_then(|ext| ext.to_extension().ok())
                })
                .expect("Should be able to get ExternalConfig")
        }

        // Verify default values are as expected
        let returned_options = extract_options(&table_options);

        assert_eq!(returned_options, ExternalConfig::default());

        table_options.set("external_config.is_enabled", "false")?;
        let returned_config = extract_options(&table_options);
        assert!(!returned_config.is_enabled);

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

        fn extract_config(config: &SessionConfig) -> ExternalConfig {
            // For our use case of this test, we do not need to check
            // using .get::<ExternalConfig>() but it is left here as an
            // example to users of this crate.

            config
                .options()
                .extensions
                .get::<ExternalConfig>()
                .map(|v| v.to_owned())
                .or_else(|| {
                    config
                        .options()
                        .extensions
                        .get::<FFI_ExtensionOptions>()
                        .and_then(|ext| ext.to_extension().ok())
                })
                .expect("Should be able to get ExternalConfig")
        }

        // Verify default values are as expected
        let returned_config = extract_config(&config);
        assert_eq!(returned_config, ExternalConfig::default());

        config = config.set(
            "external_config.is_enabled",
            &ScalarValue::Boolean(Some(false)),
        );
        let returned_config = extract_config(&config);
        assert!(!returned_config.is_enabled);

        Ok(())
    }
}
