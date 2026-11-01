// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod tests {
    use datafusion::error::Result;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::{ConfigOptions, TableOptions};
    use datafusion_execution::config::SessionConfig;
    use datafusion_ffi::config::ExtensionOptionsFFIProvider;
    use datafusion_ffi::tests::config::ExternalConfig;
    use datafusion_ffi::tests::utils::get_module;

    #[test]
    fn test_ffi_config_options_extension() -> Result<()> {
        let module = get_module()?;

        let extension_options = (module.create_extension_options)();

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

        let extension_options = (module.create_extension_options)();

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

        let extension_options = (module.create_extension_options)();

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
