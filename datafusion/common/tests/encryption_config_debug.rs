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

//! Public API regressions for diagnostic formatting of Parquet key configuration.

use std::collections::HashMap;
use std::fmt::Debug;

use datafusion_common::config::{
    ColumnDecryptionProperties, ColumnEncryptionProperties, ConfigField,
    ConfigFileDecryptionProperties, ConfigFileEncryptionProperties, ParquetColumnOptions,
    TableOptions,
};

// Synthetic, distinct AES-128 keys. No external service or credential is used.
const FOOTER_KEY: &str = "000102030405060708090a0b0c0d0e0f";
const FIRST_COLUMN_KEY: &str = "101112131415161718191a1b1c1d1e1f";
const SECOND_COLUMN_KEY: &str = "202122232425262728292a2b2c2d2e2f";
const FOOTER_METADATA: &str = "footer-key-id";
const COLUMN_METADATA: &str = "column-key-id";
const AAD_PREFIX: &str = "file-context";

fn encryption_config() -> ConfigFileEncryptionProperties {
    ConfigFileEncryptionProperties {
        encrypt_footer: false,
        footer_key_as_hex: FOOTER_KEY.to_owned(),
        footer_key_metadata_as_hex: FOOTER_METADATA.to_owned(),
        column_encryption_properties: HashMap::from([
            (
                "first".to_owned(),
                ColumnEncryptionProperties {
                    column_key_as_hex: FIRST_COLUMN_KEY.to_owned(),
                    column_metadata_as_hex: Some(COLUMN_METADATA.to_owned()),
                },
            ),
            (
                "second".to_owned(),
                ColumnEncryptionProperties {
                    column_key_as_hex: SECOND_COLUMN_KEY.to_owned(),
                    column_metadata_as_hex: None,
                },
            ),
        ]),
        aad_prefix_as_hex: AAD_PREFIX.to_owned(),
        store_aad_prefix: true,
    }
}

fn decryption_config() -> ConfigFileDecryptionProperties {
    ConfigFileDecryptionProperties {
        footer_key_as_hex: FOOTER_KEY.to_owned(),
        column_decryption_properties: HashMap::from([
            (
                "first".to_owned(),
                ColumnDecryptionProperties {
                    column_key_as_hex: FIRST_COLUMN_KEY.to_owned(),
                },
            ),
            (
                "second".to_owned(),
                ColumnDecryptionProperties {
                    column_key_as_hex: SECOND_COLUMN_KEY.to_owned(),
                },
            ),
        ]),
        aad_prefix_as_hex: AAD_PREFIX.to_owned(),
        footer_signature_verification: false,
    }
}

fn assert_redacted_debug(value: &impl Debug, visible: &[&str]) {
    for rendered in [format!("{value:?}"), format!("{value:#?}")] {
        for secret in [FOOTER_KEY, FIRST_COLUMN_KEY, SECOND_COLUMN_KEY] {
            assert!(
                !rendered.contains(secret),
                "key material appeared in Debug: {rendered}"
            );
        }
        assert!(rendered.contains("<REDACTED>"), "{rendered}");
        for expected in visible {
            assert!(
                rendered.contains(expected),
                "missing {expected}: {rendered}"
            );
        }
    }
}

#[test]
fn file_encryption_debug_redacts_keys_and_preserves_context() {
    assert_redacted_debug(
        &encryption_config(),
        &[
            "ConfigFileEncryptionProperties",
            "encrypt_footer: false",
            "footer_key_as_hex",
            FOOTER_METADATA,
            COLUMN_METADATA,
            AAD_PREFIX,
            "store_aad_prefix: true",
            "first",
            "second",
        ],
    );
}

#[test]
fn file_decryption_debug_redacts_keys_and_preserves_context() {
    assert_redacted_debug(
        &decryption_config(),
        &[
            "ConfigFileDecryptionProperties",
            "footer_key_as_hex",
            AAD_PREFIX,
            "footer_signature_verification: false",
            "first",
            "second",
        ],
    );
}

#[test]
fn column_encryption_debug_redacts_key_and_preserves_metadata() {
    for config in encryption_config().column_encryption_properties.values() {
        assert_redacted_debug(
            config,
            &[
                "ColumnEncryptionProperties",
                "column_key_as_hex",
                "column_metadata_as_hex",
            ],
        );
    }
}

#[test]
fn column_decryption_debug_redacts_key() {
    for config in decryption_config().column_decryption_properties.values() {
        assert_redacted_debug(
            config,
            &["ColumnDecryptionProperties", "column_key_as_hex"],
        );
    }
}

#[test]
fn table_options_debug_redacts_nested_keys() {
    let mut options = TableOptions::new();
    options.parquet.crypto.file_encryption = Some(encryption_config());
    options.parquet.crypto.file_decryption = Some(decryption_config());
    assert_redacted_debug(&options, &["TableOptions", FOOTER_METADATA, AAD_PREFIX]);
    assert_redacted_debug(&options.parquet, &["TableParquetOptions", COLUMN_METADATA]);
    assert_redacted_debug(
        &options.parquet.crypto,
        &["file_encryption", "file_decryption"],
    );
}

#[test]
fn debug_does_not_change_key_configuration_or_entries() {
    let mut options = TableOptions::new();
    for operation in ["file_encryption", "file_decryption"] {
        for (suffix, value) in [
            ("footer_key_as_hex", FOOTER_KEY),
            ("column_key_as_hex::first", FIRST_COLUMN_KEY),
            ("column_key_as_hex::second", SECOND_COLUMN_KEY),
        ] {
            options
                .parquet
                .set(&format!("crypto.{operation}.{suffix}"), value)
                .unwrap();
        }
    }
    let before = options.clone();
    let _ = format!("{options:?} {options:#?}");
    assert_eq!(before.parquet, options.parquet);
    assert_eq!(
        options
            .parquet
            .crypto
            .file_encryption
            .as_ref()
            .unwrap()
            .footer_key_as_hex,
        FOOTER_KEY
    );
    assert_eq!(
        options
            .parquet
            .crypto
            .file_decryption
            .as_ref()
            .unwrap()
            .column_decryption_properties["second"]
            .column_key_as_hex,
        SECOND_COLUMN_KEY
    );

    // Explicit configuration enumeration remains a data-access API, not a log-safe view.
    let entries = options.entries();
    for operation in ["file_encryption", "file_decryption"] {
        for (suffix, expected) in [
            ("footer_key_as_hex", FOOTER_KEY),
            ("column_key_as_hex::first", FIRST_COLUMN_KEY),
            ("column_key_as_hex::second", SECOND_COLUMN_KEY),
        ] {
            let key = format!("parquet.crypto.{operation}.{suffix}");
            let entry = entries.iter().find(|entry| entry.key == key).unwrap();
            assert_eq!(entry.value.as_deref(), Some(expected));
        }
    }
}

#[test]
fn non_secret_column_options_keep_debug_values() {
    let options = ParquetColumnOptions {
        compression: Some("zstd(3)".to_owned()),
        bloom_filter_enabled: Some(true),
        ..Default::default()
    };
    for rendered in [format!("{options:?}"), format!("{options:#?}")] {
        assert!(rendered.contains("zstd(3)"));
        assert!(rendered.contains("true"));
        assert!(!rendered.contains("<REDACTED>"));
    }
}

#[cfg(feature = "parquet_encryption")]
#[test]
fn debug_preserves_parquet_encryption_conversion_and_roundtrip() {
    use std::sync::Arc;

    use datafusion_common::encryption::{
        FileDecryptionProperties, FileEncryptionProperties,
    };

    // Metadata and AAD are hex-encoded in configuration, unlike the diagnostic-only
    // context strings used by the other tests.
    let mut encrypt = encryption_config();
    encrypt.footer_key_metadata_as_hex = hex::encode(FOOTER_METADATA);
    encrypt.aad_prefix_as_hex = hex::encode(AAD_PREFIX);
    encrypt
        .column_encryption_properties
        .get_mut("first")
        .unwrap()
        .column_metadata_as_hex = Some(hex::encode(COLUMN_METADATA));
    encrypt
        .column_encryption_properties
        .get_mut("second")
        .unwrap()
        .column_metadata_as_hex = Some(hex::encode("second-column-key-id"));
    let mut decrypt = decryption_config();
    decrypt.aad_prefix_as_hex = hex::encode(AAD_PREFIX);

    let _ = format!("{encrypt:?} {encrypt:#?} {decrypt:?} {decrypt:#?}");
    let encrypt_props =
        Arc::new(FileEncryptionProperties::try_from(encrypt.clone()).unwrap());
    assert_eq!(
        ConfigFileEncryptionProperties::from(&encrypt_props),
        encrypt
    );
    let decrypt_props =
        Arc::new(FileDecryptionProperties::try_from(decrypt.clone()).unwrap());
    assert_eq!(
        ConfigFileDecryptionProperties::try_from(&decrypt_props).unwrap(),
        decrypt
    );
}
