// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Options related to how json files should be written

use crate::{
    config::JsonOptions,
    error::{DataFusionError, Result},
    parsers::CompressionTypeVariant,
};

/// Options for writing JSON files
#[derive(Clone, Debug)]
pub struct JsonWriterOptions {
    pub compression: CompressionTypeVariant,
    pub compression_level: Option<u32>,
}

impl JsonWriterOptions {
    pub fn new(compression: CompressionTypeVariant) -> Self {
        Self {
            compression,
            compression_level: None,
        }
    }

    /// Create a new `JsonWriterOptions` with the specified compression and level.
    pub fn new_with_level(
        compression: CompressionTypeVariant,
        compression_level: u32,
    ) -> Self {
        Self {
            compression,
            compression_level: Some(compression_level),
        }
    }
}

impl TryFrom<&JsonOptions> for JsonWriterOptions {
    type Error = DataFusionError;

    fn try_from(value: &JsonOptions) -> Result<Self> {
        Ok(JsonWriterOptions {
            compression: value.compression,
            compression_level: value.compression_level,
        })
    }
}
