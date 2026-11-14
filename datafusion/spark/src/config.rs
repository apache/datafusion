// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt;
use std::str::FromStr;

use datafusion_common::config::{ConfigExtension, ConfigField, Visit};
use datafusion_common::extensions_options;
use datafusion_common::{DataFusionError, Result};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MapKeyDedupPolicy {
    #[default]
    Exception,
    LastWin,
}

impl FromStr for MapKeyDedupPolicy {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "EXCEPTION" => Ok(Self::Exception),
            "LAST_WIN" => Ok(Self::LastWin),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid MapKeyDedupPolicy: {other}. Expected one of: EXCEPTION, LAST_WIN"
            ))),
        }
    }
}

impl ConfigField for MapKeyDedupPolicy {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = value.parse()?;
        Ok(())
    }
}

impl fmt::Display for MapKeyDedupPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exception => write!(f, "EXCEPTION"),
            Self::LastWin => write!(f, "LAST_WIN"),
        }
    }
}

extensions_options! {
    pub struct SparkOptions {
        pub map_key_dedup_policy: MapKeyDedupPolicy, default = MapKeyDedupPolicy::Exception
    }
}

impl ConfigExtension for SparkOptions {
    const PREFIX: &'static str = "spark";
}
