// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

#[derive(PartialEq, Debug, Clone)]
pub enum PoolType {
    Greedy,
    Fair,
}

impl FromStr for PoolType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Greedy" | "greedy" => Ok(PoolType::Greedy),
            "Fair" | "fair" => Ok(PoolType::Fair),
            _ => Err(format!("Invalid memory pool type '{s}'")),
        }
    }
}

impl Display for PoolType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PoolType::Greedy => write!(f, "greedy"),
            PoolType::Fair => write!(f, "fair"),
        }
    }
}
