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
            _ => Err(format!("Invalid memory pool type '{}'", s)),
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
