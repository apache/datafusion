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

use crate::producer;

use datafusion::error::Result;
use datafusion::prelude::*;

use prost::Message;
use substrait::protobuf::Plan;

use std::fs::OpenOptions;
use std::io::{Read, Write};

pub async fn serialize(sql: &str, ctx: &SessionContext, path: &str) -> Result<()> {
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = producer::to_substrait_plan(&plan)?;

    let mut protobuf_out = Vec::<u8>::new();
    proto.encode(&mut protobuf_out).unwrap();
    let mut file = OpenOptions::new().create(true).write(true).open(path)?;
    file.write_all(&protobuf_out)?;
    Ok(())
}

pub async fn deserialize(path: &str) -> Result<Box<Plan>> {
    let mut protobuf_in = Vec::<u8>::new();

    let mut file = OpenOptions::new().read(true).open(path)?;

    file.read_to_end(&mut protobuf_in)?;
    let proto = Message::decode(&*protobuf_in).unwrap();

    Ok(Box::new(proto))
}
