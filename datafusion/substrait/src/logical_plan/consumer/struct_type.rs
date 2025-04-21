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

use super::r#type::from_substrait_type;
use super::utils::next_struct_field_name;
use super::SubstraitConsumer;
use datafusion::arrow::datatypes::{Field, Fields};
use substrait::proto::r#type;

pub(super) fn from_substrait_struct_type(
    consumer: &impl SubstraitConsumer,
    s: &r#type::Struct,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> datafusion::common::Result<Fields> {
    let mut fields = vec![];
    for (i, f) in s.types.iter().enumerate() {
        let field = Field::new(
            next_struct_field_name(i, dfs_names, name_idx)?,
            from_substrait_type(consumer, f, dfs_names, name_idx)?,
            true, // We assume everything to be nullable since that's easier than ensuring it matches
        );
        fields.push(field);
    }
    Ok(fields.into())
}
