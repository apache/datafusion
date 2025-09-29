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

use std::any::Any;

use crate::function::map::utils::{
    get_element_type, get_list_offsets, get_list_values,
    map_from_keys_values_offsets_nulls, map_type_from_key_value_types,
};
use arrow::array::{Array, ArrayRef, NullBufferBuilder, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `map_from_entries` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#map_from_entries>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapFromEntries {
    signature: Signature,
}

impl Default for MapFromEntries {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFromEntries {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFromEntries {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_from_entries"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [entries_type] = take_function_args("map_from_entries", arg_types)?;
        let entries_element_type = get_element_type(entries_type)?;
        let (keys_type, values_type) = match entries_element_type {
            DataType::Struct(fields) if fields.len() == 2 => {
                Ok((fields[0].data_type(), fields[1].data_type()))
            }
            wrong_type => exec_err!(
                "map_from_entries: expected array<struct<key, value>>, got {:?}",
                wrong_type
            ),
        }?;
        Ok(map_type_from_key_value_types(keys_type, values_type))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(map_from_entries_inner, vec![])(&args.args)
    }
}

fn map_from_entries_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [entries] = take_function_args("map_from_entries", args)?;
    let entries_offsets = get_list_offsets(entries)?;
    let entries_values = get_list_values(entries)?;

    let (flat_keys, flat_values) =
        match entries_values.as_any().downcast_ref::<StructArray>() {
            Some(a) => Ok((a.column(0), a.column(1))),
            None => exec_err!(
                "map_from_entries: expected array<struct<key, value>>, got {:?}",
                entries_values.data_type()
            ),
        }?;

    let entries_with_nulls = entries_values.nulls().and_then(|entries_inner_nulls| {
        let mut builder = NullBufferBuilder::new_with_len(0);
        let mut cur_offset = entries_offsets
            .first()
            .map(|offset| *offset as usize)
            .unwrap_or(0);

        for next_offset in entries_offsets.iter().skip(1) {
            let num_entries = *next_offset as usize - cur_offset;
            builder.append(
                entries_inner_nulls
                    .slice(cur_offset, num_entries)
                    .null_count()
                    == 0,
            );
            cur_offset = *next_offset as usize;
        }
        builder.finish()
    });

    let res_nulls = NullBuffer::union(entries.nulls(), entries_with_nulls.as_ref());

    map_from_keys_values_offsets_nulls(
        flat_keys,
        flat_values,
        &entries_offsets,
        &entries_offsets,
        None,
        res_nulls.as_ref(),
    )
}
