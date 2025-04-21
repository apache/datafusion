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

use super::struct_type::from_substrait_struct_type;
use super::SubstraitConsumer;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{substrait_datafusion_err, substrait_err, DFSchema};
use substrait::proto::NamedStruct;

/// Convert Substrait NamedStruct to DataFusion DFSchemaRef
pub fn from_substrait_named_struct(
    consumer: &impl SubstraitConsumer,
    base_schema: &NamedStruct,
) -> datafusion::common::Result<DFSchema> {
    let mut name_idx = 0;
    let fields = from_substrait_struct_type(
        consumer,
        base_schema.r#struct.as_ref().ok_or_else(|| {
            substrait_datafusion_err!("Named struct must contain a struct")
        })?,
        &base_schema.names,
        &mut name_idx,
    );
    if name_idx != base_schema.names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            base_schema.names.len()
        );
    }
    DFSchema::try_from(Schema::new(fields?))
}
