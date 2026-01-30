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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use crate::{DFSchema, Result, TableReference};

/// Reserved metadata column names used internally by DataFusion.
pub const INPUT_FILE_NAME_COL: &str = "__datafusion_input_file_name";

pub fn append_input_file_name_field(
    schema: &DFSchema,
    qualifier: Option<TableReference>,
) -> Result<DFSchema> {
    if schema
        .fields()
        .iter()
        .any(|field| field.name() == INPUT_FILE_NAME_COL)
    {
        return Ok(schema.clone());
    }

    let mut fields: Vec<(Option<TableReference>, Arc<Field>)> = schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect();

    fields.push((
        qualifier,
        Arc::new(Field::new(INPUT_FILE_NAME_COL, DataType::Utf8, false)),
    ));

    let mut new_schema = DFSchema::new_with_metadata(fields, schema.metadata().clone())?;
    new_schema = new_schema
        .with_functional_dependencies(schema.functional_dependencies().clone())?;
    Ok(new_schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_input_file_name_field_appends_and_dedups() -> Result<()> {
        let fields = vec![(None, Arc::new(Field::new("c1", DataType::Int32, false)))];
        let schema = DFSchema::new_with_metadata(fields, Default::default())?;

        let updated = append_input_file_name_field(&schema, None)?;
        assert_eq!(updated.fields().len(), 2);
        assert_eq!(updated.field(1).name(), INPUT_FILE_NAME_COL);

        let deduped = append_input_file_name_field(&updated, None)?;
        assert_eq!(deduped.fields().len(), 2);

        Ok(())
    }
}
