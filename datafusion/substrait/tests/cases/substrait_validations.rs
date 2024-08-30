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

#[cfg(test)]
mod tests {
    use crate::utils::test::read_json;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::catalog_common::TableReference;
    use datafusion::common::{DFSchema, Result};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn generate_context_with_table(
        table_name: &str,
        field_data_type_pairs: Vec<(&str, DataType)>,
    ) -> Result<SessionContext> {
        let table_ref = TableReference::bare(table_name);
        let fields: Vec<(Option<TableReference>, Arc<Field>)> = field_data_type_pairs
            .into_iter()
            .map(|pair| {
                let (field_name, data_type) = pair;
                (
                    Some(table_ref.clone()),
                    Arc::new(Field::new(field_name, data_type, false)),
                )
            })
            .collect();

        let df_schema = DFSchema::new_with_metadata(fields, HashMap::default())?;

        let ctx = SessionContext::new();
        ctx.register_table(
            table_ref,
            Arc::new(EmptyTable::new(df_schema.inner().clone())),
        )?;
        Ok(ctx)
    }

    #[tokio::test]
    async fn substrait_schema_validation_ignores_field_name_case() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/simple_select.substrait.json");

        let ctx = generate_context_with_table("DATA", vec![("a", DataType::Int32)])?;
        from_substrait_plan(&ctx, &proto_plan).await?;
        Ok(())
    }

    #[tokio::test]
    async fn reject_plans_with_mismatched_number_of_fields() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/simple_select.substrait.json");

        let ctx = generate_context_with_table(
            "DATA",
            vec![("a", DataType::Int32), ("b", DataType::Int32)],
        )?;
        let res = from_substrait_plan(&ctx, &proto_plan).await;
        assert!(res.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn reject_plans_with_mismatched_field_names() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/simple_select.substrait.json");

        let ctx = generate_context_with_table("DATA", vec![("b", DataType::Date32)])?;
        let res = from_substrait_plan(&ctx, &proto_plan).await;
        assert!(res.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn reject_plans_with_incompatible_field_types() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/simple_select.substrait.json");

        let ctx = generate_context_with_table("DATA", vec![("a", DataType::Date32)])?;
        let res = from_substrait_plan(&ctx, &proto_plan).await;
        assert!(res.is_err());
        Ok(())
    }
}
