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

//! Tests for reading substrait plans produced by other systems

#[cfg(test)]
mod tests {
    use crate::cases::roundtrip_logical_plan::higher_order_function_ctx;
    use crate::utils::test::{add_plan_schemas_to_ctx, read_json};
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
    use datafusion::assert_batches_sorted_eq;
    use datafusion::common::test_util::format_batches;
    use datafusion::datasource::MemTable;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use datafusion::common::Result;
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn scalar_function_compound_signature() -> Result<()> {
        // DataFusion currently produces Substrait that refers to functions only by their name.
        // However, the Substrait spec requires that functions be identified by their compound signature.
        // This test confirms that DataFusion is able to consume plans following the spec, even though
        // we don't yet produce such plans.
        // Once we start producing plans with compound signatures, this test can be replaced by the roundtrip tests.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (d boolean)" "select not d from data"
        let proto_plan =
            read_json("tests/testdata/test_plans/select_not_bool.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r"
        Projection: NOT DATA.D AS EXPR$0
          TableScan: DATA
        "
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    // Aggregate function compound signature is tested through TPCH plans

    #[tokio::test]
    async fn window_function_compound_signature() -> Result<()> {
        // DataFusion currently produces Substrait that refers to functions only by their name.
        // However, the Substrait spec requires that functions be identified by their compound signature.
        // This test confirms that DataFusion is able to consume plans following the spec, even though
        // we don't yet produce such plans.
        // Once we start producing plans with compound signatures, this test can be replaced by the roundtrip tests.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (d int, part int, ord int)" "select sum(d) OVER (PARTITION BY part ORDER BY ord ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS lead_expr from data"
        let proto_plan =
            read_json("tests/testdata/test_plans/select_window.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r"
        Projection: sum(DATA.D) PARTITION BY [DATA.PART] ORDER BY [DATA.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING AS LEAD_EXPR
          WindowAggr: windowExpr=[[sum(DATA.D) PARTITION BY [DATA.PART] ORDER BY [DATA.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING]]
            TableScan: DATA
        "
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn nested_window_function_in_expression() -> Result<()> {
        // The Substrait Project expression represents:
        // SELECT 1 + count(*) OVER () FROM DATA
        let proto_plan = read_json(
            "tests/testdata/test_plans/nested_window_expression.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r"
        Projection: Int64(1) + count(Int64(1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS EXPR$0
          WindowAggr: windowExpr=[[count(Int64(1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
            TableScan: DATA
        "
                );

        // Trigger execution to ensure the nested window is physically plannable
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn double_window_function() -> Result<()> {
        // Confirms a WindowExpr can be repeated in the same project.
        // This wouldn't normally happen with DF-created plans since CSE would eliminate the duplicate.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (a int)" "select ROW_NUMBER() OVER (), ROW_NUMBER() OVER () AS aliased from data";
        let proto_plan =
            read_json("tests/testdata/test_plans/double_window.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r"
        Projection: row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS EXPR$0, row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW__temp__0 AS ALIASED
          WindowAggr: windowExpr=[[row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
            TableScan: DATA
        "
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn double_window_function_distinct_windows() -> Result<()> {
        // Confirms a single project can have multiple window functions with separate windows in it.
        // This wouldn't normally happen with DF-created plans since logical optimizer would
        // separate them out.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (a int)" "select ROW_NUMBER() OVER (), ROW_NUMBER() OVER (PARTITION BY a) from data";
        let proto_plan = read_json(
            "tests/testdata/test_plans/double_window_distinct_windows.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r"
        Projection: row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS EXPR$0, row_number() PARTITION BY [DATA.A] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS EXPR$1
          WindowAggr: windowExpr=[[row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
            WindowAggr: windowExpr=[[row_number() PARTITION BY [DATA.A] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
              TableScan: DATA
        "
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn null_literal_before_and_after_joins() -> Result<()> {
        // Confirms that literals used before and after a join but for different columns
        // are correctly handled.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table A (a int); create table B (a int, c int); create table C (a int, d int)" "select t.*, C.d, CAST(NULL AS VARCHAR) as e from (select a, CAST(NULL AS VARCHAR) as c from A UNION ALL select a, c from B) t LEFT JOIN C ON t.a = C.a"
        let proto_plan = read_json(
            "tests/testdata/test_plans/disambiguate_literals_with_same_name.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
            plan,
            @r"
        Projection: left.A, left.Utf8(NULL) AS C, right.D, Utf8(NULL) AS Utf8(NULL)__temp__0 AS E
          Left Join: left.A = right.A
            SubqueryAlias: left
              Union
                Projection: A.A, Utf8(NULL)
                  TableScan: A
                Projection: B.A, CAST(B.C AS Utf8)
                  TableScan: B
            SubqueryAlias: right
              TableScan: C
        "
        );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_nullable_lists() -> Result<()> {
        // DataFusion's Substrait consumer treats all lists as nullable, even if the Substrait plan specifies them as non-nullable.
        // That's because implementing the non-nullability consistently is non-trivial.
        // This test confirms that reading a plan with non-nullable lists works as expected.
        let proto_plan =
            read_json("tests/testdata/test_plans/non_nullable_lists.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
                &plan,
            @"Values: (List([1, 2]))"
        );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn intersect_nullability() -> Result<()> {
        // Substrait's set operation rules derive an intersection's nullability from
        // every input, not only the primary one. Each plan below intersects three
        // tables carrying the same six columns, with these nullabilities (`?` marks
        // a nullable column, `~` a column with unspecified nullability, which the
        // consumer reads as nullable):
        //
        //   primary     a? b? c? d? e? f?
        //   secondary   a  b  c? d? e~ f~
        //   secondary   a  b? c  d? e? f
        let rows: [(&str, &[[Option<i64>; 6]]); 3] = [
            (
                "data",
                &[
                    [Some(1), Some(1), Some(1), None, None, Some(1)],
                    [Some(2), None, Some(2), Some(2), Some(2), Some(2)],
                    [Some(3), Some(3), None, Some(3), Some(3), Some(3)],
                    [None, Some(4), Some(4), Some(4), Some(4), Some(4)],
                ],
            ),
            (
                "data2",
                &[
                    [Some(1), Some(1), Some(1), None, None, Some(1)],
                    [Some(3), Some(3), None, Some(3), Some(3), Some(3)],
                ],
            ),
            (
                "data3",
                &[
                    [Some(1), Some(1), Some(1), None, None, Some(1)],
                    [Some(2), None, Some(2), Some(2), Some(2), Some(2)],
                ],
            ),
        ];

        // Schema and field metadata are no part of an input's nullability, so the
        // result must not depend on whether the tables carry any. With metadata,
        // every table describes itself and the secondary tables add keys the
        // primary one lacks: the result has to keep the primary table's metadata
        // where they disagree, including on the columns read from a secondary
        // input, and the physical plan and the batches have to agree with it. The
        // secondary tables share their metadata, as `INTERSECTION_PRIMARY` unions
        // them, and a union of inputs with differing field metadata reports
        // different metadata in its logical and in its physical schema.
        let with_metadata = |schema: &SchemaRef, table: &str| -> SchemaRef {
            let role = if table == "data" { "data" } else { "secondary" };
            let tag = |mut metadata: HashMap<String, String>| {
                if role == "secondary" {
                    metadata.insert("only_in_secondary".to_string(), "yes".to_string());
                }
                metadata
            };
            let fields: Vec<Field> = schema
                .fields()
                .iter()
                .map(|field| {
                    let metadata = HashMap::from([(
                        "column".to_string(),
                        format!("{role}.{}", field.name()),
                    )]);
                    field.as_ref().clone().with_metadata(tag(metadata))
                })
                .collect();
            let metadata = HashMap::from([("table".to_string(), role.to_string())]);
            Arc::new(Schema::new_with_metadata(fields, tag(metadata)))
        };

        let cases = [
            // Nullable in the primary input and in at least one secondary input.
            (
                "intersect_primary_mixed_nullability",
                "a, b?, c?, d?, e?, f?",
                &[
                    "+---+---+---+---+---+---+",
                    "| a | b | c | d | e | f |",
                    "+---+---+---+---+---+---+",
                    "| 1 | 1 | 1 |   |   | 1 |",
                    "| 2 |   | 2 | 2 | 2 | 2 |",
                    "| 3 | 3 |   | 3 | 3 | 3 |",
                    "+---+---+---+---+---+---+",
                ][..],
            ),
            // Required as soon as any input requires it.
            (
                "intersect_multiset_mixed_nullability",
                "a, b, c, d?, e?, f",
                &[
                    "+---+---+---+---+---+---+",
                    "| a | b | c | d | e | f |",
                    "+---+---+---+---+---+---+",
                    "| 1 | 1 | 1 |   |   | 1 |",
                    "+---+---+---+---+---+---+",
                ][..],
            ),
            (
                "intersect_multiset_all_mixed_nullability",
                "a, b, c, d?, e?, f",
                &[
                    "+---+---+---+---+---+---+",
                    "| a | b | c | d | e | f |",
                    "+---+---+---+---+---+---+",
                    "| 1 | 1 | 1 |   |   | 1 |",
                    "+---+---+---+---+---+---+",
                ][..],
            ),
        ];

        for ((file, expected_nullability, expected_rows), tagged) in cases
            .into_iter()
            .flat_map(|case| [(case, false), (case, true)])
        {
            let proto_plan =
                read_json(&format!("tests/testdata/test_plans/{file}.substrait.json"));
            let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
            // Give each table rows, so the batch schemas below come from real batches
            for (table, rows) in rows {
                let schema = ctx.table_provider(table).await?.schema();
                let schema = if tagged {
                    with_metadata(&schema, table)
                } else {
                    schema
                };
                let columns = (0..schema.fields().len())
                    .map(|i| {
                        Arc::new(rows.iter().map(|row| row[i]).collect::<Int64Array>())
                            as ArrayRef
                    })
                    .collect();
                let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
                ctx.deregister_table(table)?;
                ctx.register_table(
                    table,
                    Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
                )?;
            }
            let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

            let nullability = plan
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    format!(
                        "{}{}",
                        field.name(),
                        if field.is_nullable() { "?" } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            assert_eq!(
                nullability, expected_nullability,
                "nullability of {file} (tagged: {tagged})"
            );

            if tagged {
                // Schema-level metadata is a join of both inputs' maps, per
                // `intersect_rel`'s doc comment, so a secondary-only key
                // ("only_in_secondary") can appear alongside the primary
                // table's own keys; only that a conflicting key resolves to
                // the primary table's value is asserted here. Per-field
                // metadata has no such leak and is checked exactly below,
                // and again on the optimized, physical and batch schemas.
                assert_eq!(
                    plan.schema().metadata().get("table"),
                    Some(&"data".to_string()),
                    "schema metadata of {file}"
                );
                for field in plan.schema().fields() {
                    let expected_metadata = HashMap::from([(
                        "column".to_string(),
                        format!("data.{}", field.name()),
                    )]);
                    assert_eq!(
                        field.metadata(),
                        &expected_metadata,
                        "metadata of column {} of {file}",
                        field.name()
                    );
                }
            }

            // The physical plan and the batches it produces must carry the same
            // schema as the *optimized* logical plan, since physical planning
            // runs on the optimizer's output, not on `plan` as
            // `from_substrait_plan` returned it.
            let optimized = ctx.state().optimize(&plan)?;
            let logical_schema = Arc::clone(optimized.schema().inner());
            if tagged {
                for field in logical_schema.fields() {
                    let expected_metadata = HashMap::from([(
                        "column".to_string(),
                        format!("data.{}", field.name()),
                    )]);
                    assert_eq!(
                        field.metadata(),
                        &expected_metadata,
                        "optimized metadata of column {} of {file}",
                        field.name()
                    );
                }
            }
            let df = DataFrame::new(ctx.state(), optimized);
            let physical_plan = df.clone().create_physical_plan().await?;
            assert_eq!(
                physical_plan.schema(),
                logical_schema,
                "physical schema of {file}"
            );
            let batches = df.collect().await?;
            assert!(!batches.is_empty(), "no batches for {file}");
            for batch in &batches {
                assert_eq!(batch.schema(), logical_schema, "batch schema of {file}");
            }
            assert_batches_sorted_eq!(expected_rows, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn multilayer_aggregate() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/multilayer_aggregate.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r"
        Projection: lower(sales.product) AS lower(product), sum(count(sales.product)) AS product_count
          Aggregate: groupBy=[[sales.product]], aggr=[[sum(count(sales.product))]]
            Aggregate: groupBy=[[sales.product]], aggr=[[count(sales.product)]]
              TableScan: sales
        "
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate_name_in_union() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/duplicate_name_in_union.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r"
        Projection: foo AS col1, bar AS col2
          Union
            Projection: foo, bar
              Values: (Int64(100), Int64(200))
            Projection: x, foo
              Values: (Int32(300), Int64(400))
        "
                );

        // Trigger execution to ensure plan validity
        let results = DataFrame::new(ctx.state(), plan).collect().await?;

        assert_snapshot!(
            format_batches(&results)?,
            @r"
        +------+------+
        | col1 | col2 |
        +------+------+
        | 100  | 200  |
        | 300  | 400  |
        +------+------+
        ",
        );

        // also verify that the output schema has unique field names
        let schema = results[0].schema();
        for batch in &results {
            assert_eq!(schema, batch.schema());
        }
        let field_names: HashSet<_> = schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(field_names.len(), schema.fields().len());

        Ok(())
    }

    #[tokio::test]
    async fn nested_list_expressions() -> Result<()> {
        // Tests that a Substrait Nested list expression containing non-literal
        // expressions (column references) uses the make_array UDF.
        let proto_plan =
            read_json("tests/testdata/test_plans/nested_list_expressions.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
            plan,
            @r"
        Projection: make_array(DATA.a, DATA.b) AS my_list
          TableScan: DATA
        "
        );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn higher_order_function() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/higher_order_function.json");
        // ctx already contains the queried table
        let ctx = higher_order_function_ctx().await?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @"
        Projection: array_transform2(make_array(make_array(data3.p1)), (p0, p2) -> array_concat(array_transform2(p0, (p3, p4) -> p3 * p2 * p4), array_transform2(p0, (p5, p6) -> p5 * p2 * p6))) AS array_transform2(make_array(make_array(data3.p1)),(v, i) -> array_concat(array_transform2(v,(v, j) -> v * i * j),array_transform2(v,(v, j) -> v * i * j)))
          TableScan: data3
        "
        );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;
        Ok(())
    }
}
