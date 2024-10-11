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
pub mod test {
    use datafusion::catalog_common::TableReference;
    use datafusion::common::{substrait_datafusion_err, substrait_err};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::TableProvider;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::extensions::Extensions;
    use datafusion_substrait::logical_plan::consumer::from_substrait_named_struct;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use substrait::proto::exchange_rel::ExchangeKind;
    use substrait::proto::expand_rel::expand_field::FieldType;
    use substrait::proto::expression::nested::NestedType;
    use substrait::proto::expression::subquery::SubqueryType;
    use substrait::proto::expression::RexType;
    use substrait::proto::function_argument::ArgType;
    use substrait::proto::read_rel::{NamedTable, ReadType};
    use substrait::proto::rel::RelType;
    use substrait::proto::{Expression, FunctionArgument, Plan, ReadRel, Rel};

    pub fn read_json(path: &str) -> Plan {
        serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json")
    }

    pub fn add_plan_schemas_to_ctx(
        ctx: SessionContext,
        plan: &Plan,
    ) -> Result<SessionContext> {
        let schemas = TestSchemaCollector::collect_schemas(plan)?;
        let mut schema_map: HashMap<TableReference, Arc<dyn TableProvider>> =
            HashMap::new();
        for (table_reference, table) in schemas.into_iter() {
            let schema = table.schema();
            if let Some(existing_table) =
                schema_map.insert(table_reference.clone(), table)
            {
                if existing_table.schema() != schema {
                    return substrait_err!(
                        "Substrait plan contained the same table {} with different schemas.\nSchema 1: {}\nSchema 2: {}",
                        table_reference, existing_table.schema(), schema);
                }
            }
        }
        for (table_reference, table) in schema_map.into_iter() {
            ctx.register_table(table_reference, table)?;
        }
        Ok(ctx)
    }

    pub struct TestSchemaCollector {
        schemas: Vec<(TableReference, Arc<dyn TableProvider>)>,
    }

    impl TestSchemaCollector {
        fn new() -> Self {
            TestSchemaCollector {
                schemas: Vec::new(),
            }
        }

        fn collect_schemas(
            plan: &Plan,
        ) -> Result<Vec<(TableReference, Arc<dyn TableProvider>)>> {
            let mut schema_collector = Self::new();

            for plan_rel in plan.relations.iter() {
                let rel_type = plan_rel
                    .rel_type
                    .as_ref()
                    .ok_or(substrait_datafusion_err!("PlanRel must set rel_type"))?;
                match rel_type {
                    substrait::proto::plan_rel::RelType::Rel(r) => {
                        schema_collector.collect_schemas_from_rel(r)?
                    }
                    substrait::proto::plan_rel::RelType::Root(r) => {
                        let input = r
                            .input
                            .as_ref()
                            .ok_or(substrait_datafusion_err!("RelRoot must set input"))?;
                        schema_collector.collect_schemas_from_rel(input)?
                    }
                }
            }
            Ok(schema_collector.schemas)
        }

        fn collect_named_table(&mut self, read: &ReadRel, nt: &NamedTable) -> Result<()> {
            let table_reference = match nt.names.len() {
                0 => {
                    panic!("No table name found in NamedTable");
                }
                1 => TableReference::Bare {
                    table: nt.names[0].clone().into(),
                },
                2 => TableReference::Partial {
                    schema: nt.names[0].clone().into(),
                    table: nt.names[1].clone().into(),
                },
                _ => TableReference::Full {
                    catalog: nt.names[0].clone().into(),
                    schema: nt.names[1].clone().into(),
                    table: nt.names[2].clone().into(),
                },
            };

            let substrait_schema =
                read.base_schema.as_ref().ok_or(substrait_datafusion_err!(
                    "No base schema found for NamedTable: {}",
                    table_reference
                ))?;
            let empty_extensions = Extensions {
                functions: Default::default(),
                types: Default::default(),
                type_variations: Default::default(),
            };

            let df_schema =
                from_substrait_named_struct(substrait_schema, &empty_extensions)?
                    .replace_qualifier(table_reference.clone());

            let table = EmptyTable::new(df_schema.inner().clone());
            self.schemas.push((table_reference, Arc::new(table)));
            Ok(())
        }

        #[allow(deprecated)]
        fn collect_schemas_from_rel(&mut self, rel: &Rel) -> Result<()> {
            let rel_type = rel
                .rel_type
                .as_ref()
                .ok_or(substrait_datafusion_err!("Rel must set rel_type"))?;
            match rel_type {
                RelType::Read(r) => {
                    let read_type = r
                        .read_type
                        .as_ref()
                        .ok_or(substrait_datafusion_err!("Read must set read_type"))?;
                    match read_type {
                        // Virtual Tables do not contribute to the schema
                        ReadType::VirtualTable(_) => (),
                        ReadType::LocalFiles(_) => todo!(),
                        ReadType::NamedTable(nt) => self.collect_named_table(r, nt)?,
                        ReadType::ExtensionTable(_) => todo!(),
                    }
                    if let Some(expr) = r.filter.as_ref() {
                        self.collect_schemas_from_expr(expr)?
                    };
                    if let Some(expr) = r.best_effort_filter.as_ref() {
                        self.collect_schemas_from_expr(expr)?
                    };
                }
                RelType::Filter(f) => {
                    self.apply(f.input.as_ref().map(|b| b.as_ref()))?;
                    for expr in f.condition.iter() {
                        self.collect_schemas_from_expr(expr)?;
                    }
                }
                RelType::Fetch(f) => {
                    self.apply(f.input.as_ref().map(|b| b.as_ref()))?;
                }
                RelType::Aggregate(a) => {
                    self.apply(a.input.as_ref().map(|b| b.as_ref()))?;
                    for grouping in a.groupings.iter() {
                        for expr in grouping.grouping_expressions.iter() {
                            self.collect_schemas_from_expr(expr)?
                        }
                    }
                    for measure in a.measures.iter() {
                        if let Some(agg_fn) = measure.measure.as_ref() {
                            for arg in agg_fn.arguments.iter() {
                                self.collect_schemas_from_arg(arg)?
                            }
                            for sort in agg_fn.sorts.iter() {
                                if let Some(expr) = sort.expr.as_ref() {
                                    self.collect_schemas_from_expr(expr)?
                                }
                            }
                        }
                        if let Some(expr) = measure.filter.as_ref() {
                            self.collect_schemas_from_expr(expr)?
                        }
                    }
                }
                RelType::Sort(s) => {
                    self.apply(s.input.as_ref().map(|b| b.as_ref()))?;
                    for sort_field in s.sorts.iter() {
                        if let Some(expr) = sort_field.expr.as_ref() {
                            self.collect_schemas_from_expr(expr)?
                        }
                    }
                }
                RelType::Join(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()))?;
                    self.apply(j.right.as_ref().map(|b| b.as_ref()))?;
                    if let Some(expr) = j.expression.as_ref() {
                        self.collect_schemas_from_expr(expr)?;
                    }
                    if let Some(expr) = j.post_join_filter.as_ref() {
                        self.collect_schemas_from_expr(expr)?;
                    }
                }
                RelType::Project(p) => {
                    self.apply(p.input.as_ref().map(|b| b.as_ref()))?
                }
                RelType::Set(s) => {
                    for input in s.inputs.iter() {
                        self.collect_schemas_from_rel(input)?;
                    }
                }
                RelType::ExtensionSingle(s) => {
                    self.apply(s.input.as_ref().map(|b| b.as_ref()))?
                }

                RelType::ExtensionMulti(m) => {
                    for input in m.inputs.iter() {
                        self.collect_schemas_from_rel(input)?
                    }
                }
                RelType::ExtensionLeaf(_) => {}
                RelType::Cross(c) => {
                    self.apply(c.left.as_ref().map(|b| b.as_ref()))?;
                    self.apply(c.right.as_ref().map(|b| b.as_ref()))?;
                }
                // RelType::Reference(_) => {}
                // RelType::Write(_) => {}
                // RelType::Ddl(_) => {}
                RelType::HashJoin(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()))?;
                    self.apply(j.right.as_ref().map(|b| b.as_ref()))?;
                    if let Some(expr) = j.post_join_filter.as_ref() {
                        self.collect_schemas_from_expr(expr)?;
                    }
                }
                RelType::MergeJoin(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()))?;
                    self.apply(j.right.as_ref().map(|b| b.as_ref()))?;
                    if let Some(expr) = j.post_join_filter.as_ref() {
                        self.collect_schemas_from_expr(expr)?;
                    }
                }
                RelType::NestedLoopJoin(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()))?;
                    self.apply(j.right.as_ref().map(|b| b.as_ref()))?;
                    if let Some(expr) = j.expression.as_ref() {
                        self.collect_schemas_from_expr(expr)?;
                    }
                }
                RelType::Window(w) => {
                    self.apply(w.input.as_ref().map(|b| b.as_ref()))?;
                    for wf in w.window_functions.iter() {
                        for arg in wf.arguments.iter() {
                            self.collect_schemas_from_arg(arg)?;
                        }
                    }
                    for expr in w.partition_expressions.iter() {
                        self.collect_schemas_from_expr(expr)?;
                    }
                    for sort_field in w.sorts.iter() {
                        if let Some(expr) = sort_field.expr.as_ref() {
                            self.collect_schemas_from_expr(expr)?;
                        }
                    }
                }
                RelType::Exchange(e) => {
                    self.apply(e.input.as_ref().map(|b| b.as_ref()))?;
                    let exchange_kind = e.exchange_kind.as_ref().ok_or(
                        substrait_datafusion_err!("Exhange must set exchange_kind"),
                    )?;
                    match exchange_kind {
                        ExchangeKind::ScatterByFields(_) => {}
                        ExchangeKind::SingleTarget(st) => {
                            if let Some(expr) = st.expression.as_ref() {
                                self.collect_schemas_from_expr(expr)?
                            }
                        }
                        ExchangeKind::MultiTarget(mt) => {
                            if let Some(expr) = mt.expression.as_ref() {
                                self.collect_schemas_from_expr(expr)?
                            }
                        }
                        ExchangeKind::RoundRobin(_) => {}
                        ExchangeKind::Broadcast(_) => {}
                    }
                }
                RelType::Expand(e) => {
                    self.apply(e.input.as_ref().map(|b| b.as_ref()))?;
                    for expand_field in e.fields.iter() {
                        let expand_type = expand_field.field_type.as_ref().ok_or(
                            substrait_datafusion_err!("ExpandField must set field_type"),
                        )?;
                        match expand_type {
                            FieldType::SwitchingField(sf) => {
                                for expr in sf.duplicates.iter() {
                                    self.collect_schemas_from_expr(expr)?;
                                }
                            }
                            FieldType::ConsistentField(expr) => {
                                self.collect_schemas_from_expr(expr)?
                            }
                        }
                    }
                }
                _ => todo!(),
            }
            Ok(())
        }

        fn apply(&mut self, input: Option<&Rel>) -> Result<()> {
            match input {
                None => Ok(()),
                Some(rel) => self.collect_schemas_from_rel(rel),
            }
        }

        fn collect_schemas_from_expr(&mut self, e: &Expression) -> Result<()> {
            let rex_type = e.rex_type.as_ref().ok_or(substrait_datafusion_err!(
                "rex_type must be set on Expression"
            ))?;
            match rex_type {
                RexType::Literal(_) => {}
                RexType::Selection(_) => {}
                RexType::ScalarFunction(sf) => {
                    for arg in sf.arguments.iter() {
                        self.collect_schemas_from_arg(arg)?
                    }
                }
                RexType::WindowFunction(wf) => {
                    for arg in wf.arguments.iter() {
                        self.collect_schemas_from_arg(arg)?
                    }
                    for sort_field in wf.sorts.iter() {
                        if let Some(expr) = sort_field.expr.as_ref() {
                            self.collect_schemas_from_expr(expr)?
                        }
                    }
                    for expr in wf.partitions.iter() {
                        self.collect_schemas_from_expr(expr)?
                    }
                }
                RexType::IfThen(it) => {
                    for if_clause in it.ifs.iter() {
                        if let Some(expr) = if_clause.r#if.as_ref() {
                            self.collect_schemas_from_expr(expr)?;
                        };
                        if let Some(expr) = if_clause.then.as_ref() {
                            self.collect_schemas_from_expr(expr)?;
                        };
                    }
                    if let Some(expr) = it.r#else.as_ref() {
                        self.collect_schemas_from_expr(expr)?;
                    };
                }
                RexType::SwitchExpression(se) => {
                    if let Some(expr) = se.r#match.as_ref() {
                        self.collect_schemas_from_expr(expr)?
                    }
                    for if_value in se.ifs.iter() {
                        if let Some(expr) = if_value.then.as_ref() {
                            self.collect_schemas_from_expr(expr)?
                        }
                    }
                    if let Some(expr) = se.r#else.as_ref() {
                        self.collect_schemas_from_expr(expr)?
                    }
                }
                RexType::SingularOrList(sol) => {
                    if let Some(expr) = sol.value.as_ref() {
                        self.collect_schemas_from_expr(expr)?
                    }
                    for expr in sol.options.iter() {
                        self.collect_schemas_from_expr(expr)?
                    }
                }
                RexType::MultiOrList(mol) => {
                    for expr in mol.value.iter() {
                        self.collect_schemas_from_expr(expr)?
                    }
                    for record in mol.options.iter() {
                        for expr in record.fields.iter() {
                            self.collect_schemas_from_expr(expr)?
                        }
                    }
                }
                RexType::Cast(c) => {
                    if let Some(expr) = c.input.as_ref() {
                        self.collect_schemas_from_expr(expr)?
                    }
                }
                RexType::Subquery(subquery) => {
                    let subquery_type = subquery
                        .subquery_type
                        .as_ref()
                        .ok_or(substrait_datafusion_err!("subquery_type must be set"))?;
                    match subquery_type {
                        SubqueryType::Scalar(s) => {
                            if let Some(rel) = s.input.as_ref() {
                                self.collect_schemas_from_rel(rel)?;
                            }
                        }
                        SubqueryType::InPredicate(ip) => {
                            for expr in ip.needles.iter() {
                                self.collect_schemas_from_expr(expr)?;
                            }
                            if let Some(rel) = ip.haystack.as_ref() {
                                self.collect_schemas_from_rel(rel)?;
                            }
                        }
                        SubqueryType::SetPredicate(sp) => {
                            if let Some(rel) = sp.tuples.as_ref() {
                                self.collect_schemas_from_rel(rel)?;
                            }
                        }
                        SubqueryType::SetComparison(sc) => {
                            if let Some(expr) = sc.left.as_ref() {
                                self.collect_schemas_from_expr(expr)?;
                            }
                            if let Some(rel) = sc.right.as_ref() {
                                self.collect_schemas_from_rel(rel)?;
                            }
                        }
                    }
                }
                RexType::Nested(n) => {
                    let nested_type = n.nested_type.as_ref().ok_or(
                        substrait_datafusion_err!("Nested must set nested_type"),
                    )?;
                    match nested_type {
                        NestedType::Struct(s) => {
                            for expr in s.fields.iter() {
                                self.collect_schemas_from_expr(expr)?;
                            }
                        }
                        NestedType::List(l) => {
                            for expr in l.values.iter() {
                                self.collect_schemas_from_expr(expr)?;
                            }
                        }
                        NestedType::Map(m) => {
                            for key_value in m.key_values.iter() {
                                if let Some(expr) = key_value.key.as_ref() {
                                    self.collect_schemas_from_expr(expr)?;
                                }
                                if let Some(expr) = key_value.value.as_ref() {
                                    self.collect_schemas_from_expr(expr)?;
                                }
                            }
                        }
                    }
                }
                // Enum is deprecated
                RexType::Enum(_) => {}
            }
            Ok(())
        }

        fn collect_schemas_from_arg(&mut self, fa: &FunctionArgument) -> Result<()> {
            let arg_type = fa.arg_type.as_ref().ok_or(substrait_datafusion_err!(
                "FunctionArgument must set arg_type"
            ))?;
            match arg_type {
                ArgType::Enum(_) => {}
                ArgType::Type(_) => {}
                ArgType::Value(expr) => self.collect_schemas_from_expr(expr)?,
            }
            Ok(())
        }
    }
}
