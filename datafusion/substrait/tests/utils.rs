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
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::extensions::Extensions;
    use datafusion_substrait::logical_plan::consumer::from_substrait_named_struct;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use substrait::proto::read_rel::{NamedTable, ReadType};
    use substrait::proto::rel::RelType;
    use substrait::proto::{Plan, ReadRel, Rel};

    pub fn read_json(path: &str) -> Plan {
        serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json")
    }

    pub fn add_plan_schemas_to_ctx(ctx: SessionContext, plan: &Plan) -> SessionContext {
        let schemas = TestSchemaCollector::collect_schemas(plan);
        for (table_reference, table) in schemas {
            ctx.register_table(table_reference, table)
                .expect("Failed to register table");
        }
        ctx
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

        fn collect_schemas(plan: &Plan) -> Vec<(TableReference, Arc<dyn TableProvider>)> {
            let mut schema_collector = Self::new();

            for plan_rel in plan.relations.iter() {
                match plan_rel
                    .rel_type
                    .as_ref()
                    .expect("PlanRel must set rel_type")
                {
                    substrait::proto::plan_rel::RelType::Rel(r) => {
                        schema_collector.collect_schemas_from_rel(r)
                    }
                    substrait::proto::plan_rel::RelType::Root(r) => schema_collector
                        .collect_schemas_from_rel(
                            r.input.as_ref().expect("RelRoot must set input"),
                        ),
                }
            }
            schema_collector.schemas
        }

        fn collect_named_table(&mut self, read: &ReadRel, nt: &NamedTable) {
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

            let substrait_schema = read
                .base_schema
                .as_ref()
                .expect("No base schema found for NamedTable");
            let empty_extensions = Extensions {
                functions: Default::default(),
                types: Default::default(),
                type_variations: Default::default(),
            };

            let df_schema =
                from_substrait_named_struct(substrait_schema, &empty_extensions)
                    .expect(
                        "Unable to generate DataFusion schema from Substrait NamedStruct",
                    )
                    .replace_qualifier(table_reference.clone());

            let table = EmptyTable::new(df_schema.inner().clone());
            self.schemas.push((table_reference, Arc::new(table)));
        }

        fn collect_schemas_from_rel(&mut self, rel: &Rel) {
            match rel.rel_type.as_ref().unwrap() {
                RelType::Read(r) => match r.read_type.as_ref().unwrap() {
                    // Virtual Tables do not contribute to the schema
                    ReadType::VirtualTable(_) => (),
                    ReadType::LocalFiles(_) => todo!(),
                    ReadType::NamedTable(nt) => self.collect_named_table(r, nt),
                    ReadType::ExtensionTable(_) => todo!(),
                },
                RelType::Filter(f) => self.apply(f.input.as_ref().map(|b| b.as_ref())),
                RelType::Fetch(f) => self.apply(f.input.as_ref().map(|b| b.as_ref())),
                RelType::Aggregate(a) => self.apply(a.input.as_ref().map(|b| b.as_ref())),
                RelType::Sort(s) => self.apply(s.input.as_ref().map(|b| b.as_ref())),
                RelType::Join(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()));
                    self.apply(j.right.as_ref().map(|b| b.as_ref()));
                }
                RelType::Project(p) => self.apply(p.input.as_ref().map(|b| b.as_ref())),
                RelType::Set(s) => {
                    for input in s.inputs.iter() {
                        self.collect_schemas_from_rel(input);
                    }
                }
                RelType::ExtensionSingle(s) => {
                    self.apply(s.input.as_ref().map(|b| b.as_ref()))
                }
                RelType::ExtensionMulti(m) => {
                    for input in m.inputs.iter() {
                        self.collect_schemas_from_rel(input)
                    }
                }
                RelType::ExtensionLeaf(_) => {}
                RelType::Cross(c) => {
                    self.apply(c.left.as_ref().map(|b| b.as_ref()));
                    self.apply(c.right.as_ref().map(|b| b.as_ref()));
                }
                // RelType::Reference(_) => {}
                // RelType::Write(_) => {}
                // RelType::Ddl(_) => {}
                RelType::HashJoin(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()));
                    self.apply(j.right.as_ref().map(|b| b.as_ref()));
                }
                RelType::MergeJoin(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()));
                    self.apply(j.right.as_ref().map(|b| b.as_ref()));
                }
                RelType::NestedLoopJoin(j) => {
                    self.apply(j.left.as_ref().map(|b| b.as_ref()));
                    self.apply(j.right.as_ref().map(|b| b.as_ref()));
                }
                RelType::Window(w) => self.apply(w.input.as_ref().map(|b| b.as_ref())),
                RelType::Exchange(e) => self.apply(e.input.as_ref().map(|b| b.as_ref())),
                RelType::Expand(e) => self.apply(e.input.as_ref().map(|b| b.as_ref())),
                _ => todo!(),
            }
        }

        fn apply(&mut self, input: Option<&Rel>) {
            match input {
                None => {}
                Some(rel) => self.collect_schemas_from_rel(rel),
            }
        }
    }
}
