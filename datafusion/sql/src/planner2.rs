#![allow(non_snake_case)]
#![allow(dead_code)]

use antlr_rust::tree::ParseTree;
use datafusion_expr::{EmptyRelation, LogicalPlan, LogicalPlanBuilder, TableSource};

use crate::antlr::presto::prestoparser::*;
use datafusion_common::{
    DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result, TableReference,
};
use std::{
    cell::{Cell, RefCell},
    iter,
    rc::Rc,
    sync::Arc,
};

trait BindingContext {
    fn resolve_table(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        Err(DataFusionError::Plan(format!(
            "No table named: {} found",
            name.table()
        )))
    }
}

struct BindingContextStack {
    stack: RefCell<Vec<Box<dyn BindingContext>>>,
}

impl BindingContextStack {
    fn push(&self, bc: Box<dyn BindingContext>) {
        self.stack.borrow_mut().push(bc);
    }
}

impl BindingContext for BindingContextStack {
    fn resolve_table(&self, table_ref: TableReference) -> Result<Arc<dyn TableSource>> {
        for bc in self.stack.borrow().iter().rev() {
            let result = bc.resolve_table(table_ref);
            if result.is_ok() {
                return result;
            }
        }
        Err(DataFusionError::Plan(format!(
            "No table named: {} found",
            table_ref.table()
        )))
    }
}

fn bind_LogicalPlan_from_singleStatement<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<SingleStatementContextAll<'input>>,
) -> Result<LogicalPlan> {
    bind_LogicalPlan_from_statement(bc, ctx.statement().unwrap())
}

fn bind_LogicalPlan_from_statement<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<StatementContextAll<'input>>,
) -> Result<LogicalPlan> {
    match &*ctx {
        StatementContextAll::StatementDefaultContext(c) => {
            bind_LogicalPlan_from_statementDefault(bc, c)
        }
        // StatmentContextAll::Use
        _ => Err(DataFusionError::NotImplemented(String::from(
            "not implemented bind_LogicalPlan_from_statement",
        ))),
    }
}

fn bind_LogicalPlan_from_statementDefault<'input>(
    bc: Rc<BindingContextStack>,
    ctx: &StatementDefaultContext<'input>,
) -> Result<LogicalPlan> {
    bind_LogicalPlan_from_query(bc, ctx.query().unwrap())
}

fn bind_LogicalPlan_from_query<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<QueryContextAll<'input>>,
) -> Result<LogicalPlan> {
    if ctx.with().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented bind_LogicalPlan_from_query",
        )));
    }
    bind_LogicalPlan_from_queryNoWith(bc, ctx.queryNoWith().unwrap())
}

fn bind_LogicalPlan_from_queryNoWith<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<QueryNoWithContextAll<'input>>,
) -> Result<LogicalPlan> {
    if ctx.sortItem_all().len() > 0 {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented sortItem",
        )));
    }
    if ctx.offset.is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented offset",
        )));
    }
    if ctx.limit.is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented limit",
        )));
    }
    if ctx.FETCH().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented FETCH",
        )));
    }
    bind_LogicalPlan_from_queryTerm(bc, ctx.queryTerm().unwrap())
}

fn bind_LogicalPlan_from_queryTerm<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<QueryTermContextAll<'input>>,
) -> Result<LogicalPlan> {
    match &*ctx {
        QueryTermContextAll::QueryTermDefaultContext(c) => {
            bind_LogicalPlan_from_queryTermDefault(bc, c)
        }
        _ => Err(DataFusionError::NotImplemented(String::from(
            "not implemented bind_LogicalPlan_from_queryTerm",
        ))),
    }
}

fn bind_LogicalPlan_from_queryTermDefault<'input>(
    bc: Rc<BindingContextStack>,
    ctx: &QueryTermDefaultContext<'input>,
) -> Result<LogicalPlan> {
    bind_LogicalPlan_from_queryPrimary(bc, ctx.queryPrimary().unwrap())
}

fn bind_LogicalPlan_from_queryPrimary<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<QueryPrimaryContextAll<'input>>,
) -> Result<LogicalPlan> {
    match &*ctx {
        QueryPrimaryContextAll::QueryPrimaryDefaultContext(c) => {
            bind_LogicalPlan_from_queryPrimaryDefault(bc, c)
        }
        _ => Err(DataFusionError::NotImplemented(String::from(
            "not implemented bind_LogicalPlan_from_queryPrimary",
        ))),
    }
}

fn bind_LogicalPlan_from_queryPrimaryDefault<'input>(
    bc: Rc<BindingContextStack>,
    ctx: &QueryPrimaryDefaultContext<'input>,
) -> Result<LogicalPlan> {
    bind_LogicalPlan_from_querySpecification(bc, ctx.querySpecification().unwrap())
}

fn bind_LogicalPlan_from_querySpecification<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<QuerySpecificationContextAll<'input>>,
) -> Result<LogicalPlan> {
    if ctx.setQuantifier().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented setQuantifier",
        )));
    }
    if ctx.where_.is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented where",
        )));
    }
    if ctx.groupBy().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented groupby",
        )));
    }
    if ctx.having.is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented having",
        )));
    }
    if ctx.windowDefinition_all().len() > 0 {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented windowDefinition",
        )));
    }
    if ctx.relation_all().len() > 1 {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented relation",
        )));
    }
    let parent = if ctx.relation_all().len() > 0 {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: DFSchemaRef::new(DFSchema::empty()),
        })
    } else {
        bind_LogicalPlan_from_relation(bc, ctx.relation(0).unwrap())?
    };

    // TODO
    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: DFSchemaRef::new(DFSchema::empty()),
    }))
}

fn bind_LogicalPlan_from_relation<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<RelationContextAll<'input>>,
) -> Result<LogicalPlan> {
    match &*ctx {
        RelationContextAll::RelationDefaultContext(c) => {
            bind_LogicalPlan_from_relationDefault(bc, c)
        }
        _ => Err(DataFusionError::NotImplemented(String::from(
            "not implemented bind_LogicalPlan_from_relation",
        ))),
    }
}

fn bind_LogicalPlan_from_relationDefault<'input>(
    bc: Rc<BindingContextStack>,
    ctx: &RelationDefaultContext<'input>,
) -> Result<LogicalPlan> {
    bind_LogicalPlan_from_sampledRelation(bc, ctx.sampledRelation().unwrap())
}

fn bind_LogicalPlan_from_sampledRelation<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<SampledRelationContextAll<'input>>,
) -> Result<LogicalPlan> {
    if ctx.sampleType().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented sampleType",
        )));
    }
    bind_LogicalPlan_from_patternRecognition(bc, ctx.patternRecognition().unwrap())
}

fn bind_LogicalPlan_from_patternRecognition<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<PatternRecognitionContextAll<'input>>,
) -> Result<LogicalPlan> {
    if ctx.MATCH_RECOGNIZE().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented MATCH_RECOGNIZE",
        )));
    }
    bind_LogicalPlan_from_aliasedRelation(bc, ctx.aliasedRelation().unwrap())
}

fn bind_LogicalPlan_from_aliasedRelation<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<AliasedRelationContextAll<'input>>,
) -> Result<LogicalPlan> {
    if ctx.identifier().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented identifier in aliasedRelation",
        )));
    }
    bind_LogicalPlan_from_relationPrimary(bc, ctx.relationPrimary().unwrap())
}

fn bind_LogicalPlan_from_relationPrimary<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<RelationPrimaryContextAll<'input>>,
) -> Result<LogicalPlan> {
    match &*ctx {
        RelationPrimaryContextAll::TableNameContext(c) => {
            bind_LogicalPlan_from_tableName(bc, c)
        }
        _ => Err(DataFusionError::NotImplemented(String::from(
            "not implemented bind_LogicalPlan_from_relationPrimary",
        ))),
    }
}

fn bind_LogicalPlan_from_tableName<'input>(
    bc: Rc<BindingContextStack>,
    ctx: &TableNameContext<'input>,
) -> Result<LogicalPlan> {
    if ctx.queryPeriod().is_some() {
        return Err(DataFusionError::NotImplemented(String::from(
            "not implemented queryPeriod",
        )));
    }

    let table_ref_result = bind_OwnedTableReference_from_qualified_name(
        bc.clone(),
        ctx.qualifiedName().unwrap(),
    );
    if table_ref_result.is_err() {
        return Err(table_ref_result.unwrap_err());
    }
    match bc
        .clone()
        .resolve_table(table_ref_result.unwrap().as_table_reference())
    {
        Ok(table_source) => {
            LogicalPlanBuilder::scan(&String::from("B"), table_source, None)?.build()
        }
        Err(e) => Err(e),
    }
}

fn bind_OwnedTableReference_from_qualified_name<'input>(
    bc: Rc<BindingContextStack>,
    ctx: Rc<QualifiedNameContextAll<'input>>,
) -> Result<OwnedTableReference> {
    let identifiers: Vec<_> = ctx
        .identifier_all()
        .iter()
        .map(|i| bind_str_from_identifier(bc.clone(), i))
        .collect();
    if identifiers.len() == 1 {
        Ok(OwnedTableReference::Bare {
            table: identifiers[0].clone(),
        })
    } else if identifiers.len() == 2 {
        Ok(OwnedTableReference::Partial {
            schema: identifiers[0].clone(),
            table: identifiers[1].clone(),
        })
    } else {
        Err(DataFusionError::Plan(
            "Cannot bind TableReference".to_owned(),
        ))
    }
}

fn bind_str_from_identifier<'input>(
    _: Rc<BindingContextStack>,
    ctx: &Rc<IdentifierContextAll<'input>>,
) -> String {
    ctx.get_text()
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::result;
    use std::sync::Arc;

    use crate::antlr::presto::prestolexer::PrestoLexer;
    use crate::antlr::presto::prestoparser::{PrestoParser, SingleStatementContextAll};
    use crate::planner2::{bind_LogicalPlan_from_singleStatement, BindingContextStack};
    use antlr_rust::common_token_stream::CommonTokenStream;
    use antlr_rust::errors::ANTLRError;
    use antlr_rust::input_stream::InputStream;
    use antlr_rust::token_factory::ArenaCommonFactory;
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion_common::Result;
    use datafusion_common::{DataFusionError, TableReference};
    use datafusion_expr::TableSource;

    use super::BindingContext;

    fn parse<'input>(
        sql: &'input str,
        tf: &'input ArenaCommonFactory<'input>,
    ) -> result::Result<Rc<SingleStatementContextAll<'input>>, ANTLRError> {
        println!("test started");

        let mut _lexer: PrestoLexer<'input, InputStream<&'input str>> =
            PrestoLexer::new_with_token_factory(InputStream::new(&sql), &tf);
        let token_source = CommonTokenStream::new(_lexer);
        let mut parser = PrestoParser::new(token_source);
        println!("\nstart parsing");
        parser.singleStatement()
    }

    struct EmptyTable {
        table_schema: SchemaRef,
    }

    impl EmptyTable {
        fn new(table_schema: SchemaRef) -> Self {
            Self { table_schema }
        }
    }

    impl TableSource for EmptyTable {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table_schema.clone()
        }
    }

    struct TableBindingContext;

    impl BindingContext for TableBindingContext {
        fn resolve_table(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            let schema = match name.table() {
                "person" => Ok(Schema::new(vec![
                    Field::new("id", DataType::UInt32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                    Field::new("state", DataType::Utf8, false),
                    Field::new("salary", DataType::Float64, false),
                    Field::new(
                        "birth_date",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                    Field::new("😀", DataType::Int32, false),
                ])),
                _ => Err(DataFusionError::Plan(format!(
                    "No table named: {} found",
                    name.table()
                ))),
            };

            match schema {
                Ok(t) => Ok(Arc::new(EmptyTable::new(Arc::new(t)))),
                Err(e) => Err(e),
            }
        }
    }
    #[test]
    fn it_works() {
        let tf = ArenaCommonFactory::default();
        let root = parse("SELECT A FROM B", &tf).unwrap();
        let bc = Rc::new(BindingContextStack {
            stack: RefCell::new(vec![Box::new(TableBindingContext {})]),
        });
        let plan = bind_LogicalPlan_from_singleStatement(bc, root).unwrap();
        let expected = "EmptyRelation";
        assert_eq!(expected, format!("{plan:?}"));
    }
}
