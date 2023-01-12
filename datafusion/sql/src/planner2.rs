#![allow(non_snake_case)]

use datafusion_expr::{EmptyRelation, LogicalPlan};

use crate::antlr::presto::prestoparser::*;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use std::rc::Rc;

struct Binder();

impl Binder {
    fn bind_LogicalPlan_from_singleStatement<'input>(
        &self,
        ctx: Rc<SingleStatementContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_statement(ctx.statement().unwrap())
    }

    fn bind_LogicalPlan_from_statement<'input>(
        &self,
        ctx: Rc<StatementContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            StatementContextAll::StatementDefaultContext(c) => {
                self.bind_LogicalPlan_from_statementDefault(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_statement",
            ))),
        }
    }

    fn bind_LogicalPlan_from_statementDefault<'input>(
        &self,
        ctx: &StatementDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_query(ctx.query().unwrap())
    }

    fn bind_LogicalPlan_from_query<'input>(
        &self,
        ctx: Rc<QueryContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.with().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_query",
            )));
        }
        self.bind_LogicalPlan_from_queryNoWith(ctx.queryNoWith().unwrap())
    }

    fn bind_LogicalPlan_from_queryNoWith<'input>(
        &self,
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
        self.bind_LogicalPlan_from_queryTerm(ctx.queryTerm().unwrap())
    }

    fn bind_LogicalPlan_from_queryTerm<'input>(
        &self,
        ctx: Rc<QueryTermContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            QueryTermContextAll::QueryTermDefaultContext(c) => {
                self.bind_LogicalPlan_from_queryTermDefault(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_queryTerm",
            ))),
        }
    }

    fn bind_LogicalPlan_from_queryTermDefault<'input>(
        &self,
        ctx: &QueryTermDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_queryPrimary(ctx.queryPrimary().unwrap())
    }

    fn bind_LogicalPlan_from_queryPrimary<'input>(
        &self,
        ctx: Rc<QueryPrimaryContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            QueryPrimaryContextAll::QueryPrimaryDefaultContext(c) => {
                self.bind_LogicalPlan_from_queryPrimaryDefault(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_queryPrimary",
            ))),
        }
    }

    fn bind_LogicalPlan_from_queryPrimaryDefault<'input>(
        &self,
        ctx: &QueryPrimaryDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_querySpecification(ctx.querySpecification().unwrap())
    }

    fn bind_LogicalPlan_from_querySpecification<'input>(
        &self,
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
            self.bind_LogicalPlan_from_relation(ctx.relation(0).unwrap())?
        };

        // TODO
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    fn bind_LogicalPlan_from_relation<'input>(
        &self,
        ctx: Rc<RelationContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            RelationContextAll::RelationDefaultContext(c) => {
                self.bind_LogicalPlan_from_relationDefault(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_relation",
            ))),
        }
    }

    fn bind_LogicalPlan_from_relationDefault<'input>(
        &self,
        ctx: &RelationDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_sampledRelation(ctx.sampledRelation().unwrap())
    }

    fn bind_LogicalPlan_from_sampledRelation<'input>(
        &self,
        ctx: Rc<SampledRelationContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.sampleType().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented sampleType",
            )));
        }
        self.bind_LogicalPlan_from_patternRecognition(ctx.patternRecognition().unwrap())
    }

    fn bind_LogicalPlan_from_patternRecognition<'input>(
        &self,
        ctx: Rc<PatternRecognitionContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.MATCH_RECOGNIZE().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented MATCH_RECOGNIZE",
            )));
        }
        self.bind_LogicalPlan_from_aliasedRelation(ctx.aliasedRelation().unwrap())
    }

    fn bind_LogicalPlan_from_aliasedRelation<'input>(
        &self,
        ctx: Rc<AliasedRelationContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.identifier().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented identifier in aliasedRelation",
            )));
        }
        self.bind_LogicalPlan_from_relationPrimary(ctx.relationPrimary().unwrap())
    }

    fn bind_LogicalPlan_from_relationPrimary<'input>(
        &self,
        ctx: Rc<RelationPrimaryContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            RelationPrimaryContextAll::TableNameContext(c) => {
                self.bind_LogicalPlan_from_tableName(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_relationPrimary",
            ))),
        }
    }

    fn bind_LogicalPlan_from_tableName<'input>(
        &self,
        ctx: &TableNameContext<'input>,
    ) -> Result<LogicalPlan> {
        if ctx.queryPeriod().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented queryPeriod",
            )));
        }
        
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }
}

pub fn bind<'input>(root: Rc<SingleStatementContextAll<'input>>) -> Result<LogicalPlan> {
    let binder = Binder();
    return binder.bind_LogicalPlan_from_singleStatement(root);
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::antlr::presto::prestolexer::PrestoLexer;
    use crate::antlr::presto::prestoparser::{PrestoParser, SingleStatementContextAll};
    use antlr_rust::common_token_stream::CommonTokenStream;
    use antlr_rust::errors::ANTLRError;
    use antlr_rust::input_stream::InputStream;
    use antlr_rust::token_factory::ArenaCommonFactory;

    use super::bind;

    fn parse<'input>(
        sql: &'input str,
        tf: &'input ArenaCommonFactory<'input>,
    ) -> Result<Rc<SingleStatementContextAll<'input>>, ANTLRError> {
        println!("test started");

        let mut _lexer: PrestoLexer<'input, InputStream<&'input str>> =
            PrestoLexer::new_with_token_factory(InputStream::new(&sql), &tf);
        let token_source = CommonTokenStream::new(_lexer);
        let mut parser = PrestoParser::new(token_source);
        println!("\nstart parsing");
        parser.singleStatement()
    }

    #[test]
    fn it_works() {
        let tf = ArenaCommonFactory::default();
        let root = parse("SELECT A FROM B", &tf).unwrap();
        let plan = bind(root).unwrap();
        let expected = "EmptyRelation";
        assert_eq!(expected, format!("{plan:?}"));
    }
}
