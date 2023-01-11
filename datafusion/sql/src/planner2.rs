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
                self.bind_LogicalPlan_from_query(c.query().unwrap())
            }
            _ => Err(DataFusionError::NotImplemented(String::from(""))),
        }
    }

    fn bind_LogicalPlan_from_query<'input>(
        &self,
        ctx: Rc<QueryContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.with().is_some() {
            return Err(DataFusionError::NotImplemented(String::from("")));
        }
        self.bind_LogicalPlan_from_queryNoWith(ctx.queryNoWith().unwrap())
    }

    fn bind_LogicalPlan_from_queryNoWith<'input>(
        &self,
        ctx: Rc<QueryNoWithContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.sortItem_all().len() > 0 {
            return Err(DataFusionError::NotImplemented(String::from("")));
        }
        if ctx.offset.is_some() {
            return Err(DataFusionError::NotImplemented(String::from("")));
        }
        if ctx.limit.is_some() {
            return Err(DataFusionError::NotImplemented(String::from("")));
        }
        if ctx.FETCH().is_some() {
            return Err(DataFusionError::NotImplemented(String::from("")));
        }
        self.bind_LogicalPlan_from_queryTerm(ctx.queryTerm().unwrap())
    }

    fn bind_LogicalPlan_from_queryTerm<'input>(
        &self,
        ctx: Rc<QueryTermContextAll<'input>>,
    ) -> Result<LogicalPlan> {
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
