use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};

use crate::antlr::presto::prestoparser::*;
use std::rc::Rc;
use datafusion_common::Result;

struct Binder();

impl Binder {
    fn bind_Query_from_singleStatement<'input>(
        &self,
        ctx: Rc<SingleStatementContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        self.bind_Query_from_statement(
            ctx.statement().expect("No statement in singleStatement"),
        )
    }

    fn bind_Query_from_statement<'input>(
        &self,
        ctx: Rc<StatementContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::empty(true).build()
    }
}

pub fn bind<'input>(
    root: Rc<SingleStatementContextAll<'input>>,
) -> Result<LogicalPlan> {
    let binder = Binder();
    return binder.bind_Query_from_singleStatement(root);
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use antlr_rust::common_token_stream::CommonTokenStream;
    use antlr_rust::errors::ANTLRError;
    use antlr_rust::input_stream::InputStream;
    use antlr_rust::token_factory::ArenaCommonFactory;
    use crate::antlr::presto::prestoparser::{PrestoParser, SingleStatementContextAll};
    use crate::antlr::presto::prestolexer::PrestoLexer;

    use super::bind;
    
    fn parse<'input>(sql: &'input str, tf: &'input ArenaCommonFactory<'input>) -> Result<Rc<SingleStatementContextAll<'input>>,ANTLRError> {
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