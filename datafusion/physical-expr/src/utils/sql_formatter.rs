use std::fmt::{self, Formatter};

use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

pub fn sql_formatter(expr: &dyn PhysicalExpr) -> impl fmt::Display + '_ {
    struct Wrapper<'a> {
        expr: &'a dyn PhysicalExpr,
    }

    impl fmt::Display for Wrapper<'_> {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            self.expr.fmt_sql(f)?;
            Ok(())
        }
    }

    Wrapper { expr }
}
