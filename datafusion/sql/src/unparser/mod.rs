mod expr;

pub use expr::expr_to_sql;

use self::dialect::{DefaultDialect, Dialect};
mod dialect;

struct Unparser<'a> {
    dialect: &'a dyn Dialect,
}

impl<'a> Unparser<'a> {
    pub fn new(dialect: &'a dyn Dialect) -> Self {
        Self { dialect }
    }
}

impl<'a> Default for Unparser<'a> {
    fn default() -> Self {
        Self {
            dialect: &DefaultDialect {},
        }
    }
}
