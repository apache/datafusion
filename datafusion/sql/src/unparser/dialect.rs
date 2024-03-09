pub trait Dialect {
    fn identifier_quote_style(&self) -> Option<char>;
}
pub struct DefaultDialect {}

impl Dialect for DefaultDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        None
    }
}

pub struct PostgreSqlDialect {}

impl Dialect for PostgreSqlDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        Some('"')
    }
}

pub struct MySqlDialect {}

impl Dialect for MySqlDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        Some('`')
    }
}

pub struct SqliteDialect {}

impl Dialect for SqliteDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        Some('`')
    }
}

pub struct CustomDialect {
    identifier_quote_style: Option<char>,
}

impl CustomDialect {
    pub fn new(identifier_quote_style: Option<char>) -> Self {
        Self {
            identifier_quote_style,
        }
    }
}

impl Dialect for CustomDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        self.identifier_quote_style
    }
}
