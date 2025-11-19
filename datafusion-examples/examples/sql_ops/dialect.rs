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

//! See `main.rs` for how to run it.

use std::fmt::Display;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::{
    parser::{CopyToSource, CopyToStatement, DFParser, DFParserBuilder, Statement},
    sqlparser::{keywords::Keyword, tokenizer::Token},
};

/// This example demonstrates how to use the DFParser to parse a statement in a custom way
///
/// This technique can be used to implement a custom SQL dialect, for example.
pub async fn dialect() -> Result<()> {
    let mut my_parser =
        MyParser::new("COPY source_table TO 'file.fasta' STORED AS FASTA")?;

    let my_statement = my_parser.parse_statement()?;

    match my_statement {
        MyStatement::DFStatement(s) => println!("df: {s}"),
        MyStatement::MyCopyTo(s) => println!("my_copy: {s}"),
    }

    Ok(())
}

/// Here we define a Parser for our new SQL dialect that wraps the existing `DFParser`
struct MyParser<'a> {
    df_parser: DFParser<'a>,
}

impl<'a> MyParser<'a> {
    fn new(sql: &'a str) -> Result<Self> {
        let df_parser = DFParserBuilder::new(sql).build()?;
        Ok(Self { df_parser })
    }

    /// Returns true if the next token is `COPY` keyword, false otherwise
    fn is_copy(&self) -> bool {
        matches!(
            self.df_parser.parser.peek_token().token,
            Token::Word(w) if w.keyword == Keyword::COPY
        )
    }

    /// This is the entry point to our parser -- it handles `COPY` statements specially
    /// but otherwise delegates to the existing DataFusion parser.
    pub fn parse_statement(&mut self) -> Result<MyStatement, DataFusionError> {
        if self.is_copy() {
            self.df_parser.parser.next_token(); // COPY
            let df_statement = self.df_parser.parse_copy()?;

            if let Statement::CopyTo(s) = df_statement {
                Ok(MyStatement::from(s))
            } else {
                Ok(MyStatement::DFStatement(Box::from(df_statement)))
            }
        } else {
            let df_statement = self.df_parser.parse_statement()?;
            Ok(MyStatement::from(df_statement))
        }
    }
}

enum MyStatement {
    DFStatement(Box<Statement>),
    MyCopyTo(MyCopyToStatement),
}

impl Display for MyStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyStatement::DFStatement(s) => write!(f, "{s}"),
            MyStatement::MyCopyTo(s) => write!(f, "{s}"),
        }
    }
}

impl From<Statement> for MyStatement {
    fn from(s: Statement) -> Self {
        Self::DFStatement(Box::from(s))
    }
}

impl From<CopyToStatement> for MyStatement {
    fn from(s: CopyToStatement) -> Self {
        if s.stored_as == Some("FASTA".to_string()) {
            Self::MyCopyTo(MyCopyToStatement::from(s))
        } else {
            Self::DFStatement(Box::from(Statement::CopyTo(s)))
        }
    }
}

struct MyCopyToStatement {
    pub source: CopyToSource,
    pub target: String,
}

impl From<CopyToStatement> for MyCopyToStatement {
    fn from(s: CopyToStatement) -> Self {
        Self {
            source: s.source,
            target: s.target,
        }
    }
}

impl Display for MyCopyToStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "COPY {} TO '{}' STORED AS FASTA",
            self.source, self.target
        )
    }
}
