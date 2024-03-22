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

use std::fmt::Display;

use datafusion::error::Result;
use datafusion_sql::{
    parser::{CopyToSource, CopyToStatement, DFParser, Statement},
    sqlparser::{keywords::Keyword, parser::ParserError, tokenizer::Token},
};

/// This example demonstrates how to use the DFParser to parse a statement in a custom way
#[tokio::main]
async fn main() -> Result<()> {
    let mut my_parser =
        MyParser::new("COPY source_table TO 'file.fasta' STORED AS FASTA")?;

    let my_statement = my_parser.parse_statement()?;

    match my_statement {
        MyStatement::DFStatement(s) => println!("df: {}", s),
        MyStatement::MyCopyTo(s) => println!("my_copy: {}", s),
    }

    Ok(())
}

struct MyParser<'a> {
    df_parser: DFParser<'a>,
}

impl MyParser<'_> {
    fn new(sql: &str) -> Result<Self> {
        let df_parser = DFParser::new(sql)?;
        Ok(Self { df_parser })
    }

    pub fn parse_statement(&mut self) -> Result<MyStatement, ParserError> {
        match self.df_parser.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::COPY => {
                        self.df_parser.parser.next_token(); // COPY
                        let df_statement = self.df_parser.parse_copy()?;

                        if let Statement::CopyTo(s) = df_statement {
                            Ok(MyStatement::from(s))
                        } else {
                            Ok(MyStatement::DFStatement(Box::from(df_statement)))
                        }
                    }
                    _ => {
                        // use sqlparser-rs parser
                        let df_statement = self.df_parser.parse_statement()?;
                        Ok(MyStatement::from(df_statement))
                    }
                }
            }
            _ => {
                let df_statement = self.df_parser.parse_statement()?;
                Ok(MyStatement::from(df_statement))
            }
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
            MyStatement::DFStatement(s) => write!(f, "{}", s),
            MyStatement::MyCopyTo(s) => write!(f, "{}", s),
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
