// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL Macro expansion mechanism

use sqlparser::ast::Value;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{plan_err, MacroCatalog, MacroDefinition, Result};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    ObjectName, ObjectNamePart, Query, SelectItem, SetExpr, TableAlias, TableFactor,
    TableWithJoins, WindowType,
};

use crate::macro_context::MacroContextProvider;

/// A macro expander that replaces macro invocations with their expanded definitions
pub struct MacroExpander {
    catalog: Arc<dyn MacroCatalog>,
}

fn parse_sql_to_query(sql: &str, macro_name: &str) -> Result<Query> {
    match sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::GenericDialect {},
        sql,
    ) {
        Ok(stmts) if stmts.len() == 1 => {
            // Extract the first statement, which should be a SELECT query
            if let sqlparser::ast::Statement::Query(query) = &stmts[0] {
                Ok(*query.clone())
            } else {
                plan_err!("Macro '{}' body must be a query statement", macro_name)
            }
        }
        Ok(_) => {
            plan_err!(
                "Macro '{}' body must contain exactly one statement",
                macro_name
            )
        }
        Err(e) => {
            plan_err!("Error parsing macro '{}' body: {}", macro_name, e)
        }
    }
}

fn validate_macro_argument(
    expr: &Expr,
    param_name: &str,
    macro_name: &str,
) -> Result<()> {
    match expr {
        Expr::Identifier(_) => Ok(()),

        Expr::Value(_) => Ok(()),

        Expr::BinaryOp { .. } => Ok(()),

        Expr::Cast { .. } => Ok(()),

        Expr::UnaryOp { .. } => Ok(()),

        Expr::Function(function) => {
            let func_name = if !function.name.0.is_empty() {
                let ident = &function.name.0[0];
                ident.to_string().to_lowercase()
            } else {
                return plan_err!(
                    "Empty function name in macro '{}' parameter '{}'",
                    macro_name,
                    param_name
                );
            };

            // Disallow certain sensitive functions that could lead to security issues
            const DISALLOWED_FUNCTIONS: [&str; 3] = ["execute", "eval", "run_command"];
            if DISALLOWED_FUNCTIONS.contains(&func_name.as_str()) {
                return plan_err!(
                    "Function '{}' is not allowed as an argument to macro '{}' parameter '{}'",
                    func_name,
                    macro_name,
                    param_name
                );
            }

            if func_name == "columns" {
                match &function.args {
                    FunctionArguments::None => Ok(()),
                    FunctionArguments::List(arg_list) => {
                        if arg_list.args.is_empty() {
                            return Ok(());
                        }

                        if arg_list.args.len() == 1 {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Value(val),
                            )) = &arg_list.args[0]
                            {
                                let val_str = val.to_string();
                                if val_str.starts_with('\'') || val_str.starts_with('"') {
                                    return Ok(());
                                }
                            }

                            plan_err!(
                                "COLUMNS function in macro '{}' parameter '{}' expects a string pattern",
                                macro_name,
                                param_name
                            )
                        } else {
                            plan_err!(
                                "COLUMNS function in macro '{}' parameter '{}' expects 0 or 1 arguments",
                                macro_name,
                                param_name
                            )
                        }
                    }

                    FunctionArguments::Subquery(_) => {
                        plan_err!(
                            "COLUMNS function in macro '{}' parameter '{}' does not accept subquery arguments",
                            macro_name,
                            param_name
                        )
                    }
                }
            } else {
                match &function.args {
                    FunctionArguments::None => Ok(()),

                    FunctionArguments::List(arg_list) => {
                        let mut arg_index = 0;
                        for arg in &arg_list.args {
                            match arg {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                    validate_macro_argument(
                                        expr,
                                        &format!("{}.arg{}", param_name, arg_index),
                                        macro_name,
                                    )?;
                                    arg_index += 1;
                                }
                                FunctionArg::Named { name, arg: FunctionArgExpr::Expr(expr), .. } => {
                                    validate_macro_argument(
                                        expr,
                                        &format!("{}.{}", param_name, name),
                                        macro_name,
                                    )?;
                                    arg_index += 1;
                                }
                                _ => return plan_err!(
                                    "Unsupported argument type in function '{}' for macro '{}' parameter '{}'",
                                    func_name, macro_name, param_name
                                ),
                            }
                        }
                        Ok(())
                    }

                    FunctionArguments::Subquery(_) => {
                        plan_err!(
                            "Subquery as argument to function '{}' is not supported in macro '{}' parameter '{}'",
                            func_name, macro_name, param_name
                        )
                    }
                }
            }
        }

        Expr::Subquery(_) => {
            plan_err!(
                "Subqueries are not supported as arguments to macro '{}' parameter '{}'",
                macro_name,
                param_name
            )
        }

        Expr::Exists { .. } => {
            plan_err!("EXISTS expressions are not supported as arguments to macro '{}' parameter '{}'",
                     macro_name, param_name)
        }

        Expr::InSubquery { .. } => {
            plan_err!("IN subquery expressions are not supported as arguments to macro '{}' parameter '{}'",
                     macro_name, param_name)
        }

        Expr::Array(_) => {
            plan_err!("Array expressions are not currently supported as arguments to macro '{}' parameter '{}'",
                     macro_name, param_name)
        }

        _ => {
            plan_err!("Unsupported expression type for macro '{}' parameter '{}'. Only simple expressions, \
                     identifiers, literal values, and basic functions are supported.",
                     macro_name, param_name)
        }
    }
}

impl MacroExpander {
    /// Create a new macro expander with the given catalog
    pub fn new(catalog: Arc<dyn MacroCatalog>) -> Self {
        Self { catalog }
    }

    /// Get a macro definition and parse its body into a Query object
    fn get_parsed_macro(
        &self,
        macro_name: &str,
    ) -> Result<(Arc<MacroDefinition>, Query)> {
        let macro_def = self.catalog.get_macro(macro_name)?;
        let query = parse_sql_to_query(&macro_def.body, macro_name)?;

        Ok((macro_def, query))
    }

    /// Expand any macro references in a query
    pub fn expand_macros_in_query(&self, query: &mut Query) -> Result<()> {
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.expand_macros_in_query(cte.query.as_mut())?;
            }
        }

        if let SetExpr::Select(select) = query.body.as_mut() {
            for table_with_joins in &mut select.from {
                self.expand_macros_in_table_with_joins(table_with_joins)?;
            }
        }

        Ok(())
    }

    fn expand_macros_in_table_with_joins(
        &self,
        table_with_joins: &mut TableWithJoins,
    ) -> Result<()> {
        if let TableFactor::Table { name, args, .. } = &mut table_with_joins.relation {
            let macro_name = name.to_string();

            if self.catalog.macro_exists(&macro_name) {
                let (macro_def, _expanded_body) = self.get_parsed_macro(&macro_name)?;

                let mut param_map = HashMap::new();

                if let Some(arg_list) = args {
                    if arg_list.args.len() != macro_def.parameters.len() {
                        return plan_err!(
                            "Macro '{}' expects {} parameters but got {}",
                            macro_name,
                            macro_def.parameters.len(),
                            arg_list.args.len()
                        );
                    }

                    for (i, arg) in arg_list.args.iter().enumerate() {
                        let param_name: &String = &macro_def.parameters[i];
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                validate_macro_argument(expr, param_name, &macro_name)?;
                                param_map.insert(param_name.clone(), expr.clone());
                            }
                            _ => {
                                return plan_err!(
                                    "Unsupported argument type for macro '{}' parameter '{}'",
                                    macro_name, param_name
                                );
                            }
                        }
                    }
                } else if !macro_def.parameters.is_empty() {
                    return plan_err!(
                        "Macro '{}' requires {} parameters but none were provided",
                        macro_name,
                        macro_def.parameters.len()
                    );
                }

                let macro_body = macro_def.body.clone();
                let mut expanded_body = parse_sql_to_query(&macro_body, &macro_name)?;

                self.substitute_parameters(&mut expanded_body, &param_map)?;

                let alias = TableAlias {
                    name: Ident::new(macro_name.clone()),
                    columns: vec![],
                };

                table_with_joins.relation = TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(expanded_body),
                    alias: Some(alias),
                };
            }
        }

        Ok(())
    }

    fn substitute_parameters(
        &self,
        query: &mut Query,
        param_map: &HashMap<String, Expr>,
    ) -> Result<()> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            for select_item in &mut select.projection {
                match select_item {
                    SelectItem::UnnamedExpr(expr) => {
                        self.substitute_param_in_expr(expr, param_map)?;
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        self.substitute_param_in_expr(expr, param_map)?;
                    }
                    _ => {}
                }
            }

            for table_with_joins in &mut select.from {
                match &mut table_with_joins.relation {
                    TableFactor::Table {
                        args: Some(table_args),
                        ..
                    } => {
                        for arg in &table_args.args {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(ident),
                            )) = arg
                            {
                                let _ident_name = ident.value.to_string();
                            }
                        }
                    }
                    TableFactor::Derived { subquery, .. } => {
                        self.substitute_parameters(subquery, param_map)?;
                    }
                    TableFactor::Function { args, .. } => {
                        if !args.is_empty() {
                            for arg in args {
                                match arg {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        let mut expr_mut = expr.clone();
                                        self.substitute_param_in_expr(
                                            &mut expr_mut,
                                            param_map,
                                        )?;
                                        *expr = expr_mut;
                                    }
                                    FunctionArg::Named {
                                        arg: FunctionArgExpr::Expr(expr),
                                        ..
                                    } => {
                                        let mut expr_mut = expr.clone();
                                        self.substitute_param_in_expr(
                                            &mut expr_mut,
                                            param_map,
                                        )?;
                                        *expr = expr_mut;
                                    }
                                    _ => {
                                        return plan_err!(
                                            "Unsupported function argument type in macro parameter substitution"
                                        );
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }

                for join in &mut table_with_joins.joins {
                    match &mut join.relation {
                        TableFactor::Derived { subquery, .. } => {
                            self.substitute_parameters(subquery, param_map)?;
                        }
                        TableFactor::Function { args, .. } => {
                            if !args.is_empty() {
                                for arg in args {
                                    match arg {
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                            expr,
                                        )) => {
                                            let mut expr_mut = expr.clone();
                                            self.substitute_param_in_expr(
                                                &mut expr_mut,
                                                param_map,
                                            )?;
                                            *expr = expr_mut;
                                        }
                                        FunctionArg::Named {
                                            name: _,
                                            arg: FunctionArgExpr::Expr(expr),
                                            operator: _,
                                        } => {
                                            let mut expr_mut = expr.clone();
                                            self.substitute_param_in_expr(
                                                &mut expr_mut,
                                                param_map,
                                            )?;
                                            *expr = expr_mut;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            if let Some(where_expr) = &mut select.selection {
                self.substitute_param_in_expr(where_expr, param_map)?;
            }

            match &mut select.group_by {
                GroupByExpr::All(modifiers) => {
                    if !modifiers.is_empty() {
                        for _modifier in modifiers {
                            // For now, we don't process modifiers until there's a clear use case
                            // or a specification for how parameters should be handled in modifiers
                            // This can be extended in the future if needed
                        }
                    }
                }
                GroupByExpr::Expressions(exprs, _) => {
                    for expr in exprs {
                        self.substitute_param_in_expr(expr, param_map)?;
                    }
                }
            }

            if let Some(having_expr) = &mut select.having {
                self.substitute_param_in_expr(having_expr, param_map)?;
            }
        }

        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.substitute_parameters(cte.query.as_mut(), param_map)?;
            }
        }

        Ok(())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn substitute_param_in_expr(
        &self,
        expr: &mut Expr,
        param_map: &HashMap<String, Expr>,
    ) -> Result<()> {
        match expr {
            Expr::Identifier(ident) => {
                let param_name = ident.value.clone();
                if let Some(replacement) = param_map.get(&param_name) {
                    *expr = replacement.clone();
                }
            }
            Expr::Function(func) => {
                match &mut func.args {
                    FunctionArguments::None => {}
                    FunctionArguments::Subquery(subquery) => {
                        self.substitute_parameters(subquery.as_mut(), param_map)?
                    }
                    FunctionArguments::List(arg_list) => {
                        for arg in &mut arg_list.args {
                            match arg {
                                FunctionArg::Named {
                                    name: _,
                                    arg: func_arg_expr,
                                    operator: _,
                                } => self.process_function_arg_expr(
                                    func_arg_expr,
                                    param_map,
                                )?,
                                FunctionArg::ExprNamed {
                                    name,
                                    arg: func_arg_expr,
                                    operator: _,
                                } => {
                                    self.substitute_param_in_expr(&mut *name, param_map)?;
                                    self.process_function_arg_expr(
                                        func_arg_expr,
                                        param_map,
                                    )?
                                }
                                FunctionArg::Unnamed(func_arg_expr) => self
                                    .process_function_arg_expr(
                                        func_arg_expr,
                                        param_map,
                                    )?,
                            }
                        }
                    }
                }

                if let Some(filter) = &mut func.filter {
                    self.substitute_param_in_expr(filter, param_map)?
                }

                if let Some(WindowType::WindowSpec(window_spec)) = &mut func.over {
                    for expr in &mut window_spec.partition_by {
                        self.substitute_param_in_expr(expr, param_map)?
                    }

                    for order_by_expr in &mut window_spec.order_by {
                        let mut expr_mut = order_by_expr.expr.clone();
                        self.substitute_param_in_expr(&mut expr_mut, param_map)?;
                        order_by_expr.expr = expr_mut;
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                let mut left_expr = left.clone();
                let mut right_expr = right.clone();
                self.substitute_param_in_expr(&mut left_expr, param_map)?;
                self.substitute_param_in_expr(&mut right_expr, param_map)?;
                *left = left_expr;
                *right = right_expr;
            }
            Expr::UnaryOp {
                expr: inner_expr, ..
            } => {
                let mut expr_copy = inner_expr.clone();
                self.substitute_param_in_expr(&mut expr_copy, param_map)?;
                *inner_expr = expr_copy;
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(case_operand) = operand {
                    let mut operand_expr = (**case_operand).clone();
                    self.substitute_param_in_expr(&mut operand_expr, param_map)?;
                    *case_operand = Box::new(operand_expr);
                }

                for case_when in conditions {
                    let mut condition_expr = case_when.condition.clone();
                    self.substitute_param_in_expr(&mut condition_expr, param_map)?;
                    case_when.condition = condition_expr;
                    let mut result_expr = case_when.result.clone();
                    self.substitute_param_in_expr(&mut result_expr, param_map)?;
                    case_when.result = result_expr;
                }
                if let Some(else_expr) = else_result {
                    let mut expr_copy = (**else_expr).clone();
                    self.substitute_param_in_expr(&mut expr_copy, param_map)?;
                    *else_expr = Box::new(expr_copy);
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn process_function_arg_expr(
        &self,
        arg_expr: &mut FunctionArgExpr,
        param_map: &HashMap<String, Expr>,
    ) -> Result<()> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => {
                let mut expr_mut = expr.clone();
                self.substitute_param_in_expr(&mut expr_mut, param_map)?;
                *expr = expr_mut;
                Ok(())
            }
            FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => Ok(()),
        }
    }
}

/// Extension trait for ContextProvider to expand macros in SQL queries
pub trait MacroExpansion: MacroContextProvider {
    fn expand_macros_in_query(&self, query: &mut Query) -> Result<()> {
        let catalog = self.macro_catalog()?;
        let expander = MacroExpander::new(catalog);
        expander.expand_macros_in_query(query)
    }

    fn expand_macros_in_relation(&self, relation: &mut TableFactor) -> Result<bool> {
        match relation {
            TableFactor::Table {
                name, args, alias, ..
            } => {
                if let Some(object_name) = name.0.last() {
                    let macro_name = object_name.to_string();
                    if let Ok(catalog) = self.macro_catalog() {
                        if let Ok(macro_def) = catalog.get_macro(&macro_name) {
                            let macro_body = macro_def.body.clone();
                            let mut expanded_query =
                                parse_sql_to_query(&macro_body, &macro_name)?;

                            let mut param_map = HashMap::new();
                            if let Some(arg_list) = args {
                                if arg_list.args.len() != macro_def.parameters.len() {
                                    return plan_err!(
                                        "Macro '{}' expects {} parameters but got {}",
                                        macro_name,
                                        macro_def.parameters.len(),
                                        arg_list.args.len()
                                    );
                                }

                                for (i, arg) in arg_list.args.iter().enumerate() {
                                    let param_name: &String = &macro_def.parameters[i];
                                    match arg {
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                            validate_macro_argument(expr, param_name, &macro_name)?;
                                            param_map.insert(param_name.clone(), expr.clone());
                                        },
                                        FunctionArg::Named { name, operator: _, arg: FunctionArgExpr::Expr(expr) } => {
                                            if !macro_def.parameters.contains(&name.to_string()) {
                                                return plan_err!(
                                                    "Macro '{}' does not have a parameter named '{}'",
                                                    macro_name, name
                                                );
                                            }
                                            validate_macro_argument(expr, &name.to_string(), &macro_name)?;
                                            param_map.insert(name.to_string(), expr.clone());
                                        },
                                        FunctionArg::Named { name: _, operator: _, arg: _ } => {
                                            return plan_err!(
                                                "Unsupported argument type for macro '{}'. Only expressions are supported",
                                                macro_name
                                            );
                                        },
                                        _ => return plan_err!(
                                            "Unsupported argument type for macro '{}'. Only expressions are supported",
                                            macro_name
                                        ),
                                    }
                                }
                            }

                            let expander = MacroExpander::new(catalog);
                            expander
                                .substitute_parameters(&mut expanded_query, &param_map)?;
                            *relation = TableFactor::Derived {
                                lateral: false,
                                subquery: Box::new(expanded_query),
                                alias: Some(alias.clone().unwrap_or_else(|| {
                                    TableAlias {
                                        name: Ident::new(macro_name),
                                        columns: vec![],
                                    }
                                })),
                            };

                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            TableFactor::Derived { subquery, .. } => {
                self.expand_macros_in_query(subquery)?;
                Ok(false)
            }
            TableFactor::Function {
                name, args, alias, ..
            } => {
                if let Some(object_name) = name.0.last() {
                    if object_name.to_string().to_uppercase() == "COLUMNS" {
                        {
                            let function_args = args;
                            if function_args.is_empty() {
                                return plan_err!("COLUMNS function requires at least a table name argument");
                            }

                            let table_name = match &function_args[0] {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(ident),
                                )) => ident.to_string(),
                                _ => {
                                    return plan_err!(
                                    "COLUMNS first argument must be a table identifier"
                                )
                                }
                            };

                            let pattern = if function_args.len() > 1 {
                                match &function_args[1] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::Value(value),
                                    )) => {
                                        if let Value::SingleQuotedString(pattern_str) =
                                            &value.value
                                        {
                                            Some(pattern_str.clone())
                                        } else {
                                            return plan_err!("COLUMNS pattern must be a string literal");
                                        }
                                    }
                                    _ => {
                                        return plan_err!(
                                            "COLUMNS pattern must be a string literal"
                                        )
                                    }
                                }
                            } else {
                                None
                            };

                            let regex_pattern = if let Some(ref pattern_str) = pattern {
                                if pattern_str.trim().is_empty() {
                                    return plan_err!(
                                        "Empty pattern provided for COLUMNS function. Use without a pattern to select all columns."
                                    );
                                }

                                let regex_str =
                                    pattern_str.replace('%', ".*").replace('_', ".");

                                let anchored_regex = format!("^{}$", regex_str);

                                match regex::Regex::new(&anchored_regex) {
                                    Ok(re) => Some(re),
                                    Err(e) => {
                                        return plan_err!(
                                        "Invalid pattern '{}' for COLUMNS function: {}", 
                                        pattern_str, e
                                    )
                                    }
                                }
                            } else {
                                None
                            };

                            if regex_pattern.is_none() {
                                let table_factor = TableFactor::Table {
                                    name: ObjectName(vec![ObjectNamePart::Identifier(
                                        Ident::new(table_name.clone()),
                                    )]),
                                    alias: None,
                                    args: None,
                                    with_hints: vec![],
                                    version: None,
                                    with_ordinality: false,
                                    partitions: vec![],
                                    json_path: None,
                                    sample: None,
                                    index_hints: vec![],
                                };

                                *relation = table_factor;
                            } else {
                                let pattern_str = if let Some(p) = pattern {
                                    p
                                } else {
                                    "*".to_string()
                                };

                                let (prefix, wildcard) = if pattern_str.contains('.') {
                                    let parts: Vec<&str> =
                                        pattern_str.split('.').collect();
                                    if parts.len() == 2 && parts[1] == "*" {
                                        (parts[0].to_string(), true)
                                    } else if parts.len() == 3 && parts[2] == "*" {
                                        (format!("{}.{}", parts[0], parts[1]), true)
                                    } else {
                                        (pattern_str.to_string(), false)
                                    }
                                } else {
                                    (pattern_str.to_string(), false)
                                };

                                let table_with_pattern = TableFactor::Table {
                                    name: ObjectName(vec![ObjectNamePart::Identifier(
                                        Ident::new(table_name.clone()),
                                    )]),
                                    alias: None,
                                    args: None,
                                    with_hints: vec![
                                        Expr::Identifier(Ident::new("__column_pattern")),
                                        Expr::Value(
                                            Value::SingleQuotedString(
                                                pattern_str.to_string(),
                                            )
                                            .into(),
                                        ),
                                        Expr::Identifier(Ident::new("__pattern_prefix")),
                                        Expr::Value(
                                            Value::SingleQuotedString(prefix).into(),
                                        ),
                                        Expr::Identifier(Ident::new(
                                            "__pattern_wildcard",
                                        )),
                                        Expr::Value(Value::Boolean(wildcard).into()),
                                    ],
                                    version: None,
                                    with_ordinality: false,
                                    partitions: vec![],
                                    json_path: None,
                                    sample: None,
                                    index_hints: vec![],
                                };

                                *relation = table_with_pattern;
                            }

                            return Ok(true);
                        }
                    }
                }

                if let Some(object_name) = name.0.last() {
                    let macro_name = object_name.to_string();

                    if let Ok(catalog) = self.macro_catalog() {
                        if let Ok(macro_def) = catalog.get_macro(&macro_name) {
                            let macro_body = macro_def.body.clone();
                            let mut expanded_query =
                                parse_sql_to_query(&macro_body, &macro_name)?;

                            let mut param_map = HashMap::new();
                            {
                                if args.len() != macro_def.parameters.len() {
                                    return plan_err!(
                                        "Macro '{}' expects {} parameters but got {}",
                                        macro_name,
                                        macro_def.parameters.len(),
                                        args.len()
                                    );
                                }

                                for (i, arg) in args.iter().enumerate() {
                                    let param_name: &String = &macro_def.parameters[i];
                                    match arg {
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                            validate_macro_argument(expr, param_name, &macro_name)?;
                                            param_map.insert(param_name.clone(), expr.clone());
                                        },
                                        FunctionArg::Named { name, operator: _, arg: FunctionArgExpr::Expr(expr) } => {
                                            if !macro_def.parameters.contains(&name.to_string()) {
                                                return plan_err!(
                                                    "Macro '{}' does not have a parameter named '{}'",
                                                    macro_name, name
                                                );
                                            }
                                            validate_macro_argument(expr, &name.to_string(), &macro_name)?;
                                            param_map.insert(name.to_string(), expr.clone());
                                        },
                                        FunctionArg::Named { name: _, operator: _, arg: _ } => {
                                            return plan_err!(
                                                "Unsupported argument type for macro '{}'. Only expressions are supported",
                                                macro_name
                                            );
                                        },
                                        _ => return plan_err!(
                                            "Unsupported argument type for macro '{}'. Only expressions are supported",
                                            macro_name
                                        ),
                                    }
                                }
                            }

                            let expander = MacroExpander::new(catalog);
                            expander
                                .substitute_parameters(&mut expanded_query, &param_map)?;

                            *relation = TableFactor::Derived {
                                lateral: false,
                                subquery: Box::new(expanded_query),
                                alias: Some(alias.clone().unwrap_or_else(|| {
                                    TableAlias {
                                        name: Ident::new(macro_name),
                                        columns: vec![],
                                    }
                                })),
                            };

                            return Ok(true);
                        }
                    }
                }

                Ok(false)
            }
            _ => Ok(false),
        }
    }
}
