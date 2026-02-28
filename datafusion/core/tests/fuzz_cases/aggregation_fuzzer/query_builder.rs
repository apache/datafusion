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

use std::{collections::HashSet, str::FromStr};

use rand::{Rng, rng, seq::SliceRandom};

/// Random aggregate query builder
///
/// Creates queries like
/// ```sql
/// SELECT AGG(..) FROM table_name GROUP BY <group_by_columns>
/// ```
#[derive(Debug, Default, Clone)]
pub struct QueryBuilder {
    // ===================================
    // Table settings
    // ===================================
    /// The name of the table to query
    table_name: String,

    // ===================================
    // Grouping settings
    // ===================================
    /// Columns to be used in randomly generate `groupings`
    ///
    /// # Example
    ///
    /// Columns:
    ///
    /// ```text
    ///   [a,b,c,d]
    /// ```
    ///
    /// And randomly generated `groupings` (at least 1 column)
    /// can be:
    ///
    /// ```text
    ///   [a]
    ///   [a,b]
    ///   [a,b,d]
    ///   ...
    /// ```
    ///
    /// So the finally generated sqls will be:
    ///
    /// ```text
    ///   SELECT aggr FROM t GROUP BY a;
    ///   SELECT aggr FROM t GROUP BY a,b;
    ///   SELECT aggr FROM t GROUP BY a,b,d;
    ///   ...
    /// ```
    group_by_columns: Vec<String>,

    /// Max columns num in randomly generated `groupings`
    max_group_by_columns: usize,

    /// Min columns num in randomly generated `groupings`
    min_group_by_columns: usize,

    /// The sort keys of dataset
    ///
    /// Due to optimizations will be triggered when all or some
    /// grouping columns are the sort keys of dataset.
    /// So it is necessary to randomly generate some `groupings` basing on
    /// dataset sort keys for test coverage.
    ///
    /// # Example
    ///
    /// Dataset including columns [a,b,c], and sorted by [a,b]
    ///
    /// And we may generate sqls to try covering the sort-optimization cases like:
    ///
    /// ```text
    ///   SELECT aggr FROM t GROUP BY b; // no permutation case
    ///   SELECT aggr FROM t GROUP BY a,c; // partial permutation case
    ///   SELECT aggr FROM t GROUP BY a,b,c; // full permutation case
    ///   ...
    /// ```
    ///
    /// More details can see [`GroupOrdering`].
    ///
    /// [`GroupOrdering`]:  datafusion_physical_plan::aggregates::order::GroupOrdering
    dataset_sort_keys: Vec<Vec<String>>,

    /// If we will also test the no grouping case like:
    ///
    /// ```text
    ///   SELECT aggr FROM t;
    /// ```
    no_grouping: bool,

    // ====================================
    // Aggregation function settings
    // ====================================
    /// Aggregate functions to be used in the query
    /// (function_name, is_distinct)
    aggregate_functions: Vec<(String, bool)>,

    /// Possible columns for arguments in the aggregate functions
    ///
    /// Assumes each
    arguments: Vec<String>,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            no_grouping: true,
            max_group_by_columns: 5,
            min_group_by_columns: 1,
            ..Default::default()
        }
    }

    /// return the table name if any
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Set the table name for the query builder
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Add a new possible aggregate function to the query builder
    pub fn with_aggregate_function(
        mut self,
        aggregate_function: impl Into<String>,
    ) -> Self {
        self.aggregate_functions
            .push((aggregate_function.into(), false));
        self
    }

    /// Add a new possible `DISTINCT` aggregate function to the query
    ///
    /// This is different than `with_aggregate_function` because only certain
    /// aggregates support `DISTINCT`
    pub fn with_distinct_aggregate_function(
        mut self,
        aggregate_function: impl Into<String>,
    ) -> Self {
        self.aggregate_functions
            .push((aggregate_function.into(), true));
        self
    }

    /// Set the columns to be used in the group bys clauses
    pub fn set_group_by_columns<'a>(
        mut self,
        group_by: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        self.group_by_columns = group_by.into_iter().map(String::from).collect();
        self
    }

    /// Add one or more columns to be used as an argument in the aggregate functions
    pub fn with_aggregate_arguments<'a>(
        mut self,
        arguments: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let arguments = arguments.into_iter().map(String::from);
        self.arguments.extend(arguments);
        self
    }

    /// Add max columns num in group by(default: 3), for example if it is set to 1,
    /// the generated sql will group by at most 1 column
    #[expect(dead_code)]
    pub fn with_max_group_by_columns(mut self, max_group_by_columns: usize) -> Self {
        self.max_group_by_columns = max_group_by_columns;
        self
    }

    #[expect(dead_code)]
    pub fn with_min_group_by_columns(mut self, min_group_by_columns: usize) -> Self {
        self.min_group_by_columns = min_group_by_columns;
        self
    }

    /// Add sort keys of dataset if any, then the builder will generate queries basing on it
    /// to cover the sort-optimization cases
    pub fn with_dataset_sort_keys(mut self, dataset_sort_keys: Vec<Vec<String>>) -> Self {
        self.dataset_sort_keys = dataset_sort_keys;
        self
    }

    /// Add if also test the no grouping aggregation case(default: true)
    #[expect(dead_code)]
    pub fn with_no_grouping(mut self, no_grouping: bool) -> Self {
        self.no_grouping = no_grouping;
        self
    }

    pub fn generate_queries(mut self) -> Vec<String> {
        const NUM_QUERIES: usize = 3;
        let mut sqls = Vec::new();

        // Add several queries group on randomly picked columns
        for _ in 0..NUM_QUERIES {
            let sql = self.generate_query();
            sqls.push(sql);
        }

        // Also add several queries limited to grouping on the group by
        // dataset sorted columns only, if any.
        // So if the data is sorted on `a,b` only group by `a,b` or`a` or `b`.
        if !self.dataset_sort_keys.is_empty() {
            let dataset_sort_keys = self.dataset_sort_keys.clone();
            for sort_keys in dataset_sort_keys {
                let group_by_columns = sort_keys.iter().map(|s| s.as_str());
                self = self.set_group_by_columns(group_by_columns);
                for _ in 0..NUM_QUERIES {
                    let sql = self.generate_query();
                    sqls.push(sql);
                }
            }
        }

        // Also add a query with no grouping
        if self.no_grouping {
            self = self.set_group_by_columns(vec![]);
            let sql = self.generate_query();
            sqls.push(sql);
        }

        sqls
    }

    fn generate_query(&self) -> String {
        let group_by = self.random_group_by();
        dbg!(&group_by);
        let mut query = String::from("SELECT ");
        query.push_str(&group_by.join(", "));
        if !group_by.is_empty() {
            query.push_str(", ");
        }
        query.push_str(&self.random_aggregate_functions(&group_by).join(", "));
        query.push_str(" FROM ");
        query.push_str(&self.table_name);
        if !group_by.is_empty() {
            query.push_str(" GROUP BY ");
            query.push_str(&group_by.join(", "));
        }
        query
    }

    /// Generate a some random aggregate function invocations (potentially repeating).
    ///
    /// Each aggregate function invocation is of the form
    ///
    /// ```sql
    /// function_name(<DISTINCT> argument) as alias
    /// ```
    ///
    /// where
    /// * `function_names` are randomly selected from [`Self::aggregate_functions`]
    /// * `<DISTINCT> argument` is randomly selected from [`Self::arguments`]
    /// * `alias` is a unique alias `colN` for the column (to avoid duplicate column names)
    fn random_aggregate_functions(&self, group_by_cols: &[String]) -> Vec<String> {
        const MAX_NUM_FUNCTIONS: usize = 5;
        let mut rng = rng();
        let num_aggregate_functions = rng.random_range(1..=MAX_NUM_FUNCTIONS);

        let mut alias_gen = 1;

        let mut aggregate_functions = vec![];

        let mut order_by_black_list: HashSet<String> =
            group_by_cols.iter().cloned().collect();
        // remove one random col
        if let Some(first) = order_by_black_list.iter().next().cloned() {
            order_by_black_list.remove(&first);
        }

        while aggregate_functions.len() < num_aggregate_functions {
            let idx = rng.random_range(0..self.aggregate_functions.len());
            let (function_name, is_distinct) = &self.aggregate_functions[idx];
            let argument = self.random_argument();
            let alias = format!("col{alias_gen}");
            let distinct = if *is_distinct { "DISTINCT " } else { "" };
            alias_gen += 1;

            let (order_by, null_opt) = if function_name.eq("first_value")
                || function_name.eq("last_value")
            {
                (
                    self.order_by(&order_by_black_list), /* Among the order by columns, at most one group by column can be included to avoid all order by column values being identical */
                    self.null_opt(),
                )
            } else {
                ("".to_string(), "".to_string())
            };

            let function = format!(
                "{function_name}({distinct}{argument}{order_by}) {null_opt} as {alias}"
            );
            aggregate_functions.push(function);
        }
        aggregate_functions
    }

    /// Pick a random aggregate function argument
    fn random_argument(&self) -> String {
        let mut rng = rng();
        let idx = rng.random_range(0..self.arguments.len());
        self.arguments[idx].clone()
    }

    fn order_by(&self, black_list: &HashSet<String>) -> String {
        let mut available_columns: Vec<String> = self
            .arguments
            .iter()
            .filter(|col| !black_list.contains(*col))
            .cloned()
            .collect();

        available_columns.shuffle(&mut rng());

        let num_of_order_by_col = 12;
        let column_count = std::cmp::min(num_of_order_by_col, available_columns.len());

        let selected_columns = &available_columns[0..column_count];

        let mut rng = rng();
        let mut result = String::from_str(" order by ").unwrap();
        for col in selected_columns {
            let order = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
            result.push_str(&format!("{col} {order},"));
        }

        result.strip_suffix(",").unwrap().to_string()
    }

    fn null_opt(&self) -> String {
        if rng().random_bool(0.5) {
            "RESPECT NULLS".to_string()
        } else {
            "IGNORE NULLS".to_string()
        }
    }

    /// Pick a random number of fields to group by (non-repeating)
    ///
    /// Limited to `max_group_by_columns` group by columns to ensure coverage for large groups.
    /// With larger numbers of columns, each group has many fewer values.
    fn random_group_by(&self) -> Vec<String> {
        let mut rng = rng();
        let min_groups = self.min_group_by_columns;
        let max_groups = self.max_group_by_columns;
        assert!(min_groups <= max_groups);
        let num_group_by = rng.random_range(min_groups..=max_groups);

        let mut already_used = HashSet::new();
        let mut group_by = vec![];
        while group_by.len() < num_group_by
            && already_used.len() != self.group_by_columns.len()
        {
            let idx = rng.random_range(0..self.group_by_columns.len());
            if already_used.insert(idx) {
                group_by.push(self.group_by_columns[idx].clone());
            }
        }
        group_by
    }
}
