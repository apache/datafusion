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

mod guarantee;
pub use guarantee::{Guarantee, LiteralGuarantee};

use std::borrow::Borrow;
use std::sync::Arc;

use crate::PhysicalExpr;
use crate::PhysicalSortExpr;
use crate::expressions::{BinaryExpr, Column};
use crate::tree_node::ExprContext;

use arrow::datatypes::Schema;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{HashMap, HashSet, Result};
use datafusion_expr::Operator;

use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;

/// Assume the predicate is in the form of CNF, split the predicate to a Vec of PhysicalExprs.
///
/// For example, split "a1 = a2 AND b1 <= b2 AND c1 != c2" into ["a1 = a2", "b1 <= b2", "c1 != c2"]
pub fn split_conjunction(
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<&Arc<dyn PhysicalExpr>> {
    split_impl(Operator::And, predicate, vec![])
}

/// Create a conjunction of the given predicates.
/// If the input is empty, return a literal true.
/// If the input contains a single predicate, return the predicate.
/// Otherwise, return a conjunction of the predicates (e.g. `a AND b AND c`).
pub fn conjunction(
    predicates: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
) -> Arc<dyn PhysicalExpr> {
    conjunction_opt(predicates).unwrap_or_else(|| crate::expressions::lit(true))
}

/// Create a conjunction of the given predicates.
/// If the input is empty or the return None.
/// If the input contains a single predicate, return Some(predicate).
/// Otherwise, return a Some(..) of a conjunction of the predicates (e.g. `Some(a AND b AND c)`).
pub fn conjunction_opt(
    predicates: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
) -> Option<Arc<dyn PhysicalExpr>> {
    predicates
        .into_iter()
        .fold(None, |acc, predicate| match acc {
            None => Some(predicate),
            Some(acc) => Some(Arc::new(BinaryExpr::new(acc, Operator::And, predicate))),
        })
}

/// Assume the predicate is in the form of DNF, split the predicate to a Vec of PhysicalExprs.
///
/// For example, split "a1 = a2 OR b1 <= b2 OR c1 != c2" into ["a1 = a2", "b1 <= b2", "c1 != c2"]
pub fn split_disjunction(
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<&Arc<dyn PhysicalExpr>> {
    split_impl(Operator::Or, predicate, vec![])
}

fn split_impl<'a>(
    operator: Operator,
    predicate: &'a Arc<dyn PhysicalExpr>,
    mut exprs: Vec<&'a Arc<dyn PhysicalExpr>>,
) -> Vec<&'a Arc<dyn PhysicalExpr>> {
    match predicate.as_any().downcast_ref::<BinaryExpr>() {
        Some(binary) if binary.op() == &operator => {
            let exprs = split_impl(operator, binary.left(), exprs);
            split_impl(operator, binary.right(), exprs)
        }
        Some(_) | None => {
            exprs.push(predicate);
            exprs
        }
    }
}

/// This function maps back requirement after ProjectionExec
/// to the Executor for its input.
// Specifically, `ProjectionExec` changes index of `Column`s in the schema of its input executor.
// This function changes requirement given according to ProjectionExec schema to the requirement
// according to schema of input executor to the ProjectionExec.
// For instance, Column{"a", 0} would turn to Column{"a", 1}. Please note that this function assumes that
// name of the Column is unique. If we have a requirement such that Column{"a", 0}, Column{"a", 1}.
// This function will produce incorrect result (It will only emit single Column as a result).
pub fn map_columns_before_projection(
    parent_required: &[Arc<dyn PhysicalExpr>],
    proj_exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Vec<Arc<dyn PhysicalExpr>> {
    if parent_required.is_empty() {
        // No need to build mapping.
        return vec![];
    }
    let column_mapping = proj_exprs
        .iter()
        .filter_map(|(expr, name)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|column| (name.clone(), column.clone()))
        })
        .collect::<HashMap<_, _>>();
    parent_required
        .iter()
        .filter_map(|r| {
            r.as_any()
                .downcast_ref::<Column>()
                .and_then(|c| column_mapping.get(c.name()))
        })
        .map(|e| Arc::new(e.clone()) as _)
        .collect()
}

/// This function returns all `Arc<dyn PhysicalExpr>`s inside the given
/// `PhysicalSortExpr` sequence.
pub fn convert_to_expr<T: Borrow<PhysicalSortExpr>>(
    sequence: impl IntoIterator<Item = T>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    sequence
        .into_iter()
        .map(|elem| Arc::clone(&elem.borrow().expr))
        .collect()
}

/// This function finds the indices of `targets` within `items` using strict
/// equality.
pub fn get_indices_of_exprs_strict<T: Borrow<Arc<dyn PhysicalExpr>>>(
    targets: impl IntoIterator<Item = T>,
    items: &[Arc<dyn PhysicalExpr>],
) -> Vec<usize> {
    targets
        .into_iter()
        .filter_map(|target| items.iter().position(|e| e.eq(target.borrow())))
        .collect()
}

pub type ExprTreeNode<T> = ExprContext<Option<T>>;

/// This struct is used to convert a [`PhysicalExpr`] tree into a DAEG (i.e. an expression
/// DAG) by collecting identical expressions in one node. Caller specifies the node type
/// in the DAEG via the `constructor` argument, which constructs nodes in the DAEG from
/// the [`ExprTreeNode`] ancillary object.
struct PhysicalExprDAEGBuilder<'a, T, F: Fn(&ExprTreeNode<NodeIndex>) -> Result<T>> {
    // The resulting DAEG (expression DAG).
    graph: StableGraph<T, usize>,
    // A vector of visited expression nodes and their corresponding node indices.
    visited_plans: Vec<(Arc<dyn PhysicalExpr>, NodeIndex)>,
    // A function to convert an input expression node to T.
    constructor: &'a F,
}

impl<T, F: Fn(&ExprTreeNode<NodeIndex>) -> Result<T>> PhysicalExprDAEGBuilder<'_, T, F> {
    // This method mutates an expression node by transforming it to a physical expression
    // and adding it to the graph. The method returns the mutated expression node.
    fn mutate(
        &mut self,
        mut node: ExprTreeNode<NodeIndex>,
    ) -> Result<Transformed<ExprTreeNode<NodeIndex>>> {
        // Get the expression associated with the input expression node.
        let expr = &node.expr;

        // Check if the expression has already been visited.
        let node_idx = match self.visited_plans.iter().find(|(e, _)| expr.eq(e)) {
            // If the expression has been visited, return the corresponding node index.
            Some((_, idx)) => *idx,
            // If the expression has not been visited, add a new node to the graph and
            // add edges to its child nodes. Add the visited expression to the vector
            // of visited expressions and return the newly created node index.
            None => {
                let node_idx = self.graph.add_node((self.constructor)(&node)?);
                for expr_node in node.children.iter() {
                    self.graph.add_edge(node_idx, expr_node.data.unwrap(), 0);
                }
                self.visited_plans.push((Arc::clone(expr), node_idx));
                node_idx
            }
        };
        // Set the data field of the input expression node to the corresponding node index.
        node.data = Some(node_idx);
        // Return the mutated expression node.
        Ok(Transformed::yes(node))
    }
}

// A function that builds a directed acyclic graph of physical expression trees.
pub fn build_dag<T, F>(
    expr: Arc<dyn PhysicalExpr>,
    constructor: &F,
) -> Result<(NodeIndex, StableGraph<T, usize>)>
where
    F: Fn(&ExprTreeNode<NodeIndex>) -> Result<T>,
{
    // Create a new expression tree node from the input expression.
    let init = ExprTreeNode::new_default(expr);
    // Create a new `PhysicalExprDAEGBuilder` instance.
    let mut builder = PhysicalExprDAEGBuilder {
        graph: StableGraph::<T, usize>::new(),
        visited_plans: Vec::<(Arc<dyn PhysicalExpr>, NodeIndex)>::new(),
        constructor,
    };
    // Use the builder to transform the expression tree node into a DAG.
    let root = init.transform_up(|node| builder.mutate(node)).data()?;
    // Return a tuple containing the root node index and the DAG.
    Ok((root.data.unwrap(), builder.graph))
}

/// Recursively extract referenced [`Column`]s within a [`PhysicalExpr`].
pub fn collect_columns(expr: &Arc<dyn PhysicalExpr>) -> HashSet<Column> {
    let mut columns = HashSet::<Column>::new();
    expr.apply(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            columns.get_or_insert_with(column, |c| c.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    // pre_visit always returns OK, so this will always too
    .expect("no way to return error during recursion");
    columns
}

/// Re-assign indices of [`Column`]s within the given [`PhysicalExpr`] according to
/// the provided [`Schema`].
///
/// This can be useful when attempting to map an expression onto a different schema.
///
/// # Errors
///
/// This function will return an error if any column in the expression cannot be found
/// in the provided schema.
pub fn reassign_expr_columns(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform_down(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            let index = schema.index_of(column.name())?;

            return Ok(Transformed::yes(Arc::new(Column::new(
                column.name(),
                index,
            ))));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

/// Extract Parquet field ID from Arrow field metadata
fn get_field_id(field: &arrow::datatypes::Field) -> Option<i32> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .and_then(|s| s.parse::<i32>().ok())
}

/// Find field index by field ID with fallback to name-based matching
///
/// # Limitations
///
/// TODO: Currently only supports flat schemas. For nested schemas, this function
/// would need to accept a field path (e.g., ["address", "city"]) and return
/// a path of indices. This requires matching nested field IDs at each level
/// of the schema hierarchy.
fn find_field_index(
    column_name: &str,
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<usize> {
    // Try to find the field in source schema
    let source_field = source_schema.field_with_name(column_name)?;

    // Check if field has a field ID
    if let Some(source_field_id) = get_field_id(source_field) {
        // Search target schema for matching field ID
        // TODO: For nested schemas, this needs to recursively match field IDs
        // through the struct hierarchy
        for (idx, target_field) in target_schema.fields().iter().enumerate() {
            if let Some(target_field_id) = get_field_id(target_field)
                && source_field_id == target_field_id
            {
                return Ok(idx);
            }
        }
    }

    // Fallback to name-based matching
    Ok(target_schema.index_of(column_name)?)
}

/// Re-assign column indices in expressions using field ID-based matching.
///
/// This function traverses the expression tree and updates all `Column` references
/// to use field IDs for matching between source and target schemas, falling back
/// to name-based matching when field IDs are unavailable.
///
/// # Arguments
///
/// * `expr` - The physical expression to update
/// * `source_schema` - The schema that the expression currently references
/// * `target_schema` - The schema to map columns to
///
/// # Limitations
///
/// TODO: Currently only supports flat schemas (top-level columns). Nested field
/// references (e.g., "address.city") are not yet supported. Supporting nested
/// fields would require:
/// - Path-based field ID matching through struct hierarchies
/// - Recursive traversal of both expression tree and schema tree
/// - Updates to Column representation to track nested paths
///
/// # Errors
///
/// This function will return an error if any column in the expression cannot be found
/// in the target schema by either field ID or name.
pub fn reassign_expr_columns_with_field_ids(
    expr: Arc<dyn PhysicalExpr>,
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform_down(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            let index = find_field_index(column.name(), source_schema, target_schema)?;
            return Ok(Transformed::yes(Arc::new(Column::new(
                column.name(),
                index,
            ))));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::fmt::{Display, Formatter};

    use super::*;
    use crate::expressions::{Literal, binary, cast, col, in_list, lit};

    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ScalarValue, exec_err, internal_datafusion_err};
    use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    };

    use petgraph::visit::Bfs;

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct TestScalarUDF {
        pub(crate) signature: Signature,
    }

    impl TestScalarUDF {
        pub fn new() -> Self {
            use DataType::*;
            Self {
                signature: Signature::uniform(
                    1,
                    vec![Float64, Float32],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for TestScalarUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "test-scalar-udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            let arg_type = &arg_types[0];

            match arg_type {
                DataType::Float32 => Ok(DataType::Float32),
                _ => Ok(DataType::Float64),
            }
        }

        fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
            Ok(input[0].sort_properties)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;

            let arr: ArrayRef = match args[0].data_type() {
                DataType::Float64 => Arc::new({
                    let arg = &args[0]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            internal_datafusion_err!(
                                "could not cast {} to {}",
                                self.name(),
                                std::any::type_name::<Float64Array>()
                            )
                        })?;

                    arg.iter()
                        .map(|a| a.map(f64::floor))
                        .collect::<Float64Array>()
                }),
                DataType::Float32 => Arc::new({
                    let arg = &args[0]
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(|| {
                            internal_datafusion_err!(
                                "could not cast {} to {}",
                                self.name(),
                                std::any::type_name::<Float32Array>()
                            )
                        })?;

                    arg.iter()
                        .map(|a| a.map(f32::floor))
                        .collect::<Float32Array>()
                }),
                other => {
                    return exec_err!(
                        "Unsupported data type {other:?} for function {}",
                        self.name()
                    );
                }
            };
            Ok(ColumnarValue::Array(arr))
        }
    }

    #[derive(Clone)]
    struct DummyProperty {
        expr_type: String,
    }

    /// This is a dummy node in the DAEG; it stores a reference to the actual
    /// [PhysicalExpr] as well as a dummy property.
    #[derive(Clone)]
    struct PhysicalExprDummyNode {
        pub expr: Arc<dyn PhysicalExpr>,
        pub property: DummyProperty,
    }

    impl Display for PhysicalExprDummyNode {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.expr)
        }
    }

    fn make_dummy_node(node: &ExprTreeNode<NodeIndex>) -> Result<PhysicalExprDummyNode> {
        let expr = Arc::clone(&node.expr);
        let dummy_property = if expr.as_any().is::<BinaryExpr>() {
            "Binary"
        } else if expr.as_any().is::<Column>() {
            "Column"
        } else if expr.as_any().is::<Literal>() {
            "Literal"
        } else {
            "Other"
        }
        .to_owned();
        Ok(PhysicalExprDummyNode {
            expr,
            property: DummyProperty {
                expr_type: dummy_property,
            },
        })
    }

    #[test]
    fn test_build_dag() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let expr = binary(
            cast(
                binary(
                    col("0", &schema)?,
                    Operator::Plus,
                    col("1", &schema)?,
                    &schema,
                )?,
                &schema,
                DataType::Int64,
            )?,
            Operator::Gt,
            binary(
                cast(col("2", &schema)?, &schema, DataType::Int64)?,
                Operator::Plus,
                lit(ScalarValue::Int64(Some(10))),
                &schema,
            )?,
            &schema,
        )?;
        let mut vector_dummy_props = vec![];
        let (root, graph) = build_dag(expr, &make_dummy_node)?;
        let mut bfs = Bfs::new(&graph, root);
        while let Some(node_index) = bfs.next(&graph) {
            let node = &graph[node_index];
            vector_dummy_props.push(node.property.clone());
        }

        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Binary")
                .count(),
            3
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Column")
                .count(),
            3
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Literal")
                .count(),
            1
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Other")
                .count(),
            2
        );
        Ok(())
    }

    #[test]
    fn test_convert_to_expr() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt64, false)]);
        let sort_expr = vec![PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: Default::default(),
        }];
        assert!(convert_to_expr(&sort_expr)[0].eq(&sort_expr[0].expr));
        Ok(())
    }

    #[test]
    fn test_get_indices_of_exprs_strict() {
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("d", 3)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert_eq!(get_indices_of_exprs_strict(&list1, &list2), vec![2, 0, 1]);
        assert_eq!(get_indices_of_exprs_strict(&list2, &list1), vec![1, 2, 0]);
    }

    #[test]
    fn test_reassign_expr_columns_in_list() {
        let int_field = Field::new("should_not_matter", DataType::Int64, true);
        let dict_field = Field::new(
            "id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let schema_small = Arc::new(Schema::new(vec![dict_field.clone()]));
        let schema_big = Arc::new(Schema::new(vec![int_field, dict_field]));
        let pred = in_list(
            Arc::new(Column::new_with_schema("id", &schema_big).unwrap()),
            vec![lit(ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from("2")),
            ))],
            &false,
            &schema_big,
        )
        .unwrap();

        let actual = reassign_expr_columns(pred, &schema_small).unwrap();

        let expected = in_list(
            Arc::new(Column::new_with_schema("id", &schema_small).unwrap()),
            vec![lit(ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from("2")),
            ))],
            &false,
            &schema_small,
        )
        .unwrap();

        assert_eq!(actual.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_collect_columns() -> Result<()> {
        let expr1 = Arc::new(Column::new("col1", 2)) as _;
        let mut expected = HashSet::new();
        expected.insert(Column::new("col1", 2));
        assert_eq!(collect_columns(&expr1), expected);

        let expr2 = Arc::new(Column::new("col2", 5)) as _;
        let mut expected = HashSet::new();
        expected.insert(Column::new("col2", 5));
        assert_eq!(collect_columns(&expr2), expected);

        let expr3 = Arc::new(BinaryExpr::new(expr1, Operator::Plus, expr2)) as _;
        let mut expected = HashSet::new();
        expected.insert(Column::new("col1", 2));
        expected.insert(Column::new("col2", 5));
        assert_eq!(collect_columns(&expr3), expected);
        Ok(())
    }

    // ========================================================================
    // Field ID Tests
    // ========================================================================

    #[test]
    fn test_get_field_id_present() {
        let mut metadata = HashMap::new();
        metadata.insert("PARQUET:field_id".to_string(), "42".to_string());
        let field = Field::new("test", DataType::Int64, false).with_metadata(metadata);

        assert_eq!(get_field_id(&field), Some(42));
    }

    #[test]
    fn test_get_field_id_absent() {
        let field = Field::new("test", DataType::Int64, false);
        assert_eq!(get_field_id(&field), None);
    }

    #[test]
    fn test_get_field_id_invalid() {
        let mut metadata = HashMap::new();
        metadata.insert("PARQUET:field_id".to_string(), "not_a_number".to_string());
        let field = Field::new("test", DataType::Int64, false).with_metadata(metadata);

        assert_eq!(get_field_id(&field), None);
    }

    #[test]
    fn test_find_field_index_by_field_id() -> Result<()> {
        // Source schema: field IDs present
        let mut metadata1 = HashMap::new();
        metadata1.insert("PARQUET:field_id".to_string(), "1".to_string());
        let mut metadata2 = HashMap::new();
        metadata2.insert("PARQUET:field_id".to_string(), "2".to_string());

        let source_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false)
                .with_metadata(metadata1.clone()),
            Field::new("amount", DataType::Float64, false)
                .with_metadata(metadata2.clone()),
        ]);

        // Target schema: renamed columns but same field IDs
        let target_schema = Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false).with_metadata(metadata1),
            Field::new("price", DataType::Float64, false).with_metadata(metadata2),
        ]);

        // Should match by field ID, not name
        let index = find_field_index("user_id", &source_schema, &target_schema)?;
        assert_eq!(
            index, 0,
            "user_id (field_id=1) should match customer_id at index 0"
        );

        let index = find_field_index("amount", &source_schema, &target_schema)?;
        assert_eq!(
            index, 1,
            "amount (field_id=2) should match price at index 1"
        );

        Ok(())
    }

    #[test]
    fn test_find_field_index_by_field_id_reordered() -> Result<()> {
        // Source schema: columns in order [a, b, c]
        let mut meta_a = HashMap::new();
        meta_a.insert("PARQUET:field_id".to_string(), "1".to_string());
        let mut meta_b = HashMap::new();
        meta_b.insert("PARQUET:field_id".to_string(), "2".to_string());
        let mut meta_c = HashMap::new();
        meta_c.insert("PARQUET:field_id".to_string(), "3".to_string());

        let source_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false).with_metadata(meta_a.clone()),
            Field::new("b", DataType::Int64, false).with_metadata(meta_b.clone()),
            Field::new("c", DataType::Int64, false).with_metadata(meta_c.clone()),
        ]);

        // Target schema: columns reordered [c, a, b]
        let target_schema = Schema::new(vec![
            Field::new("c", DataType::Int64, false).with_metadata(meta_c),
            Field::new("a", DataType::Int64, false).with_metadata(meta_a),
            Field::new("b", DataType::Int64, false).with_metadata(meta_b),
        ]);

        // Should match by field ID
        assert_eq!(find_field_index("a", &source_schema, &target_schema)?, 1);
        assert_eq!(find_field_index("b", &source_schema, &target_schema)?, 2);
        assert_eq!(find_field_index("c", &source_schema, &target_schema)?, 0);

        Ok(())
    }

    #[test]
    fn test_find_field_index_fallback_to_name() -> Result<()> {
        // Source schema: no field IDs
        let source_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]);

        // Target schema: no field IDs
        let target_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]);

        // Should fall back to name-based matching
        assert_eq!(
            find_field_index("user_id", &source_schema, &target_schema)?,
            0
        );
        assert_eq!(
            find_field_index("amount", &source_schema, &target_schema)?,
            1
        );

        Ok(())
    }

    #[test]
    fn test_find_field_index_mixed_field_ids() -> Result<()> {
        // Source schema: some fields have IDs, some don't
        let mut metadata1 = HashMap::new();
        metadata1.insert("PARQUET:field_id".to_string(), "1".to_string());

        let source_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false).with_metadata(metadata1.clone()),
            Field::new("b", DataType::Int64, false), // No field ID
        ]);

        let target_schema = Schema::new(vec![
            Field::new("renamed_a", DataType::Int64, false).with_metadata(metadata1),
            Field::new("b", DataType::Int64, false),
        ]);

        // Field with ID should match by ID
        assert_eq!(find_field_index("a", &source_schema, &target_schema)?, 0);

        // Field without ID should match by name
        assert_eq!(find_field_index("b", &source_schema, &target_schema)?, 1);

        Ok(())
    }

    #[test]
    fn test_find_field_index_not_found() {
        let source_schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        let target_schema = Schema::new(vec![Field::new("b", DataType::Int64, false)]);

        // Should fail to find non-existent field
        let result = find_field_index("a", &source_schema, &target_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_reassign_expr_columns_with_field_ids_simple() -> Result<()> {
        // Source schema: full file schema
        let mut meta1 = HashMap::new();
        meta1.insert("PARQUET:field_id".to_string(), "1".to_string());
        let mut meta2 = HashMap::new();
        meta2.insert("PARQUET:field_id".to_string(), "2".to_string());
        let mut meta3 = HashMap::new();
        meta3.insert("PARQUET:field_id".to_string(), "3".to_string());

        let source_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false).with_metadata(meta1.clone()),
            Field::new("name", DataType::Utf8, false).with_metadata(meta2),
            Field::new("age", DataType::Int32, false).with_metadata(meta3.clone()),
        ]);

        // Target schema: projected schema (only user_id and age)
        let target_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false).with_metadata(meta1),
            Field::new("age", DataType::Int32, false).with_metadata(meta3),
        ]);

        // Expression references age at index 2 in source schema
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("age", 2));

        // After transformation, should reference age at index 1 in target schema
        let result =
            reassign_expr_columns_with_field_ids(expr, &source_schema, &target_schema)?;

        let column = result.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(column.name(), "age");
        assert_eq!(
            column.index(),
            1,
            "age should be at index 1 in target schema"
        );

        Ok(())
    }

    #[test]
    fn test_reassign_expr_columns_with_field_ids_complex() -> Result<()> {
        // Source schema
        let mut meta1 = HashMap::new();
        meta1.insert("PARQUET:field_id".to_string(), "1".to_string());
        let mut meta2 = HashMap::new();
        meta2.insert("PARQUET:field_id".to_string(), "2".to_string());
        let mut meta3 = HashMap::new();
        meta3.insert("PARQUET:field_id".to_string(), "3".to_string());

        let source_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(meta1.clone()),
            Field::new("b", DataType::Int32, false).with_metadata(meta2.clone()),
            Field::new("c", DataType::Int32, false).with_metadata(meta3.clone()),
        ]);

        // Target schema: only columns a and c (b excluded)
        let target_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(meta1),
            Field::new("c", DataType::Int32, false).with_metadata(meta3),
        ]);

        // Expression: a@0 + c@2
        let expr = binary(
            col("a", &source_schema)?,
            Operator::Plus,
            col("c", &source_schema)?,
            &source_schema,
        )?;

        // After transformation: a@0 + c@1
        let result =
            reassign_expr_columns_with_field_ids(expr, &source_schema, &target_schema)?;

        // Verify it's still a binary expression
        let binary_expr = result.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Check left side (a)
        let left_col = binary_expr
            .left()
            .as_any()
            .downcast_ref::<Column>()
            .unwrap();
        assert_eq!(left_col.name(), "a");
        assert_eq!(left_col.index(), 0);

        // Check right side (c)
        let right_col = binary_expr
            .right()
            .as_any()
            .downcast_ref::<Column>()
            .unwrap();
        assert_eq!(right_col.name(), "c");
        assert_eq!(
            right_col.index(),
            1,
            "c should be remapped from index 2 to 1"
        );

        Ok(())
    }

    #[test]
    fn test_reassign_expr_columns_with_field_ids_renamed_columns() -> Result<()> {
        // Source schema (file schema with old names)
        let mut meta1 = HashMap::new();
        meta1.insert("PARQUET:field_id".to_string(), "1".to_string());
        let mut meta2 = HashMap::new();
        meta2.insert("PARQUET:field_id".to_string(), "2".to_string());

        let source_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false).with_metadata(meta1.clone()),
            Field::new("amount", DataType::Float64, false).with_metadata(meta2.clone()),
        ]);

        // Target schema (query schema with renamed columns)
        let target_schema = Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false).with_metadata(meta1),
            Field::new("price", DataType::Float64, false).with_metadata(meta2),
        ]);

        // Expression references old names at their source indices
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("user_id", 0));

        // After transformation, should still reference by old name but correct index
        let result =
            reassign_expr_columns_with_field_ids(expr, &source_schema, &target_schema)?;

        let column = result.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(column.name(), "user_id", "Name should remain user_id");
        assert_eq!(
            column.index(),
            0,
            "Should match customer_id at index 0 via field_id"
        );

        Ok(())
    }

    #[test]
    fn test_reassign_expr_columns_with_field_ids_no_field_ids() -> Result<()> {
        // Schemas without field IDs - should fall back to name matching
        let source_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let target_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        // Expression: c@2
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c", 2));

        // Should fall back to name-based matching
        let result =
            reassign_expr_columns_with_field_ids(expr, &source_schema, &target_schema)?;

        let column = result.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(column.name(), "c");
        assert_eq!(column.index(), 1, "c should be found by name at index 1");

        Ok(())
    }
}
