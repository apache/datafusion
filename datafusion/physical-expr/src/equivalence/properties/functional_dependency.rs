// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use super::EquivalenceProperties;
use crate::PhysicalExpr;
use crate::equivalence::ProjectionMapping;
use crate::expressions::Column;

use datafusion_common::{Constraint, HashMap};

/// A functional dependency `determinant -> dependents` between the columns of
/// an [`EquivalenceProperties`], identified by their indices in its schema.
///
/// Any two rows that have the same values (NULLs included) in every
/// `determinant` column also have the same values in every `dependents`
/// column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FunctionalDependency {
    pub(super) determinant: Vec<usize>,
    pub(super) dependents: Vec<usize>,
}

impl FunctionalDependency {
    /// Returns the dependency over a schema whose columns are shifted by
    /// `offset`, as the right side of a join.
    fn with_offset(mut self, offset: usize) -> Self {
        self.determinant.iter_mut().for_each(|idx| *idx += offset);
        self.dependents.iter_mut().for_each(|idx| *idx += offset);
        self
    }
}

impl EquivalenceProperties {
    /// Adds `dependencies`, skipping the ones that are already known.
    pub(super) fn add_functional_dependencies(
        &mut self,
        dependencies: impl IntoIterator<Item = FunctionalDependency>,
    ) {
        for dependency in dependencies {
            if !self.functional_dependencies.contains(&dependency) {
                self.functional_dependencies.push(dependency);
            }
        }
    }

    /// Returns the functional dependencies that the output of a plan node
    /// with these properties satisfies: the stored ones, and one for each
    /// primary key or unique constraint, which determines every other column.
    ///
    /// A unique constraint only counts when none of its columns is nullable,
    /// since rows with a NULL in a unique column may still be equal in it.
    fn all_functional_dependencies(&self) -> Vec<FunctionalDependency> {
        let num_columns = self.schema.fields().len();
        let keys = self.constraints.iter().filter_map(|constraint| {
            let (Constraint::PrimaryKey(indices) | Constraint::Unique(indices)) =
                constraint;
            let non_nullable = || {
                indices.iter().all(|&idx| {
                    self.schema
                        .fields()
                        .get(idx)
                        .is_some_and(|f| !f.is_nullable())
                })
            };
            let valid = !indices.is_empty()
                && indices.iter().all(|&idx| idx < num_columns)
                && (matches!(constraint, Constraint::PrimaryKey(_)) || non_nullable());
            valid.then(|| FunctionalDependency {
                determinant: indices.clone(),
                dependents: (0..num_columns)
                    .filter(|idx| !indices.contains(idx))
                    .collect(),
            })
        });
        self.functional_dependencies
            .iter()
            .cloned()
            .chain(keys)
            .collect()
    }

    /// Returns the functional dependencies of the output of an inner join of
    /// `self` (the left side) and `right`, whose columns follow those of
    /// `self`.
    ///
    /// A dependency that holds among the rows of one side keeps holding in the
    /// join output, where every row is made of one row of each side, even if
    /// the join repeats rows. For example, the primary key of a side is no
    /// longer a key of the output, but still determines the other columns of
    /// that side. Outer joins pad one side with NULLs, which this reasoning
    /// does not cover.
    pub(super) fn inner_join_functional_dependencies(
        &self,
        right: &Self,
    ) -> Vec<FunctionalDependency> {
        let left_size = self.schema.fields().len();
        let mut dependencies = self.all_functional_dependencies();
        dependencies.extend(
            right
                .all_functional_dependencies()
                .into_iter()
                .map(|dependency| dependency.with_offset(left_size)),
        );
        dependencies
    }

    /// Returns the functional dependencies that survive `mapping`: those whose
    /// determinant columns are all in the output, restricted to the dependent
    /// columns that are in the output.
    ///
    /// A column that the output does not have is replaced by an equal output
    /// column, if there is one. For example, after the join on `s_item = i_id`,
    /// a projection of `s_item` but not `i_id` keeps `i_id -> i_name` as
    /// `s_item -> i_name`.
    pub(super) fn projected_functional_dependencies(
        &self,
        mapping: &ProjectionMapping,
    ) -> Vec<FunctionalDependency> {
        if self.functional_dependencies.is_empty() {
            return vec![];
        }
        let mut indices = self
            .functional_dependencies
            .iter()
            .flat_map(|dependency| {
                dependency.determinant.iter().chain(&dependency.dependents)
            })
            .copied()
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        let (indices, columns): (Vec<_>, Vec<_>) = indices
            .into_iter()
            .filter_map(|idx| {
                let field = self.schema.fields().get(idx)?;
                let column: Arc<dyn PhysicalExpr> =
                    Arc::new(Column::new(field.name(), idx));
                Some((idx, column))
            })
            .unzip();
        // Input column index -> index of an output column with the same value.
        let output_indices = indices
            .into_iter()
            .zip(self.eq_group.project_expressions(mapping, &columns))
            .filter_map(|(idx, projected)| {
                let projected = projected?;
                let column = projected.downcast_ref::<Column>()?;
                Some((idx, column.index()))
            })
            .collect::<HashMap<_, _>>();
        self.functional_dependencies
            .iter()
            .filter_map(|dependency| {
                let determinant = dependency
                    .determinant
                    .iter()
                    .map(|idx| output_indices.get(idx).copied())
                    .collect::<Option<Vec<_>>>()?;
                let dependents = dependency
                    .dependents
                    .iter()
                    .filter_map(|idx| output_indices.get(idx).copied())
                    .collect::<Vec<_>>();
                (!dependents.is_empty()).then_some(FunctionalDependency {
                    determinant,
                    dependents,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PhysicalSortExpr;
    use crate::equivalence::join_equivalence_properties;
    use crate::expressions::col;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{Constraints, JoinSide, JoinType, Result};

    fn schema(fields: &[(&str, bool)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, nullable)| Field::new(*name, DataType::Int32, *nullable))
                .collect::<Vec<_>>(),
        ))
    }

    fn asc(name: &str, schema: &SchemaRef) -> Result<PhysicalSortExpr> {
        Ok(PhysicalSortExpr::new_default(col(name, schema)?).asc())
    }

    fn primary_key(indices: Vec<usize>) -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(indices)])
    }

    /// `items JOIN sales ON s_item = i_id`, where `i_id` is the primary key of
    /// `items` and `sales` is sorted by `[s_item, s_day]`, which `join_type`
    /// keeps through a probe side that is `sales`.
    fn items_join_sales(
        join_type: JoinType,
    ) -> Result<(SchemaRef, EquivalenceProperties)> {
        let items = schema(&[("i_id", false), ("i_name", true), ("i_cat", true)]);
        let sales = schema(&[("s_item", false), ("s_day", false), ("s_qty", true)]);
        let join_schema = schema(&[
            ("i_id", false),
            ("i_name", true),
            ("i_cat", true),
            ("s_item", false),
            ("s_day", false),
            ("s_qty", true),
        ]);
        let left = EquivalenceProperties::new_with_orderings(
            Arc::clone(&items),
            [[asc("i_id", &items)?]],
        )
        .with_constraints(primary_key(vec![0]));
        let right = EquivalenceProperties::new_with_orderings(
            Arc::clone(&sales),
            [[asc("s_item", &sales)?, asc("s_day", &sales)?]],
        );
        let on = [(col("i_id", &items)?, col("s_item", &sales)?)];
        let properties = join_equivalence_properties(
            left,
            right,
            &join_type,
            Arc::clone(&join_schema),
            &[false, true],
            Some(JoinSide::Right),
            &on,
        )?;
        Ok((join_schema, properties))
    }

    #[test]
    fn inner_join_keeps_the_dependencies_of_each_side() -> Result<()> {
        let (_, properties) = items_join_sales(JoinType::Inner)?;
        // The left side only determines its own columns:
        assert_eq!(
            properties.functional_dependencies,
            vec![FunctionalDependency {
                determinant: vec![0],
                dependents: vec![1, 2],
            }]
        );

        let items = schema(&[("i_id", false), ("i_name", true)]);
        let sales = schema(&[("s_item", false), ("s_day", false), ("s_qty", true)]);
        let left =
            EquivalenceProperties::new(items).with_constraints(primary_key(vec![0]));
        let right =
            EquivalenceProperties::new(sales).with_constraints(primary_key(vec![0, 1]));
        assert_eq!(
            left.inner_join_functional_dependencies(&right),
            vec![
                FunctionalDependency {
                    determinant: vec![0],
                    dependents: vec![1],
                },
                FunctionalDependency {
                    determinant: vec![2, 3],
                    dependents: vec![4],
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn determined_column_is_sorted_with_its_determinant() -> Result<()> {
        let (schema, properties) = items_join_sales(JoinType::Inner)?;
        let s_item = asc("s_item", &schema)?;
        let s_day = asc("s_day", &schema)?;
        let i_name = asc("i_name", &schema)?;
        let i_cat = asc("i_cat", &schema)?;
        let s_qty = asc("s_qty", &schema)?;

        assert!(properties.ordering_satisfy([s_item.clone(), s_day.clone()])?);
        // `s_item` fixes `i_id`, the key of `items`, and thus `i_name`:
        assert!(properties.ordering_satisfy([
            s_item.clone(),
            i_name.clone(),
            s_day.clone()
        ])?);
        assert!(properties.ordering_satisfy([
            s_item.clone(),
            i_name.clone(),
            i_cat,
            s_day.clone()
        ])?);
        // `i_name` alone is not sorted, and `s_item` does not determine `s_qty`:
        assert!(!properties.ordering_satisfy([i_name.clone(), s_day.clone()])?);
        assert!(!properties.ordering_satisfy([s_item, s_qty, s_day])?);
        Ok(())
    }

    #[test]
    fn determined_column_is_grouped_with_its_determinant() -> Result<()> {
        let (schema, properties) = items_join_sales(JoinType::Inner)?;
        let s_item = col("s_item", &schema)?;
        let i_name = col("i_name", &schema)?;
        let s_qty = col("s_qty", &schema)?;

        assert!(properties.grouping_satisfy([Arc::clone(&s_item)])?);
        assert!(properties.grouping_satisfy([Arc::clone(&s_item), i_name])?);
        assert!(!properties.grouping_satisfy([s_item, s_qty])?);
        Ok(())
    }

    #[test]
    fn outer_join_drops_the_dependencies() -> Result<()> {
        let (schema, properties) = items_join_sales(JoinType::Right)?;
        assert!(properties.functional_dependencies.is_empty());
        assert!(!properties.ordering_satisfy([
            asc("s_item", &schema)?,
            asc("i_name", &schema)?,
            asc("s_day", &schema)?,
        ])?);
        Ok(())
    }

    #[test]
    fn nullable_unique_constraint_is_not_a_key() -> Result<()> {
        let nullable = schema(&[("a", true), ("b", true)]);
        let unique = Constraints::new_unverified(vec![Constraint::Unique(vec![0])]);
        let properties = EquivalenceProperties::new(Arc::clone(&nullable))
            .with_constraints(unique.clone());
        assert!(properties.all_functional_dependencies().is_empty());

        let non_nullable = schema(&[("a", false), ("b", true)]);
        let properties =
            EquivalenceProperties::new(non_nullable).with_constraints(unique);
        assert_eq!(
            properties.all_functional_dependencies(),
            vec![FunctionalDependency {
                determinant: vec![0],
                dependents: vec![1],
            }]
        );
        Ok(())
    }

    #[test]
    fn projection_keeps_the_dependencies_it_can_express() -> Result<()> {
        let (schema, properties) = items_join_sales(JoinType::Inner)?;
        let output_schema =
            self::schema(&[("id", false), ("name", true), ("day", false)]);
        let mapping = ProjectionMapping::try_new(
            [
                (col("i_id", &schema)?, "id".to_string()),
                (col("i_name", &schema)?, "name".to_string()),
                (col("s_day", &schema)?, "day".to_string()),
            ],
            &schema,
        )?;
        let projected = properties.project(&mapping, Arc::clone(&output_schema));
        assert_eq!(
            projected.functional_dependencies,
            vec![FunctionalDependency {
                determinant: vec![0],
                dependents: vec![1],
            }]
        );

        // `s_item` is equal to `i_id` and takes its place:
        let output_schema = self::schema(&[("item", false), ("name", true)]);
        let mapping = ProjectionMapping::try_new(
            [
                (col("s_item", &schema)?, "item".to_string()),
                (col("i_name", &schema)?, "name".to_string()),
            ],
            &schema,
        )?;
        let projected = properties.project(&mapping, output_schema);
        assert_eq!(
            projected.functional_dependencies,
            vec![FunctionalDependency {
                determinant: vec![0],
                dependents: vec![1],
            }]
        );

        // Without `i_id` or an equal column, nothing determines `i_name`:
        let output_schema = self::schema(&[("name", true), ("day", false)]);
        let mapping = ProjectionMapping::try_new(
            [
                (col("i_name", &schema)?, "name".to_string()),
                (col("s_day", &schema)?, "day".to_string()),
            ],
            &schema,
        )?;
        let projected = properties.project(&mapping, output_schema);
        assert!(projected.functional_dependencies.is_empty());
        Ok(())
    }
}
