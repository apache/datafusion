# Synnada Case

- `LogicalPlan::display_indent` for displaying plans and using `insta`
- `LogicalPlanBuilder::join` doesn't respect `join_keys` and `filter` when deciding `Join::join_type` if `join_keys` is empty then `JoinType::On` and if `filters` it should be `JoinType::Using`
- **TODO:** Is it possible to have a self join with a different join type?
- **TODO:** Relax `JoinConstraint` to allow `On` variant using `Join.filters`
- **TODO:** Check if the column references are the same
- **TODO:** `ON` Check if the columns combined form a unique constraint similar to composite primary keys in PostgreSQL

## Remarks

- In `JOIN ... USING (...)` expressions if any of join constraints is a unique key then other keys are redundant

```rust
#[derive(Debug)]
struct AliasRenamer<'a> {
    pub left: &'a TableReference,
    pub right: &'a TableReference,
}

impl TreeNodeVisitor<'_> for AliasRenamer<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: &'_ Self::Node) -> Result<TreeNodeRecursion> {
        dbg!(node);
        Ok(TreeNodeRecursion::Continue)
    }
}
```

## Simple Case

Simple `employees` table.

```sql
CREATE TABLE IF NOT EXISTS employees (
  id         INTEGER UNSIGNED NOT NULL,
  department VARCHAR          NOT NULL
);
```

Logical plan

```text
CreateMemoryTable: Bare { table: "employees" }
  EmptyRelation
```

### Self Join with `ON`

```sql
SELECT a.id
FROM employees a
JOIN employees b ON a.id = b.id
WHERE b.department = 'HR';
```

```text
Projection: a.id
  Filter: b.department = Utf8("HR")
    Inner Join:  Filter: a.id = b.id
      SubqueryAlias: a
        TableScan: employees
      SubqueryAlias: b
        TableScan: employees
```

### Self Join with `USING`

```sql
SELECT a.id
FROM employees a
JOIN employees b USING (id)
WHERE b.department = 'HR';
```

```text
Projection: a.id
  Filter: b.department = Utf8("HR")
    Inner Join: Using a.id = b.id
      SubqueryAlias: a
        TableScan: employees
      SubqueryAlias: b
        TableScan: employees
```

### Self Join with Sub-Query with `ON`

```sql
SELECT a.id
FROM employees a
JOIN (SELECT id FROM employees WHERE department = 'HR') b ON a.id = b.id;
```

```text
Projection: a.id
  Inner Join:  Filter: a.id = b.id
    SubqueryAlias: a
      TableScan: employees
    SubqueryAlias: b
      Projection: employees.id
        Filter: employees.department = Utf8("HR")
          TableScan: employees
```

### Self Join with Sub-Query with `USING`

```sql
SELECT a.id
FROM employees a
JOIN (SELECT id FROM employees WHERE department = 'HR') b USING (id)
```

```text
Projection: a.id
  Inner Join: Using a.id = b.id
    SubqueryAlias: a
      TableScan: employees
    SubqueryAlias: b
      Projection: employees.id
        Filter: employees.department = Utf8("HR")
          TableScan: employees
```

[PostgreSQL]: https://www.phoronix.com/news/PostgreSQL-Self-Join-Eliminate
[postgres-constraints]: https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-PRIMARY-KEYS
