# Synnada Case

- `LogicalPlan::display_indent` for displaying plans and using `insta`
- `LogicalPlanBuilder::join` doesn't respect `join_keys` and `filter` when deciding `Join::join_type` if `join_keys` is empty then `JoinType::On` and if `filters` it should be `JoinType::Using`
- **TODO:** Is it possible to have a self join with a different join type?
- **TODO:** Relax `JoinConstraint` to allow `On` variant using `Join.filters`
- **TODO:** Check if the column references are the same
- **TODO:** `ON` Check if the columns combined form a unique constraint similar to composite primary keys in PostgreSQL

- Deduplication for `SubqueryAlias` and `Projection`
- `TableSource` can be bound by `Any` trait and can have a blanket implementation for `as_any() -> &dyn Any`
- `USING` is never instantiated, lol

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

- Combine `Projection` and `Projection` into one

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

```sql
-- Create table statement (with SERIAL auto-increment)
CREATE TABLE IF NOT EXISTS employees (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, department VARCHAR(255) NOT NULL);

-- Bulk insert only
INSERT INTO
	employees (id, name, department)
VALUES
	(1, 'John Smith', 'Engineering'),
	(2, 'Sarah Johnson', 'Marketing'),
	(3, 'Michael Brown', 'Sales'),
	(4, 'Emily Davis', 'Human Resources'),
	(5, 'Robert Wilson', 'Finance'),
	(6, 'Jennifer Lee', 'Engineering'),
	(7, 'David Martinez', 'Sales'),
	(8, 'Lisa Garcia', 'Customer Support'),
	(9, 'Thomas Anderson', 'Research and Development'),
	(10, 'Amanda Taylor', 'Operations');

SELECT
	a.id
FROM
	employees a
	JOIN (
		SELECT
			id
		FROM
			employees
		WHERE
			department = 'Finance'
	) b USING (id);

CREATE TABLE purchases (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL, purchase_date DATE NOT NULL, amount NUMERIC(12, 2) NOT NULL);

CREATE TABLE users (id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT NOW());

SELECT
	a.user_id,
	a.purchase_date,
	SUM(b.amount) AS running_total
FROM
	purchases a
	JOIN purchases b ON a.user_id = b.user_id
	AND b.purchase_date <= a.purchase_date
GROUP BY
	a.user_id,
	a.purchase_date;

INSERT INTO
	users (username, email)
VALUES
	('alice', 'alice@example.com'),
	('bob', 'bob@example.com'),
	('carol', 'carol@example.com');

INSERT INTO
	purchases (user_id, purchase_date, amount)
VALUES
	-- Alice's purchases
	(1, '2024-05-01', 50.00),
	(1, '2024-05-03', 25.00),
	(1, '2024-05-10', 40.00),
	-- Bob's purchases
	(2, '2024-05-02', 100.00),
	(2, '2024-05-05', 60.00),
	-- Carol's purchases
	(3, '2024-05-01', 20.00),
	(3, '2024-05-04', 30.00),
	(3, '2024-05-08', 10.00);

SELECT
	*
FROM
	purchases a
	JOIN purchases b ON a.user_id = b.user_id
	AND b.purchase_date <= a.purchase_date
WHERE
	a.user_id = 1;


SELECT
	id,
FROM
	employees a
	JOIN employees b ON a.id = b.id
WHERE
	b.department = 'HR';
```
