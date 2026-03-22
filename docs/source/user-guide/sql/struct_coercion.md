<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Struct Type Coercion and Field Mapping

DataFusion uses **name-based field mapping** when coercing struct types across different operations. This document explains how struct coercion works, when it applies, and how to handle NULL fields.

## Overview: Name-Based vs Positional Mapping

When combining structs from different sources (e.g., in UNION, array construction, or JOINs), DataFusion matches struct fields by **name** rather than by **position**. This provides more robust and predictable behavior compared to positional matching.

### Example: Field Reordering is Handled Transparently

```sql
-- These two structs have the same fields in different order
SELECT [{a: 1, b: 2}, {b: 3, a: 4}];

-- Result: Field names matched, values unified
-- [{"a": 1, "b": 2}, {"a": 4, "b": 3}]
```

## Coercion Paths Using Name-Based Matching

The following query operations use name-based field mapping for struct coercion:

### 1. Array Literal Construction

When creating array literals with struct elements that have different field orders:

```sql
-- Structs with reordered fields in array literal
SELECT [{x: 1, y: 2}, {y: 3, x: 4}];

-- Unified type: List(Struct("x": Int32, "y": Int32))
-- Values: [{"x": 1, "y": 2}, {"x": 4, "y": 3}]
```

**When it applies:**

- Array literals with struct elements: `[{...}, {...}]`
- Nested arrays with structs: `[[{x: 1}, {x: 2}]]`

### 2. Array Construction from Columns

When constructing arrays from table columns with different struct schemas:

```sql
CREATE TABLE t_left (s struct(x int, y int)) AS VALUES ({x: 1, y: 2});
CREATE TABLE t_right (s struct(y int, x int)) AS VALUES ({y: 3, x: 4});

-- Dynamically constructs unified array schema
SELECT [t_left.s, t_right.s] FROM t_left JOIN t_right;

-- Result: [{"x": 1, "y": 2}, {"x": 4, "y": 3}]
```

**When it applies:**

- Array construction with column references: `[col1, col2]`
- Array construction in joins with matching field names

### 3. UNION Operations

When combining query results with different struct field orders:

```sql
SELECT {a: 1, b: 2} as s
UNION ALL
SELECT {b: 3, a: 4} as s;

-- Result: {"a": 1, "b": 2} and {"a": 4, "b": 3}
```

**When it applies:**

- UNION ALL with structs: field names matched across branches
- UNION (deduplicated) with structs

### 4. Common Table Expressions (CTEs)

When multiple CTEs produce structs with different field orders that are combined:

```sql
WITH
  t1 AS (SELECT {a: 1, b: 2} as s),
  t2 AS (SELECT {b: 3, a: 4} as s)
SELECT s FROM t1
UNION ALL
SELECT s FROM t2;

-- Result: Field names matched across CTEs
```

### 5. VALUES Clauses

When creating tables or temporary results with struct values in different field orders:

```sql
CREATE TABLE t AS VALUES ({a: 1, b: 2}), ({b: 3, a: 4});

-- Table schema unified: struct(a: int, b: int)
-- Values: {a: 1, b: 2} and {a: 4, b: 3}
```

### 6. JOIN Operations

When joining tables where the JOIN condition involves structs with different field orders:

```sql
CREATE TABLE orders (customer struct(name varchar, id int));
CREATE TABLE customers (info struct(id int, name varchar));

-- Join matches struct fields by name
SELECT * FROM orders
JOIN customers ON orders.customer = customers.info;
```

### 7. Aggregate Functions

When collecting structs with different field orders using aggregate functions like `array_agg`:

```sql
SELECT array_agg(s) FROM (
  SELECT {x: 1, y: 2} as s
  UNION ALL
  SELECT {y: 3, x: 4} as s
) t
GROUP BY category;

-- Result: Array of structs with unified field order
```

### 8. Window Functions

When using window functions with struct expressions having different field orders:

```sql
SELECT
  id,
  row_number() over (partition by s order by id) as rn
FROM (
  SELECT {category: 1, value: 10} as s, 1 as id
  UNION ALL
  SELECT {value: 20, category: 1} as s, 2 as id
);

-- Fields matched by name in PARTITION BY clause
```

## NULL Handling for Missing Fields

When structs have different field sets, missing fields are filled with **NULL** values during coercion.

### Example: Partial Field Overlap

```sql
-- Struct in first position has fields: a, b
-- Struct in second position has fields: b, c
-- Unified schema includes all fields: a, b, c

SELECT [
  CAST({a: 1, b: 2} AS STRUCT(a INT, b INT, c INT)),
  CAST({b: 3, c: 4} AS STRUCT(a INT, b INT, c INT))
];

-- Result:
-- [
--   {"a": 1, "b": 2, "c": NULL},
--   {"a": NULL, "b": 3, "c": 4}
-- ]
```

### Limitations

**Field count must match exactly.** If structs have different numbers of fields and their field names don't completely overlap, the query will fail:

```sql
-- This fails because field sets don't match:
-- t_left has {x, y} but t_right has {x, y, z}
SELECT [t_left.s, t_right.s] FROM t_left JOIN t_right;
-- Error: Cannot coerce struct with mismatched field counts
```

**Workaround: Use explicit CAST**

To handle partial field overlap, explicitly cast structs to a unified schema:

```sql
SELECT [
  CAST(t_left.s AS STRUCT(x INT, y INT, z INT)),
  CAST(t_right.s AS STRUCT(x INT, y INT, z INT))
] FROM t_left JOIN t_right;
```

## Migration Guide: From Positional to Name-Based Matching

If you have existing code that relied on **positional** struct field matching, you may need to update it.

### Example: Query That Changes Behavior

**Old behavior (positional):**

```sql
-- These would have been positionally mapped (left-to-right)
SELECT [{x: 1, y: 2}, {y: 3, x: 4}];
-- Old result (positional): [{"x": 1, "y": 2}, {"y": 3, "x": 4}]
```

**New behavior (name-based):**

```sql
-- Now uses name-based matching
SELECT [{x: 1, y: 2}, {y: 3, x: 4}];
-- New result (by name): [{"x": 1, "y": 2}, {"x": 4, "y": 3}]
```

### Migration Steps

1. **Review struct operations** - Look for queries that combine structs from different sources
2. **Check field names** - Verify that field names match as expected (not positions)
3. **Test with new coercion** - Run queries and verify the results match your expectations
4. **Handle field reordering** - If you need specific field orders, use explicit CAST operations

### Using Explicit CAST for Compatibility

If you need precise control over struct field order and types, use explicit `CAST`:

```sql
-- Guarantee specific field order and types
SELECT CAST({b: 3, a: 4} AS STRUCT(a INT, b INT));
-- Result: {"a": 4, "b": 3}
```

## Best Practices

### 1. Be Explicit with Schema Definitions

When joining or combining structs, define target schemas explicitly:

```sql
-- Good: explicit schema definition
SELECT CAST(data AS STRUCT(id INT, name VARCHAR, active BOOLEAN))
FROM external_source;
```

### 2. Use Named Struct Constructors

Prefer named struct constructors for clarity:

```sql
-- Good: field names are explicit
SELECT named_struct('id', 1, 'name', 'Alice', 'active', true);

-- Or using struct literal syntax
SELECT {id: 1, name: 'Alice', active: true};
```

### 3. Test Field Mappings

Always verify that field mappings work as expected:

```sql
-- Use arrow_typeof to verify unified schema
SELECT arrow_typeof([{x: 1, y: 2}, {y: 3, x: 4}]);
-- Result: List(Struct("x": Int32, "y": Int32))
```

### 4. Handle Partial Field Overlap Explicitly

When combining structs with partial field overlap, use explicit CAST:

```sql
-- Instead of relying on implicit coercion
SELECT [
  CAST(left_struct AS STRUCT(x INT, y INT, z INT)),
  CAST(right_struct AS STRUCT(x INT, y INT, z INT))
];
```

### 5. Document Struct Schemas

In complex queries, document the expected struct schemas:

```sql
-- Expected schema: {customer_id: INT, name: VARCHAR, age: INT}
SELECT {
  customer_id: c.id,
  name: c.name,
  age: c.age
} as customer_info
FROM customers c;
```

## Error Messages and Troubleshooting

### "Cannot coerce struct with different field counts"

**Cause:** Trying to combine structs with different numbers of fields.

**Solution:**

```sql
-- Use explicit CAST to handle missing fields
SELECT [
  CAST(struct1 AS STRUCT(a INT, b INT, c INT)),
  CAST(struct2 AS STRUCT(a INT, b INT, c INT))
];
```

### "Field X not found in struct"

**Cause:** Referencing a field name that doesn't exist in the struct.

**Solution:**

```sql
-- Verify field names match exactly (case-sensitive)
SELECT s['field_name'] FROM my_table;  -- Use bracket notation for access
-- Or use get_field function
SELECT get_field(s, 'field_name') FROM my_table;
```

### Unexpected NULL values after coercion

**Cause:** Struct coercion added NULL for missing fields.

**Solution:** Check that all structs have the required fields, or explicitly handle NULLs:

```sql
SELECT COALESCE(s['field'], default_value) FROM my_table;
```

## Related Functions

- `arrow_typeof()` - Returns the Arrow type of an expression
- `struct()` / `named_struct()` - Creates struct values
- `get_field()` - Extracts field values from structs
- `CAST()` - Explicitly casts structs to specific schemas
