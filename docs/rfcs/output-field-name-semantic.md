# Datafusion output field name semantic

Start Date: 2020-05-24

## Summary

Formally specify how Datafusion should construct its output field names based on
provided user query.

## Motivation

By formalizing the output field name semantic, users will be able to access
query output using consistent field names.

## Detailed design

The proposed semantic is chosen for the following reasons:

* Ease of implementation, field names can be derived from physical expression
without having to add extra logic to pass along arbitrary user provided input.
Users are encouraged to use ALIAS expressions for full field name control.
* Mostly compatible with Spark’s behavior except literal string handling.
* Mostly backward compatible with current Datafusion’s behavior other than
function name cases and parenthesis around operator expressions.

###  Field name rules

* All field names MUST not contain relation qualifier.
  * Both `SELECT t1.id` and `SELECT id` SHOULD result in field name: `id`
* Function names MUST be converted to lowercase.
  * `SELECT AVG(c1)` SHOULD result in field name: `avg(c1)`
* Literal string MUST not be wrapped with quotes or double quotes.
  * `SELECT 'foo'` SHOULD result in field name: `foo`
* Operator expressions MUST be wrapped with parentheses.
  * `SELECT -2` SHOULD result in field name: `(- 2)`
* Operator and operand MUST be separated by spaces.
  * `SELECT 1+2` SHOULD result in field name: `(1 + 2)`
* Function arguments MUST be separated by a comma `,` and a space.
  * `SELECT f(c1,c2)` SHOULD result in field name: `f(c1, c2)`


### Examples and comparison with other systems

Data schema for test sample queries:

```
CREATE TABLE t1 (id INT, a VARCHAR(5));
INSERT INTO t1 (id, a) VALUES (1, 'foo');
INSERT INTO t1 (id, a) VALUES (2, 'bar');

CREATE TABLE t2 (id INT, b VARCHAR(5));
INSERT INTO t2 (id, b) VALUES (1, 'hello');
INSERT INTO t2 (id, b) VALUES (2, 'world');
```

#### Projected columns

Query:

```
SELECT t1.id, a, t2.id, b
FROM t1
JOIN t2 ON t1.id = t2.id
```

Datafusion Arrow record batches output:

| id | a   | id | b     |
|----|-----|----|-------|
| 1  | foo | 1  | hello |
| 2  | bar | 2  | world |


Spark, MySQL 8 and PostgreSQL 13 output:

| id | a   | id | b     |
|----|-----|----|-------|
| 1  | foo | 1  | hello |
| 2  | bar | 2  | world |

SQLite 3 output:

| id | a   | b     |
|----|-----|-------|
| 1  | foo | hello |
| 2  | bar | world |


#### Function transformed columns

Query:

```
SELECT ABS(t1.id), abs(-id) FROM t1;
```

Datafusion Arrow record batches output:

| abs(id) | abs((- id)) |
|---------|-------------|
| 1       | 1           |
| 2       | 2           |


Spark output:

| abs(id) | abs((- id)) |
|---------|-------------|
| 1       | 1           |
| 2       | 2           |


MySQL 8 output:

| ABS(t1.id) | abs(-id) |
|------------|----------|
| 1          | 1        |
| 2          | 2        |

PostgreSQL 13 output:

| abs | abs |
|-----|-----|
| 1   | 1   |
| 2   | 2   |

SQlite 3 output:

| ABS(t1.id) | abs(-id) |
|------------|----------|
| 1          | 1        |
| 2          | 2        |


#### Function with operators

Query:

```
SELECT t1.id + ABS(id), ABS(id * t1.id) FROM t1;
```

Datafusion Arrow record batches output:

| id + abs(id) | abs(id * id) |
|--------------|--------------|
| 2            | 1            |
| 4            | 4            |


Spark output:

| id + abs(id) | abs(id * id) |
|--------------|--------------|
| 2            | 1            |
| 4            | 4            |

MySQL 8 output:

| t1.id + ABS(id) | ABS(id * t1.id) |
|-----------------|-----------------|
| 2               | 1               |
| 4               | 4               |

PostgreSQL output:

| ?column? | abs |
|----------|-----|
| 2        | 1   |
| 4        | 4   |

SQLite output:

| t1.id + ABS(id) | ABS(id * t1.id) |
|-----------------|-----------------|
| 2               | 1               |
| 4               | 4               |


#### Project literals

Query:

```
SELECT 1, 2+5, 'foo_bar';
```

Datafusion Arrow record batches output:

| 1 | (2 + 5) | foo_bar |
|---|---------|---------|
| 1 | 7       | foo_bar |


Spark output:

| 1 | (2 + 5) | foo_bar |
|---|---------|---------|
| 1 | 7       | foo_bar |

MySQL output:

| 1 | 2+5 | foo_bar |
|---|-----|---------|
| 1 | 7   | foo_bar |


PostgreSQL output:

| ?column? | ?column? | ?column? |
|----------|----------|----------|
| 1        | 7        | foo_bar  |


SQLite 3 output:

| 1 | 2+5 | 'foo_bar' |
|---|-----|-----------|
| 1 | 7   | foo_bar   |


## Alternatives

Postgres's behavior is too simple. It defaults to `?column?` as the column name
in many cases, which makes output less readable than other designs.

MySQL and SQLite preserve user query input as the field name. This adds extra
implementation and runtime overhead with little gain for end uers.

In the long run, we could make output field semantic configurable so users can
pick their own preferred semantic for full compatibility with system of their
choice.

## Unresolved questions

None so far.
