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

# Operators and Literals

## Numerical Operators

- [+ (plus)](#op_plus)
- [- (minus)](#op_minus)
- [\* (multiply)](#op_multiply)
- [/ (divide)](#op_divide)
- [% (modulo)](#op_modulo)

(op_plus)=

### `+`

Addition

```sql
> SELECT 1 + 2;
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
```

(op_minus)=

### `-`

Subtraction

```sql
> SELECT 4 - 3;
+---------------------+
| Int64(4) - Int64(3) |
+---------------------+
| 1                   |
+---------------------+
```

(op_multiply)=

### `*`

Multiplication

```sql
> SELECT 2 * 3;
+---------------------+
| Int64(2) * Int64(3) |
+---------------------+
| 6                   |
+---------------------+
```

(op_divide)=

### `/`

Division (integer division truncates toward zero)

```sql
> SELECT 8 / 4;
+---------------------+
| Int64(8) / Int64(4) |
+---------------------+
| 2                   |
+---------------------+
```

(op_modulo)=

### `%`

Modulo (remainder)

```sql
> SELECT 7 % 3;
+---------------------+
| Int64(7) % Int64(3) |
+---------------------+
| 1                   |
+---------------------+
```

## Comparison Operators

- [= (equal)](#op_eq)
- [!= (not equal)](#op_neq)
- [< (less than)](#op_lt)
- [<= (less than or equal to)](#op_le)
- [> (greater than)](#op_gt)
- [>= (greater than or equal to)](#op_ge)
- [<=> (three-way comparison, alias for IS NOT DISTINCT FROM)](#op_spaceship)
- [IS DISTINCT FROM](#is-distinct-from)
- [IS NOT DISTINCT FROM](#is-not-distinct-from)
- [~ (regex match)](#op_re_match)
- [~\* (regex case-insensitive match)](#op_re_match_i)
- [!~ (not regex match)](#op_re_not_match)
- [!~\* (not regex case-insensitive match)](#op_re_not_match_i)

(op_eq)=

### `=`

Equal

```sql
> SELECT 1 = 1;
+---------------------+
| Int64(1) = Int64(1) |
+---------------------+
| true                |
+---------------------+
```

(op_neq)=

### `!=`

Not Equal

```sql
> SELECT 1 != 2;
+----------------------+
| Int64(1) != Int64(2) |
+----------------------+
| true                 |
+----------------------+
```

(op_lt)=

### `<`

Less Than

```sql
> SELECT 3 < 4;
+---------------------+
| Int64(3) < Int64(4) |
+---------------------+
| true                |
+---------------------+
```

(op_le)=

### `<=`

Less Than or Equal To

```sql
> SELECT 3 <= 3;
+----------------------+
| Int64(3) <= Int64(3) |
+----------------------+
| true                 |
+----------------------+
```

(op_gt)=

### `>`

Greater Than

```sql
> SELECT 6 > 5;
+---------------------+
| Int64(6) > Int64(5) |
+---------------------+
| true                |
+---------------------+
```

(op_ge)=

### `>=`

Greater Than or Equal To

```sql
> SELECT 5 >= 5;
+----------------------+
| Int64(5) >= Int64(5) |
+----------------------+
| true                 |
+----------------------+
```

(op_spaceship)=

### `<=>`

Three-way comparison operator. A NULL-safe operator that returns true if both operands are equal or both are NULL, false otherwise.

```sql
> SELECT NULL <=> NULL;
+--------------------------------+
| NULL IS NOT DISTINCT FROM NULL |
+--------------------------------+
| true                           |
+--------------------------------+
```

```sql
> SELECT 1 <=> NULL;
+------------------------------------+
| Int64(1) IS NOT DISTINCT FROM NULL |
+------------------------------------+
| false                              |
+------------------------------------+
```

```sql
> SELECT 1 <=> 2;
+----------------------------------------+
| Int64(1) IS NOT DISTINCT FROM Int64(2) |
+----------------------------------------+
| false                                  |
+----------------------------------------+
```

```sql
> SELECT 1 <=> 1;
+----------------------------------------+
| Int64(1) IS NOT DISTINCT FROM Int64(1) |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

### `IS DISTINCT FROM`

Guarantees the result of a comparison is `true` or `false` and not an empty set

```sql
> SELECT 0 IS DISTINCT FROM NULL;
+--------------------------------+
| Int64(0) IS DISTINCT FROM NULL |
+--------------------------------+
| true                           |
+--------------------------------+
```

### `IS NOT DISTINCT FROM`

The negation of `IS DISTINCT FROM`

```sql
> SELECT NULL IS NOT DISTINCT FROM NULL;
+--------------------------------+
| NULL IS NOT DISTINCT FROM NULL |
+--------------------------------+
| true                           |
+--------------------------------+
```

(op_re_match)=

### `~`

Regex Match

```sql
> SELECT 'datafusion' ~ '^datafusion(-cli)*';
+-------------------------------------------------+
| Utf8("datafusion") ~ Utf8("^datafusion(-cli)*") |
+-------------------------------------------------+
| true                                            |
+-------------------------------------------------+
```

(op_re_match_i)=

### `~*`

Regex Case-Insensitive Match

```sql
> SELECT 'datafusion' ~* '^DATAFUSION(-cli)*';
+--------------------------------------------------+
| Utf8("datafusion") ~* Utf8("^DATAFUSION(-cli)*") |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```

(op_re_not_match)=

### `!~`

Not Regex Match

```sql
> SELECT 'datafusion' !~ '^DATAFUSION(-cli)*';
+--------------------------------------------------+
| Utf8("datafusion") !~ Utf8("^DATAFUSION(-cli)*") |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```

(op_re_not_match_i)=

### `!~*`

Not Regex Case-Insensitive Match

```sql
> SELECT 'datafusion' !~* '^DATAFUSION(-cli)+';
+---------------------------------------------------+
| Utf8("datafusion") !~* Utf8("^DATAFUSION(-cli)+") |
+---------------------------------------------------+
| true                                              |
+---------------------------------------------------+
```

### `~~`

Like Match

```sql
SELECT 'datafusion' ~~ 'dat_f%n';
+---------------------------------------+
| Utf8("datafusion") ~~ Utf8("dat_f%n") |
+---------------------------------------+
| true                                  |
+---------------------------------------+
```

### `~~*`

Case-Insensitive Like Match

```sql
SELECT 'datafusion' ~~* 'Dat_F%n';
+----------------------------------------+
| Utf8("datafusion") ~~* Utf8("Dat_F%n") |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

### `!~~`

Not Like Match

```sql
SELECT 'datafusion' !~~ 'Dat_F%n';
+----------------------------------------+
| Utf8("datafusion") !~~ Utf8("Dat_F%n") |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

### `!~~*`

Not Case-Insensitive Like Match

```sql
SELECT 'datafusion' !~~* 'Dat%F_n';
+-----------------------------------------+
| Utf8("datafusion") !~~* Utf8("Dat%F_n") |
+-----------------------------------------+
| true                                    |
+-----------------------------------------+
```

## Logical Operators

- [AND](#and)
- [OR](#or)

### `AND`

Logical And

```sql
> SELECT true AND true;
+---------------------------------+
| Boolean(true) AND Boolean(true) |
+---------------------------------+
| true                            |
+---------------------------------+
```

### `OR`

Logical Or

```sql
> SELECT false OR true;
+---------------------------------+
| Boolean(false) OR Boolean(true) |
+---------------------------------+
| true                            |
+---------------------------------+
```

## Bitwise Operators

- [& (bitwise and)](#op_bit_and)
- [| (bitwise or)](#op_bit_or)
- [# (bitwise xor)](#op_bit_xor)
- [>> (bitwise shift right)](#op_shift_r)
- [<< (bitwise shift left)](#op_shift_l)

(op_bit_and)=

### `&`

Bitwise And

```sql
> SELECT 5 & 3;
+---------------------+
| Int64(5) & Int64(3) |
+---------------------+
| 1                   |
+---------------------+
```

(op_bit_or)=

### `|`

Bitwise Or

```sql
> SELECT 5 | 3;
+---------------------+
| Int64(5) | Int64(3) |
+---------------------+
| 7                   |
+---------------------+
```

(op_bit_xor)=

### `#`

Bitwise Xor (interchangeable with `^`)

```sql
> SELECT 5 # 3;
+---------------------+
| Int64(5) # Int64(3) |
+---------------------+
| 6                   |
+---------------------+
```

(op_shift_r)=

### `>>`

Bitwise Shift Right

```sql
> SELECT 5 >> 3;
+----------------------+
| Int64(5) >> Int64(3) |
+----------------------+
| 0                    |
+----------------------+
```

(op_shift_l)=

### `<<`

Bitwise Shift Left

```sql
> SELECT 5 << 3;
+----------------------+
| Int64(5) << Int64(3) |
+----------------------+
| 40                   |
+----------------------+
```

## Other Operators

- [|| (string concatenation)](#op_str_cat)
- [@> (array contains)](#op_arr_contains)
- [<@ (array is contained by)](#op_arr_contained_by)

(op_str_cat)=

### `||`

String Concatenation

```sql
> SELECT 'Hello, ' || 'DataFusion!';
+----------------------------------------+
| Utf8("Hello, ") || Utf8("DataFusion!") |
+----------------------------------------+
| Hello, DataFusion!                     |
+----------------------------------------+
```

(op_arr_contains)=

### `@>`

Array Contains

```sql
> SELECT make_array(1,2,3) @> make_array(1,3);
+-------------------------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3)) @> make_array(Int64(1),Int64(3)) |
+-------------------------------------------------------------------------+
| true                                                                    |
+-------------------------------------------------------------------------+
```

(op_arr_contained_by)=

### `<@`

Array Is Contained By

```sql
> SELECT make_array(1,3) <@ make_array(1,2,3);
+-------------------------------------------------------------------------+
| make_array(Int64(1),Int64(3)) <@ make_array(Int64(1),Int64(2),Int64(3)) |
+-------------------------------------------------------------------------+
| true                                                                    |
+-------------------------------------------------------------------------+
```

## Literals

Use single quotes for literal values. For example, the string `foo bar` is
referred to using `'foo bar'`

```sql
select 'foo';
```

### Escaping

Unlike many other languages, SQL literals do not by default support C-style escape
sequences such as `\n` for newline. Instead all characters in a `'` string are treated
literally.

To escape `'` in SQL literals, use `''`:

```sql
> select 'it''s escaped';
+----------------------+
| Utf8("it's escaped") |
+----------------------+
| it's escaped         |
+----------------------+
1 row(s) fetched.
```

Strings such as `foo\nbar` mean `\` followed by `n` (not newline):

```sql
> select 'foo\nbar';
+------------------+
| Utf8("foo\nbar") |
+------------------+
| foo\nbar         |
+------------------+
1 row(s) fetched.
Elapsed 0.005 seconds.
```

To add escaped characters such as newline or tab, instead of `\n` you use the
`E` style strings. For example, to add the text with a newline

```text
foo
bar
```

You can use `E'foo\nbar'`

```sql
> select E'foo\nbar';
+-----------------+
| Utf8("foo
bar") |
+-----------------+
| foo
bar         |
+-----------------+
```
