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

# Operators

## Numerical Operators

- [+ (plus)](#id1)
- [- (minus)](#id2)
- [\* (multiply)](#id3)
- [/ (divide)](#id4)
- [% (modulo)](#id5)

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

- [= (equal)](#id6)
- [!= (not equal)](#id7)
- [< (less than)](#id8)
- [<= (less than or equal to)](#id9)
- [> (greater than)](#id10)
- [>= (greater than or equal to)](#id11)
- [IS DISTINCT FROM](#is-distinct-from)
- [IS NOT DISTINCT FROM](#is-not-distinct-from)
- [~ (regex match)](#id12)
- [~\* (regex case-insensitive match)](#id13)
- [!~ (not regex match)](#id14)
- [!~\* (not regex case-insensitive match)](#id15)

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

- [& (bitwise and)](#id16)
- [| (bitwise or)](#id17)
- [# (bitwise xor)](#id18)
- [>> (bitwise shift right)](#id19)
- [<< (bitwise shift left)](#id20)

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

- [|| (string concatenation)](#id21)
- [@> (array contains)](#id22)
- [<@ (array is contained by)](#id23)

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
