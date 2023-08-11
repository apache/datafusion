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
```

### `-`

Subtraction

```sql
> SELECT 4 - 3;
```

### `*`

Multiplication

```sql
> SELECT 2 * 3;
```

### `/`

Division (integer division truncates toward zero)

```sql
> SELECT 8 / 4;
```

### `%`

Modulo (remainder)

```sql
> SELECT 8 % 3;
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
SELECT 1 = 1;
```

### `!=`

Not Equal

```sql
SELECT 1 != 2;
```

### `<`

Less Than

```sql
SELECT 3 < 4;
```

### `<=`

Less Than or Equal To

```sql
SELECT 3 <= 3;
```

### `>`

Greater Than

```sql
SELECT 6 > 5;
```

### `>=`

Greater Than or Equal To

```sql
SELECT 5 >= 5;
```

### `IS DISTINCT FROM`

Guarantees the result of a comparison is `true` or `false` and not an empty set

```sql
SELECT 0 IS DISTINCT FROM NULL;
```

### `IS NOT DISTINCT FROM`

The negation of `IS DISTINCT FROM`

```sql
SELECT NULL IS NOT DISTINCT FROM NULL;
```

### `~`

Regex Match

```sql
SELECT 'datafusion' ~ '^datafusion(-cli)*';
```

### `~*`

Regex Case-Insensitive Match

```sql
SELECT 'datafusion' ~* '^DATAFUSION(-CLI)*';
```

### `!~`

Not Regex Match

```sql
SELECT 'datafusion' !~ '^datafusion(-cli)+';
```

### `!~*`

Not Regex Case-Insensitive Match

```sql
SELECT 'datafusion' !~* '^DATAFUSION(-CLI)+';
```

## Logical Operators

- [AND](#and)
- [OR](#or)

### `AND`

Logical And

```sql
SELECT true AND true;
```

### `OR`

Logical Or

```sql
SELECT false OR true;
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
SELECT 5 & 3;
```

### `|`

Bitwise Or

```sql
SELECT 5 | 3;
```

### `#`

Bitwise Xor

```sql
SELECT 5 # 3;
```

### `>>`

Bitwise Shift Right

```sql
SELECT 5 >> 3;
```

### `<<`

Bitwise Shift Left

```sql
SELECT 5 << 3;
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
```

### `<@`

Array Is Contained By

```sql
> SELECT make_array(1,3) <@ make_array(1,2,3);
```

<!--- All Operators to document (TODO REMOVE)
Eq => "=",
NotEq => "!=",
Lt => "<",
LtEq => "<=",
Gt => ">",
GtEq => ">=",
Plus => "+",
Minus => "-",
Multiply => "*",
Divide => "/",
Modulo => "%",
And => "AND",
Or => "OR",
RegexMatch => "~",
RegexIMatch => "~*",
RegexNotMatch => "!~",
RegexNotIMatch => "!~*",
IsDistinctFrom => "IS DISTINCT FROM",
IsNotDistinctFrom => "IS NOT DISTINCT FROM",
BitwiseAnd => "&",
BitwiseOr => "|",
BitwiseXor => "#",
BitwiseShiftRight => ">>",
BitwiseShiftLeft => "<<",
StringConcat => "||",
AtArrow => "@>",
ArrowAt => "<@",
-->
