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

- [= (equal)](#=)
- [!= (not equal)](#!=)
- [< (less than)](#<)
- [<= (less than or equal to)](#<=)
- [> (greater than)]($>)
- [>= (greater than or equal to)](#>=)
- [IS DISTINCT FROM](#IS-DISTINCT-FROM)
- [IS NOT DISTINCT FROM](#IS-NOT-DISTINCT-FROM)


Eq
NotEq
Lt
LtEq
Gt
GtEq
IsDistinctFrom
IsNotDistinctFrom
RegexMatch
RegexIMatch
RegexNotMatch
RegexNotIMatch

## Logical Operators

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
-->
