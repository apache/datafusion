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

Feature Name: subquery

Status: in-progress

Start Date: 2021-12-27

Authors: xudong.w

RFC PR: #1373

Datafusion Issue: #1209

---

### Background
Datafusion currently only supports subquery in `from`.
Almost all of the tpch queries that datafusion currently fails are due to unsupported subqueries, such as `exist subquery`, `scalar subquery`.
To improve the integrity of Datafusion SQL support, and through all TPCH cases, more complete subquery support is needed.

---

### Goals
1. Support the `Scalar subquery`.
2. Support the `Exist/Not Exist subquery`.
3. Support the `In/Not In subquery`.
4. Support the `Any/Some/All subquery`, currently sqlparser doesn't support Any/AllSubquery expr, I opened an issue to trace. [support Any/AllSubquery exprs](https://github.com/sqlparser-rs/sqlparser-rs/issues/394)
5. Support the non-correlation subquery and correlation subquery

---

### Non-Goals
1. Correlation subquery doesn't use a more complex solution to do decorrelation, for this design, I'll use nest-loop to do decorrelation.
I'll provide some materials about more advanced decorrelation, and we can research them and optimize in subsequent iterations.
2. Some unusual subqueries won't be supported, such as list subqueries.
---

### Survey

#### PostgreSQL
In postgresql, subqueries are classified into sublink and subquery based on where they appear. Sublink occurs in constraints such as WHERE/ON.
Postgres will try to pull up sublink if possible, the relevant code is in `pull_up_sublinks_jointree_recurse` function. If pull up succeeds, sublink will be converted to join.
- [NOT IN] -> [Anti] Semi Join
- [ANY/SOME] -> Semi Join
- [NOT EXISTS] -> [Anti] Semi Join

**Note that not all sublinks can be pulled up**. Only some simple sublink queries can be pulled up. If contains aggregate functions, sublink won't be pulled up, then there will be a `SubPlan` in `Query Plan` and the upper execution plan and the subquery plan do nested loops to get the final result.

Subqueries and sublinks have similar processing logic, except they are in different locations of queries


#### CockroachDB
The cockroach's subquery design draws on the ideas in this [paper](https://dl.acm.org/doi/10.1145/1247480.1247598), you can see it to learn more details.
That's the [decorrelation rules](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/opt/norm/rules/decorrelate.opt) of the cockroach.

#### Materialize
Materialize is a streaming system with incremental backend. This backend does not support nested loops, so it has to be able to handle all decorrelation cases. 
Materialize's subquery implementation should be the most complete. This [paper](https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf) is relevant to their implementation. The paper is a little indigestible, so I didn't deep into it.

---

### Design
Let's turn our attention to Datafusion.

There are two overall steps:
1. transfer sqlparser subquery exprs to datafusion exprs.
2. Rewrite the logical plan to handle the subquery before the logical plan is logically optimized.

Now, let's take a closer look at how do we rewrite logical plan.

**Remove subquery**: extract subquery expr and transfer it to join. Subquery may be in `Filter`, `Projection`, and `Join`. The following process uses the subquery in `Filter` as an example.
1. Use `input` in `Filter` to build a new Logical Plan
2. find subquery expr from `predicate` of `Filter`
3. The processing flow varies according to the subquery type, the following process uses RewriteScalarSubquery as an example.
   ```rust
   match subquery_type {
     scalar_subquery => RewriteScalarSubquery(),
     exist_subquery => RewriteExistSubquery(),
     ...
   }
   ```
4. Left join with logical plan in 1 and logical plan in subquery, if it is a correlation subquery, then it contains correlation variables
5. Return an expr used to replace the subquery expr and get a new `Filter`


**Decorrelation**



---

### Others