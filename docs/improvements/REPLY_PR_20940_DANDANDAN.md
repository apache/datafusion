# Draft reply: PR #20940 (`MultiDistinctToCrossJoin`) vs `MultiDistinctCountRewrite`

Paste into [apache/datafusion#20940](https://github.com/apache/datafusion/pull/20940) as a comment (GitHub-flavored Markdown).

---

Hi @Dandandan — thanks for this work; the cross-join split for **multiple distinct aggregates with no `GROUP BY`** is a strong fit for workloads like ClickBench extended.

I’ve been working on a related but **different** pattern: **`GROUP BY` + several `COUNT(DISTINCT …)`** in the same aggregate (typical BI). In that situation, your rule **does not apply**, because `MultiDistinctToCrossJoin` needs an **empty** `GROUP BY` and **all** aggregates to be distinct on different columns.

A concrete example from our benchmark suite (**category `08_complex_analytical`, query `Q8.3`**) on an `orders_data` table:

```sql
-- Q8.3: Seller performance analysis
SELECT
    seller_name,
    COUNT(*) as total_orders,
    COUNT(DISTINCT delivery_city) as cities_served,
    COUNT(DISTINCT state) as states_served,
    SUM(CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END) as completed_orders,
    SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_orders,
    ROUND(100.0 * SUM(CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM orders_data
GROUP BY seller_name
HAVING COUNT(*) > 100
ORDER BY total_orders DESC
LIMIT 100;
```

This is **not** “global” multi-distinct: it’s **per `seller_name`**, with **multiple `COUNT(DISTINCT …)`** plus other aggregates. That’s the class my optimizer rule (`MultiDistinctCountRewrite`) targets — rewriting the **`COUNT(DISTINCT …)`** pieces into **joinable sub-aggregates aligned on the same `GROUP BY` keys**, with correct `NULL` handling where needed.

So in simple terms:

| | **Your PR (`MultiDistinctToCrossJoin`)** | **My work (`MultiDistinctCountRewrite`)** |
|---|------------------------------------------|-------------------------------------------|
| **Typical SQL** | `SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t` (no `GROUP BY`) | `SELECT …, COUNT(DISTINCT x), COUNT(DISTINCT y), … FROM t GROUP BY …` |
| **Example workload** | ClickBench extended–style **Q0 / Q1** | Our **Q8.3** (and similar grouped BI queries) |

They’re **complementary**: different predicates, different plans, and they can **coexist** in the optimizer pipeline (we’d want to sanity-check rule order so we don’t double-rewrite the same node).

---

## Tests for `MultiDistinctCountRewrite` (what they cover)

### Optimizer unit tests — `datafusion/optimizer/src/multi_distinct_count_rewrite.rs`

| Test | What it asserts |
|------|-----------------|
| `rewrites_two_count_distinct` | `GROUP BY a` + `COUNT(DISTINCT b)`, `COUNT(DISTINCT c)` → inner joins, per-branch null filters on `b`/`c`, `mdc_base` + two `mdc_d` aliases. |
| `rewrites_global_three_count_distinct` | No `GROUP BY`, three `COUNT(DISTINCT …)` → cross/inner join rewrite; **no** `mdc_base` (global-only path). |
| `rewrites_two_count_distinct_with_non_distinct_count` | Grouped BI-style: two distincts + `COUNT(a)` → join rewrite with **`mdc_base`** holding the non-distinct agg. |
| `does_not_rewrite_two_count_distinct_same_column` | Two `COUNT(DISTINCT b)` with different aliases → **no** rewrite (duplicate distinct key). |
| `does_not_rewrite_single_count_distinct` | Only one `COUNT(DISTINCT …)` → **no** rewrite (rule needs ≥2 distincts). |
| `rewrites_three_count_distinct_grouped` | Three grouped `COUNT(DISTINCT …)` on `b`, `c`, `a` → **two** inner joins + `mdc_base`. |
| `rewrites_interleaved_non_distinct_between_distincts` | Order `COUNT(DISTINCT b)`, `COUNT(a)`, `COUNT(DISTINCT c)` → rewrite + `mdc_base` for the middle non-distinct agg (projection order / interleaving). |
| `rewrites_count_distinct_on_cast_exprs` | `COUNT(DISTINCT CAST(b AS Int64))`, same for `c` → rewrite + null filters on the **cast** expressions. |
| `does_not_rewrite_grouping_sets_multi_distinct` | `GROUPING SETS` aggregate with two `COUNT(DISTINCT …)` → **no** rewrite (rule bails on grouping sets). |
| `does_not_rewrite_mixed_agg` | `COUNT(DISTINCT b)` + `COUNT(c)` → **no** rewrite (only **one** `COUNT(DISTINCT …)`; rule requires at least two). |

### SQL integration — `datafusion/core/tests/sql/aggregates/multi_distinct_count_rewrite.rs`

| Test | What it asserts |
|------|-----------------|
| `multi_count_distinct_matches_expected_with_nulls` | End-to-end grouped two `COUNT(DISTINCT …)` with **NULLs** in distinct columns; exact sorted batch string vs expected counts. |
| `multi_count_distinct_with_count_star_matches_expected` | `COUNT(*)` plus two `COUNT(DISTINCT …)` per group (BI-style); exact result table. |
| `multi_count_distinct_two_group_keys_matches_expected` | **`GROUP BY g1, g2`** + two distincts; verifies joins line up on **all** group keys and numerics match. |
| `multi_count_distinct_lower_matches_expected_case_collapsing` | `COUNT(DISTINCT lower(b))` with `'Abc'` / `'aBC'` plus a second distinct on `c` → **one** distinct lowered value, **two** raw `c` values (semantics follow the expression inside `COUNT(DISTINCT …)`, not raw `b`). |
| `multi_count_distinct_cast_float_to_int_collapses_nearby_values` | `COUNT(DISTINCT CAST(x AS INT))` with `1.2` / `1.3` (both → `1`) vs a second distinct on `y` → exercises **cast collision** the same way as the logical-plan `CAST(column)` tests. |

### Note: `lower(b)` and `CAST` inside `COUNT(DISTINCT …)` (reviewer question)

The rule only rewrites when each distinct aggregate is a **simple** `COUNT(DISTINCT expr)` with `expr` that is:

- a column,
- `lower`/`upper` of one column, or
- `CAST` of one column (non-volatile).

The rewrite **does not change SQL semantics**: distinct is computed on the **evaluated** values of that expression (so `'Abc'` and `'aBC'` under `lower(b)` collapse to one distinct; `1.2` and `1.3` under `CAST(x AS INT)` collapse to one distinct). The two SQL tests above lock that in end-to-end alongside the multi-distinct rewrite.

---

Happy to align naming, tests, and placement with you and the maintainers.
