
# Tokomak Optimizer roadmap

## Extend to plan optimization
Currently the optimizer only applies to expression. It should be possible to extend this to be used for rewriting logical plans. This would require a couple different modifications to the current optimizer.

* Special rule syntax for logical plan rewrites, I'm thinking something along the lines of:
```
(FilterPlan predicate: ?table1.?key_col = ?table2.?key_col AND ?predicates], input: (CrossJoin left: ?table1 right:?table2))

(FilterPlan predicates:  ?predicates input: (InnerJoin
left: ?table1 right: ?table2 on:[]
))
```

* t
