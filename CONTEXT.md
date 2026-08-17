# DataFusion SQL Name Resolution

This context distinguishes catalog identity, SQL-visible names, and lexical bindings
while planning DataFusion statements.

## Language

**Provider identity**:
The catalog-resolved table reference used to locate a target `TableProvider`.
_Avoid_: Target qualifier, target alias

**Visible qualifier**:
The normalized SQL relation name exposed in one lexical scope, such as `t` after
`target AS t`. The same text may name different relations in nested scopes.
_Avoid_: Relation identity, binding identity

**Relation binding**:
The particular relation occurrence selected by lexical name resolution. A relation
binding is distinct from both its provider identity and its visible qualifier.
_Avoid_: Qualifier string
