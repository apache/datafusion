# Preserve the MERGE target's visible qualifier

MERGE will persist its target's visible SQL qualifier separately from the target
provider identity and will preserve that qualifier through logical analysis,
optimization, serialization, and provider dispatch. This removes the need to rewrite
qualifier strings or reconstruct lexical scope, allowing nested aliases to shadow the
MERGE alias and allowing a source qualifier to equal the target's real table name.
DataFusion-wide scoped relation-binding IDs would model identity more rigorously, but
their public-API and optimizer migration is deliberately deferred to a separate
architecture project.

The MERGE representation and provider contract must therefore treat the qualifier as
a scope-visible name, not provider identity. New MERGE protobuf payloads are not
promised to be readable by older DataFusion versions; new readers decode pre-field
payloads by using the target table name as the qualifier.

A shared target-first expression-schema builder will enforce visible target/source
qualifier uniqueness for SQL-planned and programmatically constructed MERGE plans.
Providers may receive residual logical subqueries in MERGE expressions and must
either handle them or return an explicit unsupported error; generic subquery
execution is outside this decision.
