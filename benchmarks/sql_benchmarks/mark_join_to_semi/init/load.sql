-- Synthetic inputs: no external data. Equality joins vary the fraction of
-- matching outer keys; range() produces unique, non-null keys on both sides.
CREATE TABLE mjs_outer AS
SELECT value AS id, value AS k, value % 1000 AS z, value % 16 AS g
FROM range(0, ${MJS_ROWS:-100000000});

CREATE TABLE mjs_inner_p01 AS
SELECT value AS k FROM range(0, ${MJS_ROWS:-100000000}) WHERE value % 100 < 1;

CREATE TABLE mjs_inner_p50 AS
SELECT value AS k FROM range(0, ${MJS_ROWS:-100000000}) WHERE value % 100 < 50;

CREATE TABLE mjs_inner_p99 AS
SELECT value AS k FROM range(0, ${MJS_ROWS:-100000000}) WHERE value % 100 < 99;

-- Independent size knob: the inequality join evaluates candidate pairs.
CREATE TABLE mjs_outer_small AS
SELECT value AS id, value % 1000 AS z FROM range(0, ${MJS_NLJ_ROWS:-150000});

CREATE TABLE mjs_inner_small AS
SELECT value % 1000 AS z FROM range(0, ${MJS_NLJ_ROWS:-150000});
