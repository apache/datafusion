-- 1M groups of two rows whose order in `ts` is scrambled, so the second row
-- wins in about two thirds of the groups. `ts` is Timestamp(Microsecond, "UTC"),
-- `tsn` the same instants without a time zone, `s` a string with the same
-- scrambled order.
CREATE TABLE first_last_orderings AS
SELECT value / 2 AS k,
       (value * 7919) % 1000003 AS o,
       value % 977 AS v,
       arrow_cast(
         to_timestamp_micros(1735689600000000 + ((value * 2654435761) % 1000000007) * 1000),
         'Timestamp(Microsecond, Some("UTC"))'
       ) AS ts,
       to_timestamp_micros(1735689600000000 + ((value * 2654435761) % 1000000007) * 1000) AS tsn,
       'user-' || ((value * 2654435761) % 1000000007)::VARCHAR AS s
FROM range(${FIRST_LAST_ROWS:-2000000});
