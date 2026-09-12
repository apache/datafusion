SELECT
  array_agg(utf8_value ORDER BY i64_high_cardinality ASC NULLS LAST),
  array_agg(
    utf8_value
    ORDER BY utf8_low_cardinality DESC, i64_high_cardinality DESC NULLS FIRST
  )
FROM aggregate_data;
