SELECT string_agg(
  utf8_value,
  ',' ORDER BY i64_high_cardinality ASC NULLS LAST, utf8_low_cardinality DESC
)
FROM aggregate_data;
