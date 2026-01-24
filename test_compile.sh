#!/bin/bash
cd /Users/kosiew/GitHub/df-temp
timeout 120 cargo test -p datafusion-physical-expr expressions::cast_column::tests::cast_column_schema_mismatch 2>&1 | tail -20
