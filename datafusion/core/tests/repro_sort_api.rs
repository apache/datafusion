use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_expr::SortOptions;

#[test]
fn test_sort_api_usage() -> Result<()> {
    let expr = col("a");
    
    // Old API: sort(asc, nulls_first)
    // "True, False" -> Ascending, Nulls Last
    let sort_expr = expr.clone().sort(true, false);
    
    assert_eq!(sort_expr.asc, true);
    assert_eq!(sort_expr.nulls_first, false);

    // New API: sort_by with SortOptions
    // Descending, Nulls First
    let sort_expr = expr.clone().sort_by(SortOptions::new().desc().nulls_first());
    
    assert_eq!(sort_expr.asc, false);
    assert_eq!(sort_expr.nulls_first, true);

    // New API: Ascending, Nulls Last (default)
    let sort_expr = expr.sort_by(SortOptions::new());
    
    assert_eq!(sort_expr.asc, true);
    assert_eq!(sort_expr.nulls_first, false);

    Ok(())
}


