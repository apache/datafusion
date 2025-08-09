use std::collections::BTreeMap;

use super::{human_readable_size, ConsumerMemoryMetrics};

/// Print summary of memory usage metrics.
///
/// Displays peak usage, cumulative allocations, and totals per operator
/// category.
pub fn print_metrics(metrics: &[ConsumerMemoryMetrics]) {
    if metrics.is_empty() {
        println!("No memory usage recorded");
        return;
    }

    let peak = metrics.iter().map(|m| m.peak).max().unwrap_or(0);
    let cumulative: usize = metrics.iter().map(|m| m.cumulative).sum();

    println!("Peak memory usage: {}", human_readable_size(peak));
    println!(
        "Cumulative allocations: {}",
        human_readable_size(cumulative)
    );

    let mut by_op: BTreeMap<&str, usize> = BTreeMap::new();
    for m in metrics {
        let category = operator_category(&m.name);
        *by_op.entry(category).or_default() += m.cumulative;
    }

    println!("Memory usage by operator:");
    for (op, bytes) in by_op {
        println!("{op}: {}", human_readable_size(bytes));
    }
}

/// Categorize operator names into high-level groups for reporting.
pub fn operator_category(name: &str) -> &str {
    if name.contains("Aggregate") {
        "Aggregation"
    } else if name.contains("Window") {
        "Window"
    } else if name.contains("Sort") {
        "Sorting"
    } else {
        name
    }
}
