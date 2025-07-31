use datafusion::prelude::*;
use datafusion_common::config::ConfigOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test different configuration paths
    let mut config = SessionConfig::new();

    println!("Testing configuration paths...");

    // Test the current path that's failing
    let result = config
        .options_mut()
        .set("datafusion.memory_profiling", "on_demand");
    println!("datafusion.memory_profiling: {:?}", result);

    // Test simpler paths
    let result = config.options_mut().set("memory_profiling", "on_demand");
    println!("memory_profiling: {:?}", result);

    // Test execution namespace
    let result = config
        .options_mut()
        .set("execution.memory_profiling", "on_demand");
    println!("execution.memory_profiling: {:?}", result);

    // Test runtime namespace
    let result = config
        .options_mut()
        .set("runtime.memory_profiling", "on_demand");
    println!("runtime.memory_profiling: {:?}", result);

    // Let's also print the actual structure
    let options = ConfigOptions::new();
    println!("Available configuration entries:");
    for entry in options.entries() {
        if entry.key.contains("memory") {
            println!("  {}: {}", entry.key, entry.description);
        }
    }

    Ok(())
}
