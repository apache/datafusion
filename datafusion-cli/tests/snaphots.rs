use insta::glob;
use insta_cmd::{assert_cmd_snapshot, get_cargo_bin};
use std::process::Command;
use std::{env, fs};

fn cli() -> Command {
    Command::new(get_cargo_bin("datafusion-cli"))
}

#[test]
fn test_snapshots() {
    if env::var("TEST_INTEGRATION").is_err() {
        eprintln!("Skipping integration tests");
        return;
    }

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"Elapsed .* seconds\.", "[ELAPSED]");
    settings.add_filter(r"DataFusion CLI v.*", "[CLI_VERSION]");

    let _bound = settings.bind_to_scope();

    glob!("sql/*.sql", |path| {
        let input = fs::read_to_string(path).unwrap();
        let file = path.file_stem().unwrap().to_string_lossy();

        assert_cmd_snapshot!(file.as_ref(), cli().pass_stdin(input))
    });
}
