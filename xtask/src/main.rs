// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! See `cargo xtask help` for usage and `.github/workflows/rust.yml` for architecture.

mod ci_jobs;
mod ci_timing;
mod ci_workflow_rust;
mod ci_workflows;
mod step_runners;
mod steps;
mod utils;

use ci_jobs::JOBS as CI_JOBS;
use ci_timing::{JobTiming, WorkflowTiming, WorkflowsTiming};
use ci_workflows::{WORKFLOWS as CI_WORKFLOWS, WorkflowInfo};
use std::env;
use std::path::PathBuf;
use std::time::SystemTime;
use steps::STEPS;

pub(crate) type Result<T> = std::result::Result<T, String>;

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if let Err(error) = Xtask::new().and_then(|xtask| xtask.run(&args)) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

pub(crate) struct Xtask {
    pub(crate) root: PathBuf,
}

impl Xtask {
    fn new() -> Result<Self> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let root = manifest_dir
            .parent()
            .ok_or_else(|| {
                format!(
                    "could not find workspace root from {}",
                    manifest_dir.display()
                )
            })?
            .to_path_buf();

        Ok(Self { root })
    }

    fn run(&self, args: &[String]) -> Result<()> {
        let Some((command, rest)) = args.split_first() else {
            self.print_help();
            return Ok(());
        };

        if matches!(command.as_str(), "-h" | "--help" | "help" | "list") {
            self.print_help();
            return Ok(());
        }

        match command.as_str() {
            "ci" => self.ci(rest),
            unknown => Err(format!(
                "unknown command `{unknown}`. Run `cargo xtask help`."
            )),
        }
    }

    fn print_help(&self) {
        println!("DataFusion local CI runner");
        println!();
        println!("Run the same CI checks locally that GitHub Actions schedules.");
        println!();
        println!("Quick Start:");
        println!("  Syntax:");
        println!("    cargo xtask ci step <step> [args]");
        println!("    cargo xtask ci job <job>");
        println!("    cargo xtask ci workflow [name]");
        println!();
        println!("  Examples:");
        println!("    cargo xtask ci step rust-test");
        println!("      Run one atomic Rust test step.");
        println!("    cargo xtask ci job linux-test");
        println!("      Run one GitHub Actions job locally.");
        println!("    cargo xtask ci step fmt --write --allow-dirty");
        println!("      Run best-effort formatting auto-fixes.");
        println!("    cargo xtask ci workflow rust");
        println!("      Reproduce `.github/workflows/rust.yml` locally.");
        println!();
        println!("Concepts:");
        println!(
            "  step      Atomic CI check item, runnable with `cargo xtask ci step <step>`"
        );
        println!("  job       Ordered list of steps matching one GitHub Actions job");
        println!("  workflow  Ordered list of jobs matching a GitHub workflow file");
        println!(
            "            Example: `cargo xtask ci workflow rust` reproduces `.github/workflows/rust.yml`"
        );
        println!();
        println!("Compatibility:");
        println!(
            "  cargo xtask ci run <step> is still accepted as an alias for `ci step`."
        );
        println!();
        println!("Common flags:");
        println!(
            "  --write        Perform best-effort auto-fixes for applicable lint errors (e.g. `cargo fmt`)"
        );
        println!(
            "  --allow-dirty  Allow `--write` to change local files with uncommitted changes"
        );
        println!();
        self.print_commands();
    }

    fn print_commands(&self) {
        println!("Steps:");
        let width = STEPS
            .iter()
            .map(|step| format!("ci step {}", step.help_usage).len())
            .max()
            .unwrap_or_default();

        for step in STEPS {
            println!(
                "  {:<width$}  {}",
                format!("ci step {}", step.help_usage),
                step.help_description
            );
        }
        println!();
        println!("Jobs:");

        let width = CI_JOBS
            .iter()
            .map(|job| format!("ci job {}", job.name).len())
            .max()
            .unwrap_or_default();

        for job in CI_JOBS {
            println!(
                "  {:<width$}  {}",
                format!("ci job {}", job.name),
                job.help_description
            );
        }
        println!();
        println!("Workflows:");

        let width = CI_WORKFLOWS
            .iter()
            .map(|workflow| workflow.help_usage.len())
            .chain(std::iter::once("ci workflow".len()))
            .max()
            .unwrap_or_default();

        println!("  {:<width$}  Run all CI workflows locally", "ci workflow");
        for workflow in CI_WORKFLOWS {
            println!(
                "  {:<width$}  {}",
                workflow.help_usage, workflow.help_description
            );
        }
        println!("  (WIP: add remaining GitHub workflows)");
    }

    fn ci(&self, args: &[String]) -> Result<()> {
        let Some((command, rest)) = args.split_first() else {
            return Err("usage: cargo xtask ci <step|job|workflow> ...".to_string());
        };

        match command.as_str() {
            "step" | "run" => self.ci_step(rest),
            "job" => self.ci_job(rest),
            "workflow" => self.ci_workflow(rest),
            "profile" => Err(
                "cargo xtask ci profile is reserved for future local profiles"
                    .to_string(),
            ),
            "-h" | "--help" | "help" => {
                self.print_help();
                Ok(())
            }
            unknown => Err(format!(
                "unknown ci command `{unknown}`. Run `cargo xtask help`."
            )),
        }
    }

    fn ci_step(&self, args: &[String]) -> Result<()> {
        let Some((command, rest)) = args.split_first() else {
            return Err("usage: cargo xtask ci step <step> [args]".to_string());
        };

        let step = steps::find_step(command).ok_or_else(|| {
            format!("unknown command `{command}`. Run `cargo xtask help`.")
        })?;

        let runner = step.runner;
        let start = SystemTime::now();
        let result = runner(self, rest);
        println!(
            "\nci step `{command}` completed in {:?}",
            start.elapsed().unwrap_or_default()
        );

        result.map_err(|error| {
            format!(
                "{}\n\nUnderlying command error:\n  {error}",
                step.error_message
            )
        })
    }

    fn ci_job(&self, args: &[String]) -> Result<()> {
        let [job_name] = args else {
            return Err("usage: cargo xtask ci job <job>".to_string());
        };

        let job = CI_JOBS
            .iter()
            .find(|job| job.name == job_name.as_str())
            .ok_or_else(|| "usage: cargo xtask ci job <job>".to_string())?;

        let mut job_timing = JobTiming::new(job);
        for step_call in job.steps {
            let step = step_call.step()?;
            let args = step_call
                .args
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<_>>();
            let runner = step.runner;
            let start = SystemTime::now();
            let result = runner(self, &args);
            job_timing.add_step_timing(step, start.elapsed().unwrap_or_default());

            if let Err(error) = result
                .map_err(|error| {
                    format!(
                        "{}\n\nUnderlying command error:\n  {error}",
                        step.error_message
                    )
                })
                .map_err(|error| {
                    format!(
                        "job `{}` failed while running `ci step {}`\n\n{error}",
                        job.name,
                        step_call.command_line()
                    )
                })
            {
                job_timing.display();
                return Err(error);
            }
        }

        job_timing.display();
        Ok(())
    }

    fn ci_workflow(&self, args: &[String]) -> Result<()> {
        match args {
            [] => {
                let mut workflows_timing = WorkflowsTiming::new();
                for workflow in CI_WORKFLOWS {
                    match self.execute_workflow(workflow) {
                        Ok(workflow_timing) => {
                            workflows_timing.add_workflow_timing(workflow_timing);
                        }
                        Err(error) => {
                            workflows_timing.display();
                            return Err(error);
                        }
                    }
                }
                workflows_timing.display();
                Ok(())
            }
            [workflow_name] => {
                let workflow = CI_WORKFLOWS
                    .iter()
                    .find(|workflow| workflow.name == workflow_name.as_str())
                    .ok_or_else(|| "usage: cargo xtask ci workflow [rust]".to_string())?;

                let workflow_timing = self.execute_workflow(workflow)?;
                workflow_timing.display();
                Ok(())
            }
            _ => Err("usage: cargo xtask ci workflow [rust]".to_string()),
        }
    }

    fn execute_workflow(
        &self,
        workflow: &'static WorkflowInfo,
    ) -> Result<WorkflowTiming> {
        let mut workflow_timing = WorkflowTiming::new(workflow);
        for job in (workflow.jobs)() {
            let mut job_timing = JobTiming::new(job);
            for step_call in job.steps {
                let step = step_call.step()?;
                let args = step_call
                    .args
                    .iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>();
                let runner = step.runner;
                let start = SystemTime::now();
                let result = runner(self, &args);
                job_timing.add_step_timing(step, start.elapsed().unwrap_or_default());

                if let Err(error) = result
                    .map_err(|error| {
                        format!(
                            "{}\n\nUnderlying command error:\n  {error}",
                            step.error_message
                        )
                    })
                    .map_err(|error| {
                        format!(
                            "job `{}` failed while running `ci step {}`\n\n{error}",
                            job.name,
                            step_call.command_line()
                        )
                    })
                    .map_err(|error| {
                        format!(
                            "workflow `{}` failed while running `ci job {}`\n\n{error}",
                            workflow.name, job.name
                        )
                    })
                {
                    workflow_timing.add_job_timing(job_timing);
                    workflow_timing.display();
                    return Err(error);
                }
            }

            workflow_timing.add_job_timing(job_timing);
        }

        Ok(workflow_timing)
    }
}
