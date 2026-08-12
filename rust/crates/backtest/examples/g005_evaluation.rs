//! Legacy compatibility wrapper for the frozen G-005 offline runner.

#[path = "../src/research_runner_shared.rs"]
mod research_runner_shared;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    research_runner_shared::run_legacy_g005_from_env()
}
