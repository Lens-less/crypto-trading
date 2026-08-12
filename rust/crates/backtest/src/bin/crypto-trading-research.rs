#[path = "../research_runner_shared.rs"]
mod research_runner_shared;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    research_runner_shared::run_product_cli_from_env()
}
