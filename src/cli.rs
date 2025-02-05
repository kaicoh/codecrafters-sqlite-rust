use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    pub db_path: PathBuf,
    pub command: String,
}
