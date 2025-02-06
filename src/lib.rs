mod cli;
pub mod db;
mod error;
#[macro_use]
mod macros;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;
pub use cli::Cli;
pub use error::Error;
