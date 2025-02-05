mod cli;
pub mod db;
mod error;
#[macro_use]
mod macros;

pub type Result<T> = std::result::Result<T, Error>;
pub use cli::Cli;
pub use error::Error;
