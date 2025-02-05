use std::io;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("ERR - io: {0}")]
    Io(#[from] io::Error),

    #[error("ERR - other: {0}")]
    Other(#[from] anyhow::Error),
}
