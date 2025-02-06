use std::io;
use std::string::FromUtf8Error;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("ERR - io: {0}")]
    Io(#[from] io::Error),

    #[error("ERR - from_utf8: {0}")]
    FromUtf8(#[from] FromUtf8Error),

    #[error("ERR - other: {0}")]
    Other(#[from] anyhow::Error),
}
