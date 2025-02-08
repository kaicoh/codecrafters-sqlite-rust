use super::err;
use std::io;
use std::num::ParseIntError;
use std::string::FromUtf8Error;
use std::sync::{MutexGuard, PoisonError};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("ERR - io: {0}")]
    Io(#[from] io::Error),

    #[error("ERR - from_utf8: {0}")]
    FromUtf8(#[from] FromUtf8Error),

    #[error("ERR - parse int: {0}")]
    ParseInt(#[from] ParseIntError),

    #[error("ERR - other: {0}")]
    Other(#[from] anyhow::Error),
}

impl<T> From<PoisonError<MutexGuard<'_, T>>> for Error {
    fn from(value: PoisonError<MutexGuard<'_, T>>) -> Self {
        err!("{value}")
    }
}
