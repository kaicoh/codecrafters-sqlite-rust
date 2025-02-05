pub mod file_header;

use super::{Error, Result};
use file_header::{FileHeader, FILE_HEADER_SIZE};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

pub type DbFile = Db<File>;

impl Db<File> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        File::open(path).map(Self::new).map_err(Error::from)
    }
}

#[derive(Debug)]
pub struct Db<R: Read + Seek>(R);

impl<R: Read + Seek> Db<R> {
    pub fn new(r: R) -> Self {
        Self(r)
    }

    pub fn file_header(&mut self) -> Result<FileHeader> {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        self.read(0, &mut buf)?;
        Ok(FileHeader::new(buf))
    }

    pub fn num_tables(&mut self) -> Result<u16> {
        let mut buf = [0u8; 2];
        self.read(103, &mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }

    fn read(&mut self, start: u64, buf: &mut [u8]) -> Result<()> {
        self.0.seek(SeekFrom::Start(start))?;
        self.0.read_exact(buf)?;
        Ok(())
    }
}
