pub mod file_header;
mod page;
mod varint;

use super::{err, utils, Error, Result};
use file_header::{FileHeader, FILE_HEADER_SIZE};
use page::Page;
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

    pub fn num_tables(&mut self) -> Result<usize> {
        self.schema_page().num_cells()
    }

    pub fn table_names(&mut self) -> Result<Vec<String>> {
        let values = self
            .schema_page()
            .cells()?
            .into_iter()
            .filter_map(|cell| cell.column(2))
            .map(|v| v.to_string())
            .collect::<Vec<String>>();
        Ok(values)
    }

    fn schema_page(&mut self) -> Page<'_, R> {
        Page::builder()
            .page_header_offset(FILE_HEADER_SIZE as u64)
            .readable(&mut self.0)
            .build()
    }

    fn read(&mut self, start: u64, buf: &mut [u8]) -> Result<()> {
        self.0.seek(SeekFrom::Start(start))?;
        self.0.read_exact(buf)?;
        Ok(())
    }
}
