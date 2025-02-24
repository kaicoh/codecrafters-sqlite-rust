mod cell;
pub mod file_header;
mod page;
mod schema_table;
mod table;
mod varint;

use super::{err, sql, utils, Error, Result};
use file_header::{FileHeader, FILE_HEADER_SIZE};
use page::Page;
use schema_table::Schema;
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    rc::Rc,
    sync::{Mutex, MutexGuard},
};
pub use table::{Table, TableRow, TableRows};

pub type DbFile = Db<File>;

impl Db<File> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        File::open(path).map(Self::new).map_err(Error::from)
    }
}

type PageNum = u32;
type Pages = HashMap<PageNum, PageBuffer>;

#[derive(Debug)]
pub struct Db<R: Read + Seek> {
    r: Mutex<R>,
    pages: Mutex<Pages>,
}

impl<R: Read + Seek> Db<R> {
    pub fn new(r: R) -> Self {
        Self {
            r: Mutex::new(r),
            pages: Mutex::new(HashMap::new()),
        }
    }

    pub fn file_header(&self) -> Result<FileHeader> {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        self.read_db(0, &mut buf)?;
        Ok(FileHeader::new(buf))
    }

    pub fn num_tables(&self) -> Result<usize> {
        self.schema_page()?.num_cells()
    }

    pub fn table_names(&self) -> Result<Vec<String>> {
        let values = self
            .schemas()?
            .map(|row| row.tbl_name().to_string())
            .collect::<Vec<String>>();
        Ok(values)
    }

    pub fn table<'a>(&'a self, name: &'a str) -> Result<Table<'a, R>> {
        let schemas = self.schemas()?;
        Table::builder(name).db(self).schemas(schemas).build()
    }

    fn schemas(&self) -> Result<impl Iterator<Item = Schema>> {
        Ok(self
            .schema_page()?
            .cells()?
            .into_iter()
            .filter_map(|cell| Schema::new(cell).ok()))
    }

    fn page(&self, num: PageNum) -> Result<Page> {
        if num == 0 {
            return Err(err!("page number must be greater than 0"));
        }

        let mut pages = self.lock_pages()?;
        let buf = match pages.get(&num) {
            Some(page_buf) => page_buf.clone(),
            None => {
                let page_size = self.file_header()?.page_size() as usize;
                let mut buf = vec![0u8; page_size];
                let offset = (num - 1) as u64 * page_size as u64;
                self.read_db(offset, &mut buf)?;

                let buf = PageBuffer::from(buf);
                pages.insert(num, buf.clone());
                buf
            }
        };

        let header_offset = if num == 1 { FILE_HEADER_SIZE } else { 0 };

        Ok(Page::builder()
            .header_offset(header_offset as u64)
            .buffer(buf)
            .build())
    }

    fn schema_page(&self) -> Result<Page> {
        self.page(1)
    }

    fn read_db(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut r = self.lock_db()?;
        r.seek(SeekFrom::Start(offset))?;
        r.read_exact(buf)?;
        Ok(())
    }

    fn lock_db(&self) -> Result<MutexGuard<R>> {
        self.r.lock().map_err(Error::from)
    }

    fn lock_pages(&self) -> Result<MutexGuard<Pages>> {
        self.pages.lock().map_err(Error::from)
    }
}

#[derive(Debug)]
pub struct PageBuffer(Rc<Vec<u8>>);

impl From<Vec<u8>> for PageBuffer {
    fn from(value: Vec<u8>) -> Self {
        Self(Rc::new(value))
    }
}

impl AsRef<[u8]> for PageBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Clone for PageBuffer {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}
