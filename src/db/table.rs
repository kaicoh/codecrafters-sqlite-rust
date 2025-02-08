use super::{
    cell::{Cell, RowId},
    page::BtreeSearch,
    Db, Page, PageNum, Result,
};
use std::io::{Read, Seek};

#[derive(Debug)]
pub struct Table<'a, R: Read + Seek> {
    db_ref: &'a Db<R>,
    rootpage: PageNum,
}

impl<'a, R: Read + Seek> Table<'a, R> {
    pub fn new(db_ref: &'a Db<R>, rootpage: PageNum) -> Self {
        Self { db_ref, rootpage }
    }

    pub fn rows(&self) -> Result<TableRows<'a, R>> {
        Ok(TableRows {
            db_ref: self.db_ref,
            rowid: RowId::MIN,
            rootpage: self.rootpage()?,
        })
    }

    fn rootpage(&self) -> Result<Page> {
        self.db_ref.page(self.rootpage)
    }
}

#[derive(Debug)]
pub struct TableRows<'a, R: Read + Seek> {
    db_ref: &'a Db<R>,
    rowid: RowId,
    rootpage: Page,
}

impl<R: Read + Seek> Iterator for TableRows<'_, R> {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        let mut search = self.rootpage.btree_scan(self.rowid + 1).unwrap();

        while let BtreeSearch::Pointer(p) = search {
            search = self
                .db_ref
                .page(p)
                .and_then(|mut page| {
                    let num_cells = page.num_cells().unwrap();
                    println!("page num: {p}, num_cells: {num_cells}");
                    page.btree_scan(self.rowid + 1)
                })
                .unwrap();
        }

        if let BtreeSearch::Leaf(Some(cell)) = search {
            if let Some(rowid) = cell.rowid() {
                if rowid > self.rowid {
                    self.rowid = rowid;
                    return Some(cell);
                }
            }
        }
        None
    }
}
