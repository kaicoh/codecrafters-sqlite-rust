use super::{
    cell::{Cell, RecordValue, RowId},
    err,
    page::BtreeSearch,
    sql::parsers::parse_create_table,
    Db, Page, PageNum, Result, Schema,
};
use std::io::{Read, Seek};

#[derive(Debug)]
pub struct Table<'a, R: Read + Seek> {
    db_ref: &'a Db<R>,
    rootpage: PageNum,
    name: String,
    columns: Vec<(String, String)>,
}

impl<'a, R: Read + Seek> Table<'a, R> {
    pub fn new(db_ref: &'a Db<R>, schema: Schema) -> Result<Self> {
        let rootpage = schema.rootpage();
        let (_, (columns, name)) = parse_create_table(schema.sql()).map_err(|e| err!("{e}"))?;

        Ok(Self {
            db_ref,
            rootpage,
            name: name.into(),
            columns: columns
                .into_iter()
                .map(|(a, b)| (a.into(), b.into()))
                .collect(),
        })
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn rows(&'a self) -> Result<TableRows<'a, R>> {
        Ok(TableRows {
            table: self,
            rowid: RowId::MIN,
            rootpage: self.rootpage()?,
        })
    }

    pub fn cols(&self) -> impl Iterator<Item = &String> {
        self.columns.iter().map(|(name, _)| name)
    }

    fn col_idx(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|(col, _)| col.as_str() == name)
    }

    fn rootpage(&self) -> Result<Page> {
        self.db_ref.page(self.rootpage)
    }
}

#[derive(Debug)]
pub struct TableRows<'a, R: Read + Seek> {
    table: &'a Table<'a, R>,
    rowid: RowId,
    rootpage: Page,
}

impl<'a, R: Read + Seek> Iterator for TableRows<'a, R> {
    type Item = TableRow<'a, R>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut search = self.rootpage.btree_scan(self.rowid + 1).unwrap();

        while let BtreeSearch::Pointer(p) = search {
            search = self
                .table
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
                    return Some(TableRow::new(self.table, cell));
                }
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct TableRow<'a, R: Read + Seek> {
    table: &'a Table<'a, R>,
    cell: Cell,
}

impl<'a, R: Read + Seek> TableRow<'a, R> {
    pub fn new(table: &'a Table<'a, R>, cell: Cell) -> Self {
        Self { table, cell }
    }

    pub fn col(&self, name: &str) -> Result<RecordValue> {
        self.table
            .col_idx(name)
            .and_then(|idx| self.cell.column(idx))
            .ok_or(err!("Invalid column name: {name}"))
    }
}
