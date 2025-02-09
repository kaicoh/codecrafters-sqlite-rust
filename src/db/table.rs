use super::{
    cell::{Cell, RecordValue, RowId},
    err,
    page::BtreeSearch,
    sql::parsers::parse_create_table,
    Db, Page, PageNum, Result, Schema,
};
use once_cell::sync::Lazy;
use regex::Regex;
use std::io::{Read, Seek};

#[derive(Debug)]
pub struct Table<'a, R: Read + Seek> {
    db_ref: &'a Db<R>,
    rootpage: PageNum,
    name: String,
    columns: Vec<TableColumn>,
}

impl<'a, R: Read + Seek> Table<'a, R> {
    pub fn new(db_ref: &'a Db<R>, schema: Schema) -> Result<Self> {
        let rootpage = schema.rootpage();
        let (_, (col_defs, name)) = parse_create_table(schema.sql()).map_err(|e| err!("{e}"))?;

        let mut columns: Vec<TableColumn> = vec![];
        for col_def in col_defs {
            columns.push(TableColumn::new(col_def)?);
        }

        Ok(Self {
            db_ref,
            rootpage,
            name: name.into(),
            columns,
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

    fn col_idx(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name() == name)
    }

    fn primary_key(&self) -> Option<&TableColumn> {
        self.columns.iter().find(|col| col.primary_key)
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
                .and_then(|mut page| page.btree_scan(self.rowid + 1))
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
        match self.table.primary_key() {
            Some(key) if key.name() == name && key.is_rowid() => self
                .cell
                .rowid()
                .map(RecordValue::PrimaryKey)
                .ok_or(err!("Invalid primary key")),
            _ => self
                .table
                .col_idx(name)
                .and_then(|idx| self.cell.column(idx))
                .ok_or(err!("Invalid column name: {name}")),
        }
    }
}

static COL_DEF: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?<name>\w+)\s+(?<ty>\w+)").unwrap());

#[derive(Debug, PartialEq)]
pub struct TableColumn {
    r#type: String,
    name: String,
    primary_key: bool,
}

impl TableColumn {
    pub fn new(def: &str) -> Result<Self> {
        match COL_DEF.captures(def) {
            Some(caps) => {
                let name = &caps["name"];
                let r#type = &caps["ty"];
                let primary_key = def.contains("primary key");

                Ok(Self {
                    r#type: r#type.into(),
                    name: name.into(),
                    primary_key,
                })
            }
            None => Err(err!("Cannot parse table column. {def}")),
        }
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn is_rowid(&self) -> bool {
        self.r#type.to_lowercase().as_str() == "integer" && self.primary_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_creates_table_column_from_string() {
        let def = "name text";
        let col = TableColumn::new(def).unwrap();
        assert_eq!(
            col,
            TableColumn {
                r#type: "text".into(),
                name: "name".into(),
                primary_key: false,
            }
        );

        let def = "id integer primary key autoincrement";
        let col = TableColumn::new(def).unwrap();
        assert_eq!(
            col,
            TableColumn {
                r#type: "integer".into(),
                name: "id".into(),
                primary_key: true,
            }
        );
    }
}
