use super::{
    cell::{Cell, RecordValue, RowId},
    err,
    page::{BtreeIndexSearch, BtreeSearch},
    sql::{
        parsers::{parse_create_index, parse_create_table},
        Conditions,
    },
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
    indexes: Vec<TableIndex>,
}

impl<'a, R: Read + Seek> Table<'a, R> {
    pub fn builder(name: &'a str) -> TableBuilder<'a, R> {
        TableBuilder::new(name)
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn search_rows(&self, conditions: &Conditions) -> Result<TableSearch<'_, R>> {
        match self.use_index(conditions) {
            Some((index, key)) => self.index_search(index, key),
            None => self.table_scan(),
        }
    }

    pub fn get_row(&self, rowid: RowId) -> Result<Option<TableRow<'_, R>>> {
        if let Some(row) = self.rows(Some(rowid))?.next() {
            if row.rowid().is_some_and(|id| id == rowid) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn table_scan(&'a self) -> Result<TableSearch<'a, R>> {
        Ok(TableSearch::Scan(self.rows(None)?))
    }

    fn index_search(&'a self, index: &'a TableIndex, key: String) -> Result<TableSearch<'a, R>> {
        Ok(TableSearch::Index(IndexRows {
            table: self,
            last_rowid: None,
            key,
            rootpage: self.db_ref.page(index.rootpage)?,
        }))
    }

    fn rows(&self, rowid: Option<RowId>) -> Result<TableRows<'_, R>> {
        Ok(TableRows {
            table: self,
            rowid,
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

    fn use_index(&self, conditions: &Conditions) -> Option<(&TableIndex, String)> {
        self.indexes.iter().find_map(|idx| idx.get_key(conditions))
    }
}

#[derive(Debug)]
pub struct TableBuilder<'a, R: Read + Seek> {
    name: &'a str,
    db_ref: Option<&'a Db<R>>,
    table_schema: Option<Schema>,
    index_schemas: Vec<Schema>,
}

impl<'a, R: Read + Seek> TableBuilder<'a, R> {
    pub fn new(name: &'a str) -> Self {
        Self {
            name,
            db_ref: None,
            table_schema: None,
            index_schemas: vec![],
        }
    }

    pub fn db(self, db: &'a Db<R>) -> Self {
        Self {
            db_ref: Some(db),
            ..self
        }
    }

    pub fn schemas(self, schemas: impl Iterator<Item = Schema>) -> Self {
        let mut table_schema: Option<Schema> = None;
        let mut index_schemas: Vec<Schema> = vec![];

        for schema in schemas.filter(|s| s.tbl_name() == self.name) {
            if schema.r#type() == "table" {
                table_schema = Some(schema);
            } else if schema.r#type() == "index" {
                index_schemas.push(schema);
            }
        }

        Self {
            table_schema,
            index_schemas,
            ..self
        }
    }

    pub fn build(self) -> Result<Table<'a, R>> {
        let db_ref = self
            .db_ref
            .ok_or(err!("A pointer to db is required to TableBuilder"))?;
        let table_schema = self
            .table_schema
            .ok_or(err!("Not found \"{}\" table", self.name))?;
        let rootpage = table_schema.rootpage();
        let sql = table_schema.sql();
        let (_, (col_defs, _)) = parse_create_table(sql).map_err(|e| err!("{e}"))?;

        let mut columns: Vec<TableColumn> = vec![];
        for col_def in col_defs {
            columns.push(TableColumn::new(col_def)?);
        }

        let mut indexes: Vec<TableIndex> = vec![];
        for idx_schema in self.index_schemas {
            let sql = idx_schema.sql();
            let (_, (cols, name)) = parse_create_index(sql).map_err(|e| err!("{e}"))?;
            indexes.push(TableIndex::new(name, cols, idx_schema.rootpage()))
        }

        Ok(Table {
            db_ref,
            rootpage,
            name: self.name.into(),
            columns,
            indexes,
        })
    }
}

#[derive(Debug)]
pub enum TableSearch<'a, R: Read + Seek> {
    Scan(TableRows<'a, R>),
    Index(IndexRows<'a, R>),
}

impl<'a, R: Read + Seek> Iterator for TableSearch<'a, R> {
    type Item = TableRow<'a, R>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Scan(scan) => scan.next(),
            Self::Index(index) => index
                .next()
                .and_then(|rowid| index.table.get_row(rowid).unwrap()),
        }
    }
}

#[derive(Debug)]
pub struct TableRows<'a, R: Read + Seek> {
    table: &'a Table<'a, R>,
    rowid: Option<RowId>,
    rootpage: Page,
}

impl<'a, R: Read + Seek> Iterator for TableRows<'a, R> {
    type Item = TableRow<'a, R>;

    fn next(&mut self) -> Option<Self::Item> {
        let rowid = self.rowid.take().unwrap_or(RowId::MIN);
        let mut search = self.rootpage.btree_scan(rowid).unwrap();

        while let BtreeSearch::Pointer(p) = search {
            search = self
                .table
                .db_ref
                .page(p)
                .and_then(|mut page| page.btree_scan(rowid))
                .unwrap();
        }

        if let BtreeSearch::Leaf(Some(cell)) = search {
            if let Some(found_rowid) = cell.rowid() {
                if found_rowid >= rowid {
                    self.rowid = Some(found_rowid + 1);
                    return Some(TableRow::new(self.table, cell));
                }
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct IndexRows<'a, R: Read + Seek> {
    table: &'a Table<'a, R>,
    last_rowid: Option<RowId>,
    key: String,
    rootpage: Page,
}

impl<R: Read + Seek> Iterator for IndexRows<'_, R> {
    type Item = RowId;

    fn next(&mut self) -> Option<Self::Item> {
        let last_rowid = self.last_rowid.take().unwrap_or(RowId::MIN);
        let mut search = self.rootpage.btree_search(last_rowid, &self.key).unwrap();
        let mut rowid_in_iterior = None;

        while let BtreeIndexSearch::PointerOrRowId(p, rowid) = search {
            rowid_in_iterior = rowid;
            search = self
                .table
                .db_ref
                .page(p)
                .and_then(|mut page| page.btree_search(last_rowid, &self.key))
                .unwrap();
        }

        if let BtreeIndexSearch::RowId(rowid_opt) = search {
            if let Some(rowid) = rowid_opt.or(rowid_in_iterior) {
                self.last_rowid = Some(rowid);
                return Some(rowid);
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

    pub fn rowid(&self) -> Option<RowId> {
        self.cell.rowid()
    }
}

static COL_DEF: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?<name>(\w+|"(\w|\s)+"))\s+(?<ty>\w+)"#).unwrap());

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
                let name = &caps["name"].trim_matches('"');
                let r#type = &caps["ty"];
                let primary_key = def.contains("primary key");

                Ok(Self {
                    r#type: r#type.to_string(),
                    name: name.to_string(),
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

#[derive(Debug)]
pub struct TableIndex {
    #[allow(unused)]
    name: String,
    columns: Vec<String>,
    rootpage: PageNum,
}

impl TableIndex {
    fn new(name: &str, columns: Vec<&str>, rootpage: PageNum) -> Self {
        Self {
            name: name.into(),
            columns: columns.iter().map(|s| s.to_string()).collect(),
            rootpage,
        }
    }

    fn cols(&self) -> Vec<&str> {
        self.columns.iter().map(|s| s.as_str()).collect()
    }

    fn get_key(&self, conditions: &Conditions) -> Option<(&Self, String)> {
        if self.cols() == conditions.cols() {
            conditions.values().first().map(|&v| (self, v.into()))
        } else {
            None
        }
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

        let def = "\"size range\" text";
        let col = TableColumn::new(def).unwrap();
        assert_eq!(
            col,
            TableColumn {
                r#type: "text".into(),
                name: "size range".into(),
                primary_key: false,
            }
        );
    }
}
