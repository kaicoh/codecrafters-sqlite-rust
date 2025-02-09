pub mod parsers;

use super::{db::Db, err, Result};
use std::io::{Read, Seek};

#[derive(Debug)]
pub enum Sql<'a> {
    Select {
        columns: Vec<&'a str>,
        table: &'a str,
    },
}

impl<'a> Sql<'a> {
    pub fn new(s: &'a str) -> Result<Self> {
        let (_, (columns, table)) = parsers::parse_select(s).map_err(|e| err!("{e}"))?;
        Ok(Self::Select { columns, table })
    }

    pub fn execute<R: Read + Seek>(self, db: &Db<R>) -> Result<Vec<String>> {
        let Self::Select {
            columns,
            table: tbl_name,
        } = self;
        let table = db.table(tbl_name)?;
        let rows = table.rows()?;

        let outputs = if count_rows(&columns) {
            vec![rows.count().to_string()]
        } else {
            rows.into_iter()
                .map(|row| {
                    columns
                        .iter()
                        .filter_map(|name| row.col(name).ok())
                        .map(|v| format!("{v}"))
                        .collect::<Vec<String>>()
                        .join("|")
                })
                .collect()
        };

        Ok(outputs)
    }
}

fn count_rows(cols: &[&str]) -> bool {
    cols.iter().any(|c| c.to_lowercase().as_str() == "count(*)")
}
