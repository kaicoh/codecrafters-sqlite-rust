pub mod parsers;

use super::{
    db::{Db, TableRow},
    err, Result,
};
use std::io::{Read, Seek};

#[derive(Debug)]
pub enum Sql<'a> {
    Select {
        columns: Vec<&'a str>,
        table: &'a str,
        conditions: Conditions,
    },
}

impl<'a> Sql<'a> {
    pub fn new(s: &'a str) -> Result<Self> {
        let (_, (columns, table, conditions)) =
            parsers::parse_select(s).map_err(|e| err!("{e}"))?;
        Ok(Self::Select {
            columns,
            table,
            conditions: Conditions(conditions.into_iter().map(Condition::new).collect()),
        })
    }

    pub fn execute<R: Read + Seek>(self, db: &Db<R>) -> Result<Vec<String>> {
        let Self::Select {
            columns,
            table: tbl_name,
            conditions,
        } = self;
        let table = db.table(tbl_name)?;
        let rows = table.rows()?.filter(|row| conditions.satisfy(row));

        let outputs = if count_rows(&columns) {
            vec![rows.count().to_string()]
        } else {
            rows.map(|row| {
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

#[derive(Debug)]
pub enum Condition {
    Eq { col: String, value: String },
}

impl Condition {
    fn new((col, value): (&str, &str)) -> Self {
        Self::Eq {
            col: col.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug)]
pub struct Conditions(Vec<Condition>);

impl Conditions {
    fn satisfy<R: Read + Seek>(&self, row: &TableRow<'_, R>) -> bool {
        self.0.iter().all(|condition| match condition {
            Condition::Eq { col, value } => row.col(col).is_ok_and(|v| v == value.as_str()),
        })
    }
}
