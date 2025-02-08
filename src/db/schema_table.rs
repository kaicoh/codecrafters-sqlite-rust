use super::{cell::Cell, err, Error, PageNum, Result};

#[derive(Debug)]
pub struct Schema {
    r#type: String,
    name: String,
    tbl_name: String,
    rootpage: PageNum,
    sql: String,
}

impl TryFrom<Cell> for Schema {
    type Error = Error;

    fn try_from(cell: Cell) -> std::result::Result<Self, Self::Error> {
        assert!(
            matches!(cell, Cell::LeafTable { .. }),
            "The cell must be a Table Leaf"
        );

        let r#type = cell
            .column(0)
            .ok_or(err!("Schema table must have column 0"))?
            .to_string();
        let name = cell
            .column(1)
            .ok_or(err!("Schema table must have column 1"))?
            .to_string();
        let tbl_name = cell
            .column(2)
            .ok_or(err!("Schema table must have column 2"))?
            .to_string();
        let rootpage = cell
            .column(3)
            .ok_or(err!("Schema table must have column 3"))?
            .to_string()
            .parse::<PageNum>()?;
        let sql = cell
            .column(4)
            .ok_or(err!("Schema table must have column 4"))?
            .to_string();

        Ok(Self {
            r#type,
            name,
            tbl_name,
            rootpage,
            sql,
        })
    }
}

impl Schema {
    pub fn new(cell: Cell) -> Result<Self> {
        cell.try_into()
    }

    pub fn tbl_name(&self) -> &str {
        self.tbl_name.as_str()
    }
}
