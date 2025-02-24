use super::{page::PageType, utils, varint::Varint, PageNum, Result};
use std::fmt;
use std::io::{Cursor, Read, Seek};

pub type RowId = u64;

#[derive(Debug)]
pub enum Cell {
    InteriorIndex { left: PageNum, payload: Record },
    InteriorTable { left: PageNum, rowid: RowId },
    LeafIndex { payload: Record },
    LeafTable { rowid: RowId, payload: Record },
}

impl Cell {
    pub fn new<R: Read + Seek>(r#type: PageType, r: &mut R) -> Result<Self> {
        match r#type {
            PageType::InteriorIndex => Self::interior_index(r),
            PageType::InteriorTable => Self::interior_table(r),
            PageType::LeafIndex => Self::leaf_index(r),
            PageType::LeafTable => Self::leaf_table(r),
        }
    }

    pub fn column(&self, num: usize) -> Option<RecordValue> {
        if let Self::LeafTable { payload, .. } = self {
            payload.column(num)
        } else {
            None
        }
    }

    pub fn rowid(&self) -> Option<RowId> {
        match self {
            Self::InteriorTable { rowid, .. } | Self::LeafTable { rowid, .. } => Some(*rowid),
            _ => None,
        }
    }

    pub fn left(&self) -> Option<PageNum> {
        match self {
            Self::InteriorIndex { left, .. } | Self::InteriorTable { left, .. } => Some(*left),
            _ => None,
        }
    }

    pub fn index_payload(&self) -> Option<(RecordValue, RowId)> {
        match self {
            Self::InteriorIndex { payload, .. } | Self::LeafIndex { payload } => {
                let key = payload.column(0);
                let rowid = payload.column(1).and_then(|v| {
                    if let RecordValue::Int(n) = v {
                        n.try_into().ok()
                    } else {
                        None
                    }
                });
                key.zip(rowid)
            }
            _ => None,
        }
    }

    fn interior_index<R: Read + Seek>(r: &mut R) -> Result<Self> {
        let left = u32::from_be_bytes(utils::read_4_bytes(r)?);
        let size = Varint::new(r)?.value() as usize;
        let bytes = utils::read_n_bytes(r, size)?;

        Ok(Self::InteriorIndex {
            left,
            payload: Record::new(bytes)?,
        })
    }

    fn interior_table<R: Read + Seek>(r: &mut R) -> Result<Self> {
        let left = u32::from_be_bytes(utils::read_4_bytes(r)?);
        let rowid = Varint::new(r)?.value();

        Ok(Self::InteriorTable { left, rowid })
    }

    fn leaf_index<R: Read + Seek>(r: &mut R) -> Result<Self> {
        let size = Varint::new(r)?.value() as usize;
        let bytes = utils::read_n_bytes(r, size)?;

        Ok(Self::LeafIndex {
            payload: Record::new(bytes)?,
        })
    }

    fn leaf_table<R: Read + Seek>(r: &mut R) -> Result<Self> {
        let size = Varint::new(r)?.value() as usize;
        let rowid = Varint::new(r)?.value();
        let bytes = utils::read_n_bytes(r, size)?;

        Ok(Self::LeafTable {
            rowid,
            payload: Record::new(bytes)?,
        })
    }
}

#[derive(Debug)]
pub struct Record(Vec<RecordValue>);

impl Record {
    fn new(bytes: Vec<u8>) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let mut headers: Vec<SerialType> = vec![];
        let header_size = Varint::new(&mut cursor)?;
        let mut bytes_read = header_size.byte_len();

        while bytes_read < header_size.value() as usize {
            let v = Varint::new(&mut cursor)?;
            bytes_read += v.byte_len();
            headers.push(SerialType::new(v.value()));
        }

        let mut values: Vec<RecordValue> = vec![];

        for header in headers {
            let value = RecordValue::new(header, &mut cursor)?;
            values.push(value);
        }

        Ok(Self(values))
    }

    fn column(&self, num: usize) -> Option<RecordValue> {
        self.0.get(num).cloned()
    }
}

#[derive(Debug)]
pub enum SerialType {
    Null,
    TwosComplement8,
    TwosComplement16,
    TwosComplement24,
    TwosComplement32,
    TwosComplement48,
    TwosComplement64,
    Float,
    Zero,
    One,
    Blob(usize),
    Text(usize),
}

impl SerialType {
    fn new(num: u64) -> Self {
        match num {
            0 => Self::Null,
            1 => Self::TwosComplement8,
            2 => Self::TwosComplement16,
            3 => Self::TwosComplement24,
            4 => Self::TwosComplement32,
            5 => Self::TwosComplement48,
            6 => Self::TwosComplement64,
            7 => Self::Float,
            8 => Self::Zero,
            9 => Self::One,
            n if n % 2 == 0 && n >= 12 => Self::Blob((n as usize - 12) / 2),
            n if n % 2 == 1 && n >= 13 => Self::Text((n as usize - 13) / 2),
            _ => panic!("Invalid serial type: {num}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecordValue {
    PrimaryKey(RowId),
    Null,
    Int(i64),
    Float(f64),
    Blob(Vec<u8>),
    Text(String),
}

impl RecordValue {
    fn new<R: Read>(r#type: SerialType, r: &mut R) -> Result<Self> {
        match r#type {
            SerialType::Null => Ok(Self::Null),
            SerialType::TwosComplement8 => {
                let byte = utils::read_1_byte(r)?;
                let val = i8::from_be_bytes([byte]);
                Ok(Self::Int(val as i64))
            }
            SerialType::TwosComplement16 => {
                let bytes = utils::read_2_bytes(r)?;
                let val = i16::from_be_bytes(bytes);
                Ok(Self::Int(val as i64))
            }
            SerialType::TwosComplement24 => {
                let [b0, b1, b2] = utils::read_3_bytes(r)?;
                let val = i32::from_be_bytes([0, b0, b1, b2]);
                Ok(Self::Int(val as i64))
            }
            SerialType::TwosComplement32 => {
                let bytes = utils::read_4_bytes(r)?;
                let val = i32::from_be_bytes(bytes);
                Ok(Self::Int(val as i64))
            }
            SerialType::TwosComplement48 => {
                let _bytes = utils::read_6_bytes(r)?;
                unimplemented!()
            }
            SerialType::TwosComplement64 => {
                let bytes = utils::read_8_bytes(r)?;
                let val = i64::from_be_bytes(bytes);
                Ok(Self::Int(val))
            }
            SerialType::Float => {
                let bytes = utils::read_8_bytes(r)?;
                let val = f64::from_be_bytes(bytes);
                Ok(Self::Float(val))
            }
            SerialType::Zero => Ok(Self::Int(0)),
            SerialType::One => Ok(Self::Int(1)),
            SerialType::Blob(n) => {
                let buf = utils::read_n_bytes(r, n)?;
                Ok(Self::Blob(buf))
            }
            SerialType::Text(n) => {
                let buf = utils::read_n_bytes(r, n)?;
                Ok(Self::Text(String::from_utf8(buf)?))
            }
        }
    }
}

impl fmt::Display for RecordValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryKey(n) => write!(f, "{n}"),
            Self::Null => write!(f, "NULL"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::Blob(bytes) => write!(f, "{bytes:?}"),
            Self::Text(t) => write!(f, "{t}"),
        }
    }
}

impl PartialEq<&str> for RecordValue {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Self::Text(t) => t.as_str() == *other,
            _ => false,
        }
    }
}

impl PartialOrd<&str> for RecordValue {
    fn partial_cmp(&self, other: &&str) -> Option<std::cmp::Ordering> {
        match self {
            Self::Text(t) => t.as_str().partial_cmp(*other),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_can_compare_with_strings() {
        let val = RecordValue::Text("foo".into());
        assert_eq!(val, "foo");
        assert!(val > "bar");
        assert!(val < "zoo");
    }
}
