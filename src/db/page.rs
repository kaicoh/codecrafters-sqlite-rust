use super::{err, utils, varint::Varint, PageBuffer, PageNum, Result};
use std::fmt;
use std::io::{Cursor, Read, Seek, SeekFrom};

#[derive(Debug, Default)]
pub struct PageBuilder {
    header_offset: u64,
    buf: Option<PageBuffer>,
}

impl PageBuilder {
    pub fn header_offset(self, offset: u64) -> Self {
        Self {
            header_offset: offset,
            ..self
        }
    }

    pub fn buffer(self, buf: PageBuffer) -> Self {
        Self {
            buf: Some(buf),
            ..self
        }
    }

    pub fn build(self) -> Page {
        let Self { header_offset, buf } = self;
        Page {
            header_offset,
            cursor: Cursor::new(
                buf.expect("You must set buffer to PageBuilder before building Page"),
            ),
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl PageType {
    fn new(byte: u8) -> Result<Self> {
        match byte {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0a => Ok(Self::LeafIndex),
            0x0d => Ok(Self::LeafTable),
            _ => Err(err!("Invalid byte for page type: {byte:#04x}")),
        }
    }

    fn is_leaf(&self) -> bool {
        matches!(self, Self::LeafIndex | Self::LeafTable)
    }

    fn header_size(&self) -> usize {
        if self.is_leaf() {
            8
        } else {
            12
        }
    }
}

#[derive(Debug)]
struct Header {
    r#type: PageType,
    #[allow(unused)]
    free_block_starts_at: u16,
    num_of_cells: u16,
    #[allow(unused)]
    cells_start_at: u16,
    #[allow(unused)]
    num_of_fragmented_free_bytes: u8,
    #[allow(unused)]
    right_most_pointer: Option<PageNum>,
}

impl Header {
    fn new<R: Read + Seek>(r: &mut R) -> Result<Self> {
        let r#type = PageType::new(utils::read_1_byte(r)?)?;
        let remaining_size = r#type.header_size() - 1;
        let bytes = utils::read_n_bytes(r, remaining_size)?;

        Ok(Self {
            r#type,
            free_block_starts_at: u16::from_be_bytes([bytes[0], bytes[1]]),
            num_of_cells: u16::from_be_bytes([bytes[2], bytes[3]]),
            cells_start_at: u16::from_be_bytes([bytes[4], bytes[5]]),
            num_of_fragmented_free_bytes: u8::from_be_bytes([bytes[6]]),
            right_most_pointer: if r#type.is_leaf() {
                None
            } else {
                Some(u32::from_be_bytes([
                    bytes[7], bytes[8], bytes[9], bytes[10],
                ]))
            },
        })
    }
}

#[derive(Debug)]
pub struct Page {
    header_offset: u64,
    cursor: Cursor<PageBuffer>,
}

impl Page {
    pub fn builder() -> PageBuilder {
        PageBuilder::default()
    }

    pub fn num_cells(&mut self) -> Result<usize> {
        self.header().map(|h| h.num_of_cells as usize)
    }

    pub fn cells(&mut self) -> Result<Vec<Cell>> {
        let mut cells: Vec<Cell> = vec![];
        for p in self.cell_pointers()? {
            self.set_offset(p)?;
            let record_size = Varint::new(&mut self.cursor)?;
            let rowid = Varint::new(&mut self.cursor)?;

            let buf = utils::read_n_bytes(&mut self.cursor, record_size.value() as usize)?;

            cells.push(Cell {
                rowid: rowid.value(),
                record: Record::new(buf)?,
            });
        }

        Ok(cells)
    }

    fn header(&mut self) -> Result<Header> {
        self.set_offset(self.header_offset)?;
        Header::new(&mut self.cursor)
    }

    fn r#type(&mut self) -> Result<PageType> {
        self.header().map(|h| h.r#type)
    }

    fn cell_pointers(&mut self) -> Result<Vec<u64>> {
        let num_cells = self.num_cells()?;
        self.set_offset_from_header(0)?;

        utils::read_n_bytes(&mut self.cursor, 2 * num_cells).map(|bytes| {
            bytes
                .chunks(2)
                .map(|chunk| {
                    let buf: [u8; 2] = chunk
                        .try_into()
                        .expect("Cannot turn 2 bytes slice into an array");
                    u16::from_be_bytes(buf) as u64
                })
                .collect()
        })
    }

    fn set_offset_from_header(&mut self, offset: u64) -> Result<()> {
        let header_size = self.r#type()?.header_size() as u64;
        self.set_offset(self.header_offset + header_size + offset)
    }

    fn set_offset(&mut self, offset: u64) -> Result<()> {
        self.cursor.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Cell {
    #[allow(unused)]
    rowid: u64,
    record: Record,
}

impl Cell {
    pub fn column(&self, num: usize) -> Option<RecordValue> {
        self.record.column(num)
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
                let _bytes = utils::read_3_bytes(r)?;
                unimplemented!()
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
            Self::Null => write!(f, "NULL"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::Blob(bytes) => write!(f, "{bytes:?}"),
            Self::Text(t) => write!(f, "{t}"),
        }
    }
}
