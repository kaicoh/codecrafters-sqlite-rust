use super::{
    cell::{Cell, RowId},
    err, utils, PageBuffer, PageNum, Result,
};
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
pub enum PageType {
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
        let r#type = self.r#type()?;
        let mut cells: Vec<Cell> = vec![];

        for p in self.cell_pointers()? {
            self.set_offset(p)?;
            cells.push(Cell::new(r#type, &mut self.cursor)?);
        }

        Ok(cells)
    }

    pub fn btree_scan(&mut self, key: RowId) -> Result<BtreeSearch> {
        match self.r#type()? {
            PageType::InteriorTable => {
                let page_num = match self.cells()?.into_iter().find_map(next_page(key)) {
                    Some(left) => left,
                    None => self
                        .header()?
                        .right_most_pointer
                        .ok_or(err!("Not set right most pointer in interior table page"))?,
                };
                Ok(BtreeSearch::Pointer(page_num))
            }
            PageType::LeafTable => {
                let cell = self.cells()?.into_iter().find(next_cell(key));
                Ok(BtreeSearch::Leaf(cell))
            }
            _ => Err(err!("Cannot get index b-tree node from rowid")),
        }
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
pub enum BtreeSearch {
    Pointer(PageNum),
    Leaf(Option<Cell>),
}

fn next_page(key: RowId) -> Box<dyn FnMut(Cell) -> Option<PageNum>> {
    Box::new(move |cell: Cell| {
        if let Cell::InteriorTable { left, rowid } = cell {
            if rowid >= key {
                return Some(left);
            }
        }
        None
    })
}

fn next_cell(key: RowId) -> Box<dyn FnMut(&Cell) -> bool> {
    Box::new(move |cell: &Cell| cell.rowid().is_some_and(|rowid| rowid >= key))
}
