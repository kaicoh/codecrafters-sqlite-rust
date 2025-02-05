pub(super) const FILE_HEADER_SIZE: usize = 100;

#[derive(Debug)]
pub struct FileHeader([u8; FILE_HEADER_SIZE]);

impl FileHeader {
    pub(super) fn new(buf: [u8; FILE_HEADER_SIZE]) -> Self {
        Self(buf)
    }

    pub fn page_size(&self) -> u16 {
        u16::from_be_bytes([self.0[16], self.0[17]])
    }
}
