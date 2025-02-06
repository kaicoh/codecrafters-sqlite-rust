use super::Result;
use std::io::Read;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Varint {
    value: u64,
    byte_len: usize,
}

impl Varint {
    pub fn new<R: Read>(r: &mut R) -> Result<Self> {
        let byte = read_one_byte(r)?;
        let mut bytes_read: usize = 1;
        let (mut read_more, mut num) = split_byte(byte);
        let mut value = num;

        while read_more {
            value <<= 7;
            let byte = read_one_byte(r)?;
            bytes_read += 1;
            (read_more, num) = split_byte(byte);
            value += num;
        }

        Ok(Self {
            value,
            byte_len: bytes_read,
        })
    }

    pub fn value(&self) -> u64 {
        self.value
    }

    pub fn byte_len(&self) -> usize {
        self.byte_len
    }
}

fn read_one_byte<R: Read>(r: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn split_byte(byte: u8) -> (bool, u64) {
    let more_byte_to_read = byte & 0b10000000 == 0b10000000;
    let num = (byte & 0b01111111) as u64;
    (more_byte_to_read, num)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn it_encodes_from_readable() {
        let bytes: &[u8] = b"\x07";
        let mut cursor = Cursor::new(bytes);
        let expected = Varint {
            value: 7,
            byte_len: 1,
        };
        assert_eq!(Varint::new(&mut cursor).unwrap(), expected);

        let bytes: &[u8] = b"\x1b";
        let mut cursor = Cursor::new(bytes);
        let expected = Varint {
            value: 27,
            byte_len: 1,
        };
        assert_eq!(Varint::new(&mut cursor).unwrap(), expected);

        let bytes: &[u8] = b"\x81\x47";
        let mut cursor = Cursor::new(bytes);
        let expected = Varint {
            value: 199,
            byte_len: 2,
        };
        assert_eq!(Varint::new(&mut cursor).unwrap(), expected);
    }
}
