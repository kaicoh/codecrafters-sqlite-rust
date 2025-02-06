use super::Result;
use paste::paste;
use std::io::Read;

pub fn read_1_byte<R: Read>(r: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_n_bytes<R: Read>(r: &mut R, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

macro_rules! read_bytes {
    ($($num:expr),*) => {
        paste! {
            $(
                pub fn [<read_ $num _bytes>]<R: Read>(r: &mut R) -> Result<[u8; $num]> {
                    let mut buf = [0u8; $num];
                    r.read_exact(&mut buf)?;
                    Ok(buf)
                }
            )*
        }
    };
}

read_bytes!(2, 3, 4, 6, 8);
