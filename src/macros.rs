#[macro_export]
macro_rules! err {
    ($fmt:expr) => {
        $crate::Error::Other(anyhow::anyhow!($fmt))
    };
    ($fmt:expr, $($arg:tt)+) => {
        $crate::Error::Other(anyhow::anyhow!($fmt, $($arg)+))
    };
}
