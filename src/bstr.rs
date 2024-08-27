use std::str::FromStr;
pub trait BStr {
    // required methods
    fn parse_into<T: FromStr>(&self) -> Option<T>;
    fn to_str(&self) -> Option<&str>;
    fn to_lower_string(&self) -> Option<String>;
    fn to_redis_error(&self) -> Vec<u8>;
}

impl<S: AsRef<[u8]>> BStr for S {
    fn parse_into<T: FromStr>(&self) -> Option<T> {
        std::str::from_utf8(self.as_ref())
            .ok()
            .and_then(|s| s.parse().ok())
    }

    fn to_str(&self) -> Option<&str> {
        std::str::from_utf8(self.as_ref()).ok()
    }

    fn to_lower_string(&self) -> Option<String> {
        std::str::from_utf8(self.as_ref())
            .ok()
            .map(|s| s.to_ascii_lowercase())
    }

    fn to_redis_error(&self) -> Vec<u8> {
        [b"-".as_slice(), self.as_ref(), b"\r\n"].concat()
    }
}
