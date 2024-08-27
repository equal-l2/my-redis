use std::collections::VecDeque;
use std::iter::Extend;

pub enum ParsedValue {
    BulkString(Vec<u8>),
    Array(Vec<ParsedValue>),
}

enum DataType {
    BulkString,
    Array,
}

pub struct Parser {
    bytes_buffer: VecDeque<u8>,
}

struct RespItemParser<'a, 'b, I: ExactSizeIterator<Item = &'b u8>>
where
    'b: 'a,
{
    cursor: &'a mut I,
    _phantom: std::marker::PhantomData<&'b ()>,
}

impl<'a, 'b, I: ExactSizeIterator<Item = &'b u8>> RespItemParser<'a, 'b, I> {
    fn new(cursor: &'a mut I) -> Self {
        RespItemParser {
            cursor,
            _phantom: std::marker::PhantomData,
        }
    }

    fn parse(&mut self) -> Option<Result<ParsedValue, &'static [u8]>> {
        let data_type = match self.cursor.next()? {
            // without len
            b'+' => todo!("simple string"),
            b'-' => todo!("error"),
            b':' => todo!("integer"),
            // with len
            b'$' => DataType::BulkString,
            b'*' => DataType::Array,
            // unsupported
            _ => return Some(Err(b"ERR invalid data type")),
        };

        // TODO: handle types without len?
        // read length
        let mut len_buffer = Vec::new();
        loop {
            match self.cursor.next()? {
                b'\r' => {
                    let tmp = self.cursor.next()?;
                    match tmp {
                        b'\n' => {
                            break;
                        }
                        _ => return Some(Err(b"ERR invalid character in length")),
                    }
                }
                ch @ b'0'..=b'9' | ch @ b'-' => len_buffer.push(*ch),
                _ => return Some(Err(b"ERR invalid character in length")),
            }
        }
        let len_str =
            std::str::from_utf8(&len_buffer).expect("the buffer should only contains 0~9 and -");
        let len = match len_str.parse::<i64>() {
            Ok(len) => {
                if len < -1 {
                    return Some(Err(b"ERR negative length is not supported"));
                } else {
                    len
                }
            }
            _ => return Some(Err(b"ERR invalid length")),
        };

        match data_type {
            DataType::Array => {
                if len == -1 {
                    return Some(Err(b"ERR unexpected null array"));
                }

                let mut item_buffer = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let mut subparser = RespItemParser::new(self.cursor);
                    match subparser.parse()? {
                        Ok(v) => item_buffer.push(v),
                        Err(e) => return Some(Err(e)),
                    }
                }

                Some(Ok(ParsedValue::Array(item_buffer)))
            }
            DataType::BulkString => {
                if len == -1 {
                    return Some(Err(b"ERR unexpected null bulkstring"));
                }

                if self.cursor.len() < (len + 2) as usize {
                    // not enough length for "<data>\r\n"
                    return None;
                }

                let mut bs_buffer = Vec::<u8>::with_capacity(len as usize);
                for _ in 0..len {
                    bs_buffer.push(*self.cursor.next()?);
                }

                if self.cursor.next()? != &b'\r' {
                    return Some(Err(b"ERR expected '\\r'"));
                };
                if self.cursor.next()? != &b'\n' {
                    return Some(Err(b"ERR expected '\\n'"));
                };

                Some(Ok(ParsedValue::BulkString(bs_buffer)))
            }
        }
    }
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            bytes_buffer: VecDeque::new(),
        }
    }

    pub fn parse(&mut self) -> Option<Result<ParsedValue, &'static [u8]>> {
        // TODO: pipeline support
        if self.bytes_buffer.is_empty() {
            return None;
        }

        match self.bytes_buffer.front().unwrap() {
            &b'*' => {
                // RESP
                let mut cursor = self.bytes_buffer.iter();
                let original_len = cursor.len();
                let mut parser = RespItemParser::new(&mut cursor);

                match parser.parse()? {
                    Ok(v) => {
                        let used = original_len - cursor.len();
                        self.bytes_buffer.drain(..used);
                        Some(Ok(v))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            _ => Some(Err(b"ERR only RESP2 is supported")),
        }
    }
}

impl<'a> Extend<&'a u8> for Parser {
    fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        self.bytes_buffer.extend(iter);
    }
}
