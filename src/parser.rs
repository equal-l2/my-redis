use crate::value::Value;
use std::collections::VecDeque;
use std::iter::Extend;

enum DataType {
    BulkString,
    Array,
}

enum ParseMode {
    Resp,
    Inline,
    Unknown,
}

pub struct Parser {
    mode: ParseMode,
    value_buffer: Vec<Value>,
    bytes_buffer: VecDeque<u8>,
}

struct RespArrayParser<'a, 'b, I: ExactSizeIterator<Item = (usize, &'b u8)>>
where
    'b: 'a,
{
    item_count: usize,
    item_buffer: Vec<Value>,
    cursor: &'a mut I,
    _phantom: std::marker::PhantomData<&'b ()>,
}

impl<'a, 'b, I: ExactSizeIterator<Item = (usize, &'b u8)>> RespArrayParser<'a, 'b, I> {
    fn new(item_count: usize, cursor: &'a mut I) -> Self {
        RespArrayParser {
            item_count,
            item_buffer: Vec::new(),
            cursor,
            _phantom: std::marker::PhantomData,
        }
    }

    fn parse(mut self) -> Option<(Value, usize)> {
        let mut last_i = 0;
        for _ in 0..self.item_count {
            match self.parse_item() {
                Some((v, i)) => {
                    last_i = i;
                    self.item_buffer.push(v);
                }
                None => return None,
            }
        }
        Some((Value::Array(self.item_buffer), last_i))
    }

    fn parse_item(&mut self) -> Option<(Value, usize)> {
        let data_type = match self.cursor.next()?.1 {
            b'*' => DataType::Array,
            b'$' => DataType::BulkString,
            _ => todo!("neither array nor bulk string"),
        };

        // read length
        let mut len_buffer = Vec::<u8>::new();
        loop {
            match self.cursor.next()?.1 {
                b'\r' => {
                    let tmp = self.cursor.next()?.1;
                    match tmp {
                        b'\n' => {
                            break;
                        }
                        _ => todo!("invalid length string"),
                    }
                }
                ch => len_buffer.push(*ch),
            }
        }
        let len_str = std::str::from_utf8(&len_buffer).expect("TODO: check UTF-8 decode error");
        let len = len_str.parse::<usize>().expect("TODO: number parse error");

        return match data_type {
            DataType::Array => {
                let subparser = RespArrayParser::new(len, self.cursor);
                subparser.parse()
            }
            DataType::BulkString => {
                if self.cursor.len() < len + 2 {
                    // not enough length for "<data>\r\n"
                    None
                } else {
                    let mut bs_buffer = Vec::<u8>::with_capacity(len);
                    for _ in 0..len {
                        bs_buffer.push(*self.cursor.next()?.1);
                    }
                    let (i, ch) = self.cursor.next()?;
                    assert_eq!(ch, &b'\r');
                    assert_eq!(self.cursor.next()?.1, &b'\n');
                    Some((Value::BulkString(bs_buffer), i + 2))
                }
            }
        };
    }
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            mode: ParseMode::Unknown,
            value_buffer: Vec::new(),
            bytes_buffer: VecDeque::new(),
        }
    }

    pub fn parse(&mut self) {
        // TODO: pipeline support
        if self.bytes_buffer.is_empty() {
            return;
        }

        if matches!(self.mode, ParseMode::Unknown) {
            // front() is always Some as we already checked buffer is not empty
            let ch = self.bytes_buffer.front().unwrap();
            self.mode = if ch == &b'*' {
                ParseMode::Resp
            } else {
                ParseMode::Inline
            };
        }

        match self.mode {
            ParseMode::Resp => self.parse_resp(),
            ParseMode::Inline => self.parse_inline(),
            ParseMode::Unknown => unreachable!(),
        };
    }

    // TODO: fix partial response parsing
    fn parse_resp(&mut self) -> Option<()> {
        let mut cursor = self.bytes_buffer.iter().enumerate().peekable();
        // check type
        match cursor.next()?.1 {
            b'*' => {}
            b'$' => todo!("bulk string outside of array"),
            _ => todo!("neither array nor bulk string"),
        };

        // read length
        let mut len_buffer = Vec::new();
        loop {
            match cursor.next()?.1 {
                b'\r' => {
                    let tmp = cursor.next()?.1;
                    match tmp {
                        b'\n' => {
                            break;
                        }
                        ch => todo!("invalid length string, encountered {}", ch),
                    }
                }
                ch => len_buffer.push(*ch),
            }
        }
        let len_str = std::str::from_utf8(&len_buffer).expect("TODO: check UTF-8 decode error");
        let len = len_str.parse::<usize>().expect("TODO: number parse error");

        let value = {
            let subparser = RespArrayParser::new(len, &mut cursor);
            subparser.parse()
        };

        if let Some((arr, used)) = value {
            self.value_buffer.push(arr);
            self.bytes_buffer.drain(..used);
        };

        Some(())
    }

    fn parse_inline(&mut self) -> Option<()> {
        todo!("inline command is not implemented yet");
    }

    pub fn pop(&mut self) -> Option<Value> {
        self.value_buffer.pop()
    }
}

impl<'a> Extend<&'a u8> for Parser {
    fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        self.bytes_buffer.extend(iter);
    }
}
