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

struct RespArrayParser<'a> {
    item_count: usize,
    item_buffer: Vec<Value>,
    bytes_buffer: &'a mut VecDeque<u8>,
}

impl<'a> RespArrayParser<'a> {
    fn new(item_count: usize, bytes_buffer: &'a mut VecDeque<u8>) -> Self {
        RespArrayParser {
            item_count,
            item_buffer: Vec::new(),
            bytes_buffer,
        }
    }

    fn parse(mut self) -> Option<Value> {
        for _ in 0..self.item_count {
            match self.parse_item() {
                Some(i) => self.item_buffer.push(i),
                None => return None,
            }
        }
        Some(Value::Array(self.item_buffer))
    }

    fn parse_item(&mut self) -> Option<Value> {
        let data_type = match self.bytes_buffer.pop_front() {
            Some(ch) => match ch {
                b'*' => DataType::Array,
                b'$' => DataType::BulkString,
                _ => unreachable!(),
            },
            None => unreachable!(),
        };

        // read length
        let mut len_buffer = Vec::new();
        loop {
            if let Some(ch) = self.bytes_buffer.pop_front() {
                if ch == b'\r' {
                    let tmp = self.bytes_buffer.pop_front();
                    match tmp {
                        Some(b'\n') => {
                            break;
                        }
                        _ => todo!("Error check for invalid length string"),
                    }
                }
                len_buffer.push(ch);
            }
        }
        let len_str = std::str::from_utf8(&len_buffer).expect("TODO: check UTF-8 decode error");
        let len = len_str.parse::<usize>().expect("TODO: number parse error");

        return match data_type {
            DataType::Array => {
                let subparser = RespArrayParser::new(len, self.bytes_buffer);
                subparser.parse()
            }
            DataType::BulkString => {
                if self.bytes_buffer.len() < len + 2 {
                    // not enough length for "<data>\r\n"
                    None
                } else {
                    let mut bs_buffer = Vec::with_capacity(len);
                    for _ in 0..len {
                        match self.bytes_buffer.pop_front() {
                            Some(ch) => bs_buffer.push(ch),
                            None => {
                                // not enought data, should be unreachable as we already checked the buffer
                                unreachable!()
                            }
                        }
                    }
                    assert_eq!(self.bytes_buffer.pop_front(), Some(b'\r'));
                    assert_eq!(self.bytes_buffer.pop_front(), Some(b'\n'));
                    Some(Value::BulkString(bs_buffer))
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
        }
    }

    // TODO: fix partial response parsing
    fn parse_resp(&mut self) {
        // check type
        match self.bytes_buffer.pop_front() {
            Some(ch) => match ch {
                b'*' => {}
                b'$' => todo!("TODO: bulk string outside of array"),
                _ => unreachable!(),
            },
            None => unreachable!(),
        };

        // read length
        let mut len_buffer = Vec::new();
        loop {
            if let Some(ch) = self.bytes_buffer.pop_front() {
                if ch == b'\r' {
                    let tmp = self.bytes_buffer.pop_front();
                    match tmp {
                        Some(b'\n') => {
                            break;
                        }
                        _ => todo!("Error check for invalid length string"),
                    }
                }
                len_buffer.push(ch);
            }
        }
        let len_str = std::str::from_utf8(&len_buffer).expect("TODO: check UTF-8 decode error");
        let len = len_str.parse::<usize>().expect("TODO: number parse error");

        let value = {
            let subparser = RespArrayParser::new(len, &mut self.bytes_buffer);
            subparser.parse()
        };

        if let Some(arr) = value {
            self.value_buffer.push(arr);
        }
    }

    fn parse_inline(&mut self) {
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
