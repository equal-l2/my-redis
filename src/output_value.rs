use crate::bstr::BStr;

#[derive(Clone, Debug)]
pub enum OutputValue {
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<OutputValue>),
    NullBulkString,
    NullArray,
    Ok,
}

impl OutputValue {
    pub fn to_bytes_vec(&self) -> Vec<u8> {
        match self {
            OutputValue::SimpleString(ref v) => [b"+", v.as_slice(), b"\r\n"].concat(),
            OutputValue::Error(v) => v.to_redis_error(),
            OutputValue::Integer(i) => [b":", i.to_string().as_bytes(), b"\r\n"].concat(),
            OutputValue::BulkString(ref v) => [
                b"$",
                v.len().to_string().as_bytes(),
                b"\r\n",
                v.as_slice(),
                b"\r\n",
            ]
            .concat(),
            OutputValue::Array(ref items) => {
                let mut output = Vec::new();
                output.extend(b"*");
                output.extend(items.len().to_string().as_bytes());
                output.extend(b"\r\n");
                for item in items {
                    output.extend(item.to_bytes_vec());
                }
                output
            }
            OutputValue::NullBulkString => b"$-1\r\n".to_vec(),
            OutputValue::NullArray => b"*-1\r\n".to_vec(),
            OutputValue::Ok => b"+OK\r\n".to_vec(),
        }
    }
}
