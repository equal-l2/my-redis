#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(u64),
    BulkString(Vec<u8>),
    Array(Vec<Value>),
    NullBulkString,
    NullArray,
}

impl Value {
    pub fn to_bytes_vec(&self) -> Vec<u8> {
        match self {
            Value::SimpleString(ref v) => [b"+", v.as_slice(), b"\r\n"].concat(),
            Value::Error(v) => [b"-", v.as_slice(), b"\r\n"].concat(),
            Value::Integer(i) => [b":", i.to_string().as_bytes(), b"\r\n"].concat(),
            Value::BulkString(ref v) => [
                b"$",
                v.len().to_string().as_bytes(),
                b"\r\n",
                v.as_slice(),
                b"\r\n",
            ]
            .concat(),
            Value::Array(ref items) => {
                let mut output = Vec::new();
                output.extend(b"*");
                output.extend(items.len().to_string().as_bytes());
                output.extend(b"\r\n");
                for item in items {
                    output.extend(item.to_bytes_vec());
                }
                output
            }
            Value::NullBulkString => b"$-1\r\n".to_vec(),
            Value::NullArray => b"*-1\r\n".to_vec(),
        }
    }

    pub fn as_bulkstr(&self) -> Option<&[u8]> {
        match self {
            Value::BulkString(ref v) => Some(v.as_slice()),
            _ => None,
        }
    }
}
