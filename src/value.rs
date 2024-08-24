#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<Value>),
    NullBulkString,
    NullArray,
    Ok,
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
            Value::Ok => b"+OK\r\n".to_vec(),
        }
    }

    pub fn into_bulkstr(self) -> Option<Vec<u8>> {
        match self {
            Value::BulkString(v) => Some(v),
            _ => None,
        }
    }

    pub fn to_usize(&self) -> Option<usize> {
        match self {
            Value::Integer(i) if i >= &0 => Some(*i as usize),
            &Value::Integer(_) => None,
            Value::BulkString(s) | Value::SimpleString(s) => {
                std::str::from_utf8(s).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::BulkString(s) | Value::SimpleString(s) => {
                std::str::from_utf8(s).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    pub fn to_floating(&self) -> Option<f64> {
        match self {
            &Value::Integer(i) => Some(i as f64),
            Value::BulkString(s) | Value::SimpleString(s) => {
                std::str::from_utf8(s).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Value::SimpleString(v) | Value::BulkString(v) => {
                Some(std::str::from_utf8(&v).ok()?.to_string())
            }
            _ => None,
        }
    }
}
