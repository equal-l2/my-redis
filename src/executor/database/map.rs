use std::collections::HashMap;

use super::value::Value;
use crate::executor::glob;
use crate::executor::types::OutputValue;
#[derive(Debug, Default)]
pub struct Map {
    data: HashMap<Vec<u8>, Value>,
}

pub trait Key: AsRef<[u8]> {}

impl Key for Vec<u8> {}
impl Key for &[u8] {}

impl Map {
    pub fn get(&self, key: impl Key) -> OutputValue {
        let Some(data) = self.data.get(key.as_ref()) else {
            return OutputValue::NullBulkString;
        };
        if let Value::String(s) = data {
            OutputValue::BulkString(s.clone())
        } else {
            OutputValue::Error(b"ERR wrong target type for 'get'".to_vec())
        }
    }

    pub fn mget(&self, key: Vec<impl Key>) -> OutputValue {
        OutputValue::Array(
            key.into_iter()
                .map(|key| {
                    if let Some(Value::String(s)) = self.data.get(key.as_ref()) {
                        OutputValue::BulkString(s.clone())
                    } else {
                        OutputValue::NullBulkString
                    }
                })
                .collect(),
        )
    }

    pub fn mset(&mut self, key_values: Vec<Vec<u8>>) -> OutputValue {
        debug_assert!(key_values.len() % 2 == 0);
        for i in (0..key_values.len()).step_by(2) {
            let key = key_values[i].clone();
            let value = key_values[i + 1].clone();
            self.data.insert(key, Value::String(value));
        }
        OutputValue::Ok
    }

    pub fn set(&mut self, key: impl Key, value: Vec<u8>) -> OutputValue {
        self.data
            .insert(key.as_ref().to_vec(), Value::String(value));
        OutputValue::Ok
    }

    pub fn append(&mut self, key: impl Key, value: Vec<u8>) -> OutputValue {
        let key = key.as_ref();
        if let Some(v) = self.data.get_mut(key) {
            if let Value::String(ref mut v) = v {
                v.extend(value);
                OutputValue::Integer(v.len() as i64)
            } else {
                OutputValue::Error(b"ERR wrong target type for 'append'".to_vec())
            }
        } else {
            let len = value.len();
            self.data.insert(key.to_vec(), Value::String(value));
            OutputValue::Integer(len as i64)
        }
    }

    pub fn strlen(&self, key: impl Key) -> OutputValue {
        if let Some(v) = self.data.get(key.as_ref()) {
            if let Value::String(ref v) = v {
                OutputValue::Integer(v.len() as i64)
            } else {
                OutputValue::Error(b"ERR wrong target type for 'strlen'".to_vec())
            }
        } else {
            OutputValue::Integer(0)
        }
    }

    pub fn exists(&self, keys: Vec<impl Key>) -> OutputValue {
        let len = keys
            .into_iter()
            .filter(|k| self.data.contains_key(k.as_ref()))
            .count();
        OutputValue::Integer(len as i64)
    }

    pub fn flushdb(&mut self) -> OutputValue {
        // TODO: support async
        self.data.clear();
        OutputValue::Ok
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn incr_by(&mut self, key: impl Key, n: i64) -> OutputValue {
        let key = key.as_ref();
        if let Err(e) = self.incr_decr_check_key_value(key) {
            return e;
        }
        if let Some(Value::String(s)) = self.data.get_mut(key) {
            let old_value: i64 = std::str::from_utf8(s).unwrap().parse().unwrap();
            let new_value = old_value.checked_add(n);
            if let Some(i) = new_value {
                *s = i.to_string().into_bytes();
                OutputValue::Integer(i)
            } else {
                OutputValue::Error(b"ERR integer overflow".to_vec())
            }
        } else {
            unreachable!()
        }
    }

    pub fn decr_by(&mut self, key: impl Key, n: i64) -> OutputValue {
        let key = key.as_ref();
        if let Err(e) = self.incr_decr_check_key_value(key) {
            return e;
        }
        if let Some(Value::String(s)) = self.data.get_mut(key) {
            let old_value: i64 = std::str::from_utf8(s).unwrap().parse().unwrap();
            let new_value = old_value.checked_sub(n);
            if let Some(i) = new_value {
                *s = i.to_string().into_bytes();
                OutputValue::Integer(i)
            } else {
                OutputValue::Error(b"ERR integer overflow".to_vec())
            }
        } else {
            unreachable!()
        }
    }

    pub fn incr_by_float(&mut self, key: impl Key, n: f64) -> OutputValue {
        let key = key.as_ref();
        if let Some(v) = self.data.get_mut(key) {
            if let Value::String(s) = v {
                if let Some(old_value) = std::str::from_utf8(s)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    let new_value = old_value + n;
                    let new_s = new_value.to_string().into_bytes();
                    *s = new_s.clone();
                    OutputValue::BulkString(new_s)
                } else {
                    OutputValue::Error(b"ERR value is not an integer".to_vec())
                }
            } else {
                OutputValue::Error(b"ERR value is not an integer".to_vec())
            }
        } else {
            self.data
                .insert(key.to_vec(), Value::String(n.to_string().into_bytes()));
            if let Value::String(s) = self.data.get(key).unwrap() {
                OutputValue::BulkString(s.clone())
            } else {
                unreachable!()
            }
        }
    }

    fn incr_decr_check_key_value(&mut self, key: impl Key) -> Result<(), OutputValue> {
        let key = key.as_ref();
        if let Some(v) = self.data.get_mut(key) {
            if let Value::String(s) = v {
                if std::str::from_utf8(s)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .is_some()
                {
                    Ok(())
                } else {
                    Err(OutputValue::Error(b"ERR value is not an integer".to_vec()))
                }
            } else {
                Err(OutputValue::Error(b"ERR value is not an integer".to_vec()))
            }
        } else {
            self.data
                .insert(key.to_owned(), Value::String(b"0".to_vec()));
            Ok(())
        }
    }

    pub fn del(&mut self, keys: Vec<impl Key>) -> OutputValue {
        OutputValue::Integer(
            keys.into_iter()
                .filter_map(|k| self.data.remove(k.as_ref()))
                .count() as i64,
        )
    }

    pub fn keys(&self, pattern: impl Key) -> OutputValue {
        let finder = glob::Finder::new(pattern.as_ref());
        OutputValue::Array(
            self.data
                .keys()
                .filter(|k| finder.do_match(k))
                .cloned()
                .map(OutputValue::BulkString)
                .collect(),
        )
    }
}
