use std::collections::HashMap;

use crate::output_value::OutputValue;

#[derive(Clone, Debug, Default)]
pub struct Map {
    data: HashMap<Vec<u8>, OutputValue>,
}

impl Map {
    pub fn get(&self, key: impl AsRef<[u8]>) -> OutputValue {
        self.data
            .get(key.as_ref())
            .cloned()
            .unwrap_or(OutputValue::NullBulkString)
    }

    pub fn set(&mut self, key: impl AsRef<[u8]>, value: Vec<u8>) -> OutputValue {
        if value.len() > const { 512 * 1024 * 1024 } {
            OutputValue::Error(b"ERR value is too large".to_vec())
        } else {
            self.data
                .insert(key.as_ref().to_vec(), OutputValue::BulkString(value));
            OutputValue::Ok
        }
    }

    pub fn append(&mut self, key: impl AsRef<[u8]>, value: Vec<u8>) -> OutputValue {
        let key = key.as_ref();
        if let Some(v) = self.data.get_mut(key) {
            if let OutputValue::BulkString(ref mut v) = v {
                v.extend(value);
                OutputValue::Integer(v.len() as i64)
            } else {
                OutputValue::Error(b"ERR wrong target type for 'append'".to_vec())
            }
        } else {
            let len = value.len();
            self.data
                .insert(key.to_vec(), OutputValue::BulkString(value));
            OutputValue::Integer(len as i64)
        }
    }

    pub fn strlen(&self, key: impl AsRef<[u8]>) -> OutputValue {
        if let Some(v) = self.data.get(key.as_ref()) {
            if let OutputValue::BulkString(ref v) = v {
                OutputValue::Integer(v.len() as i64)
            } else {
                OutputValue::Error(b"ERR wrong target type for 'strlen'".to_vec())
            }
        } else {
            OutputValue::Integer(0)
        }
    }

    pub fn exists(&self, keys: Vec<impl AsRef<[u8]>>) -> OutputValue {
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

    pub fn incr_by(&mut self, key: impl AsRef<[u8]>, n: i64) -> OutputValue {
        let key = key.as_ref();
        if let Err(e) = self.incr_decr_check_key_value(key) {
            return e;
        }
        if let Some(OutputValue::BulkString(s)) = self.data.get_mut(key) {
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

    pub fn decr_by(&mut self, key: impl AsRef<[u8]>, n: i64) -> OutputValue {
        let key = key.as_ref();
        if let Err(e) = self.incr_decr_check_key_value(key) {
            return e;
        }
        if let Some(OutputValue::BulkString(s)) = self.data.get_mut(key) {
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

    pub fn incr_by_float(&mut self, key: impl AsRef<[u8]>, n: f64) -> OutputValue {
        let key = key.as_ref();
        if let Some(v) = self.data.get_mut(key) {
            if let OutputValue::BulkString(s) = v {
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
            self.data.insert(
                key.to_vec(),
                OutputValue::BulkString(n.to_string().into_bytes()),
            );
            self.data.get(key).unwrap().clone()
        }
    }

    fn incr_decr_check_key_value(&mut self, key: impl AsRef<[u8]>) -> Result<(), OutputValue> {
        let key = key.as_ref();
        if let Some(v) = self.data.get_mut(key) {
            if let OutputValue::BulkString(s) = v {
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
                .insert(key.to_owned(), OutputValue::BulkString(b"0".to_vec()));
            Ok(())
        }
    }

    pub fn del(&mut self, keys: Vec<impl AsRef<[u8]>>) -> OutputValue {
        OutputValue::Integer(
            keys.into_iter()
                .filter_map(|k| self.data.remove(k.as_ref()))
                .count() as i64,
        )
    }

    pub fn keys(&self, pattern: impl AsRef<[u8]>) -> OutputValue {
        let finder = super::glob::Finder::new(pattern.as_ref());
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

#[derive(Debug)]
pub struct Database {
    db: Vec<Map>,
}

impl Database {
    pub fn new(db_count: usize) -> Self {
        Self {
            db: vec![Map::default(); db_count],
        }
    }

    pub fn get(&mut self, db_index: usize) -> &mut Map {
        self.db.get_mut(db_index).unwrap()
    }

    pub fn swap(&mut self, db_index1: usize, db_index2: usize) {
        self.db.swap(db_index1, db_index2);
    }

    pub fn len(&self) -> usize {
        self.db.len()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Map> {
        self.db.iter_mut()
    }
}
