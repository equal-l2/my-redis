use std::collections::HashMap;

use crate::value::Value;

#[derive(Clone, Debug, Default)]
pub struct Map {
    data: HashMap<Vec<u8>, Value>,
}

impl Map {
    pub fn get(&self, key: &[u8]) -> Value {
        self.data.get(key).cloned().unwrap_or(Value::NullBulkString)
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Value {
        self.data.insert(key.to_vec(), Value::BulkString(value));
        Value::Ok
    }

    pub fn append(&mut self, key: &[u8], value: Vec<u8>) -> Value {
        if let Some(v) = self.data.get_mut(key) {
            if let Value::BulkString(ref mut v) = v {
                v.extend(value);
                Value::Integer(v.len() as i64)
            } else {
                Value::Error(b"ERR wrong target type for 'append'".to_vec())
            }
        } else {
            let len = value.len();
            self.data.insert(key.to_vec(), Value::BulkString(value));
            Value::Integer(len as i64)
        }
    }

    pub fn exists(&self, keys: Vec<Vec<u8>>) -> Value {
        let len = keys
            .into_iter()
            .filter(|k| self.data.contains_key(k))
            .count();
        Value::Integer(len as i64)
    }

    pub fn flushdb(&mut self) -> Value {
        // TODO: support async
        self.data.clear();
        Value::Ok
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn incr_by(&mut self, key: &[u8], n: i64) -> Value {
        if let Err(e) = self.incr_decr_check_key_value(key) {
            return e;
        }
        if let Some(Value::BulkString(s)) = self.data.get_mut(key) {
            let old_value: i64 = std::str::from_utf8(s).unwrap().parse().unwrap();
            let new_value = old_value.checked_add(n);
            if let Some(i) = new_value {
                *s = i.to_string().into_bytes();
                Value::Integer(i)
            } else {
                Value::Error(b"ERR integer overflow".to_vec())
            }
        } else {
            unreachable!()
        }
    }

    pub fn decr_by(&mut self, key: &[u8], n: i64) -> Value {
        if let Err(e) = self.incr_decr_check_key_value(key) {
            return e;
        }
        if let Some(Value::BulkString(s)) = self.data.get_mut(key) {
            let old_value: i64 = std::str::from_utf8(s).unwrap().parse().unwrap();
            let new_value = old_value.checked_sub(n);
            if let Some(i) = new_value {
                *s = i.to_string().into_bytes();
                Value::Integer(i)
            } else {
                Value::Error(b"ERR integer overflow".to_vec())
            }
        } else {
            unreachable!()
        }
    }

    pub fn incr_by_float(&mut self, key: &[u8], n: f64) -> Value {
        if let Some(v) = self.data.get_mut(key) {
            if let Value::BulkString(s) = v {
                if let Some(old_value) = std::str::from_utf8(s)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    let new_value = old_value + n;
                    let new_s = new_value.to_string().into_bytes();
                    *s = new_s.clone();
                    Value::BulkString(new_s)
                } else {
                    Value::Error(b"ERR value is not an integer".to_vec())
                }
            } else {
                Value::Error(b"ERR value is not an integer".to_vec())
            }
        } else {
            self.data
                .insert(key.to_vec(), Value::BulkString(n.to_string().into_bytes()));
            self.data.get(key).unwrap().clone()
        }
    }

    fn incr_decr_check_key_value(&mut self, key: &[u8]) -> Result<(), Value> {
        if let Some(v) = self.data.get_mut(key) {
            if let Value::BulkString(s) = v {
                if std::str::from_utf8(s)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .is_some()
                {
                    Ok(())
                } else {
                    Err(Value::Error(b"ERR value is not an integer".to_vec()))
                }
            } else {
                Err(Value::Error(b"ERR value is not an integer".to_vec()))
            }
        } else {
            self.data
                .insert(key.to_vec(), Value::BulkString(b"0".to_vec()));
            Ok(())
        }
    }

    pub fn del(&mut self, keys: Vec<Vec<u8>>) -> Value {
        Value::Integer(
            keys.into_iter()
                .filter_map(|k| self.data.remove(&k))
                .count() as i64,
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
