use std::collections::HashMap;

use crate::value::Value;

#[derive(Clone, Debug, Default)]
pub struct Map {
    data: HashMap<Vec<u8>, Value>,
}

impl Map {
    pub fn get(&self, key: &Value) -> Value {
        if let Some(k) = key.to_bulkstr() {
            self.data.get(k).cloned().unwrap_or(Value::NullBulkString)
        } else {
            todo!()
        }
    }

    pub fn set(&mut self, key: &Value, value: &Value) -> Value {
        if let Some(k) = key.to_bulkstr() {
            self.data.insert(k.to_vec(), value.clone());
            Value::Ok
        } else {
            todo!()
        }
    }

    pub fn flushdb(&mut self) -> Value {
        // TODO: support async
        self.data.clear();
        Value::Ok
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
}
