use crate::interface::types::OutputValue;

pub trait Key: AsRef<[u8]> {}

pub trait IMap: Default {
    fn flushdb(&mut self) -> OutputValue;
}

pub trait MapAllCommands: IMap + MapStringCommands + MapMiscCommands {}

pub trait MapStringCommands {
    fn get(&self, key: impl Key) -> OutputValue;
    fn set(&mut self, key: impl Key, value: Vec<u8>) -> OutputValue;
    fn mget(&self, key: Vec<impl Key>) -> OutputValue;
    fn mset(&mut self, key_values: Vec<Vec<u8>>) -> OutputValue;
    fn msetnx(&mut self, key_values: Vec<Vec<u8>>) -> OutputValue;
    fn append(&mut self, key: impl Key, value: Vec<u8>) -> OutputValue;
    fn strlen(&self, key: impl Key) -> OutputValue;
    fn incrby(&mut self, key: impl Key, n: i64) -> OutputValue;
    fn decrby(&mut self, key: impl Key, n: i64) -> OutputValue;
    fn incrbyfloat(&mut self, key: impl Key, n: f64) -> OutputValue;

    fn incr(&mut self, key: impl Key) -> OutputValue {
        self.incrby(key, 1)
    }

    fn decr(&mut self, key: impl Key) -> OutputValue {
        self.decrby(key, 1)
    }
}

pub trait MapMiscCommands {
    fn del(&mut self, keys: Vec<impl Key>) -> OutputValue;
    fn keys(&self, pattern: impl Key) -> OutputValue;
    fn exists(&self, keys: Vec<impl Key>) -> OutputValue;
    fn len(&self) -> usize;
}
