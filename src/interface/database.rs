use map::IMap;

use super::types::OutputValue;

pub mod map;

pub trait IDatabase {
    type Inner;
    fn get_mut(&mut self, db_index: usize) -> &mut Self::Inner;
    fn swap(&mut self, db_index_1: usize, db_index_2: usize);
    fn len(&self) -> usize;
    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Self::Inner>;
}

pub trait IDatabaseWithInner: IDatabase
where
    Self::Inner: IMap,
{
    fn flushall(&mut self) -> OutputValue {
        for db in self.iter_mut() {
            db.flushdb();
        }
        OutputValue::Ok
    }
}
