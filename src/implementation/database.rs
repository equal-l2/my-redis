use crate::interface::database::{map::IMap, IDatabase, IDatabaseWithInner};

mod map;
mod value;

pub use map::Map;

#[derive(Debug)]
pub struct Database<I: Default> {
    db: Vec<I>,
}

impl<I: Default> Database<I> {
    pub fn new(db_count: usize) -> Self {
        Self {
            db: std::iter::repeat_with(I::default).take(db_count).collect(),
        }
    }
}

impl<I: Default> IDatabase for Database<I> {
    type Inner = I;

    fn get_mut(&mut self, db_index: usize) -> &mut Self::Inner {
        self.db.get_mut(db_index).unwrap()
    }

    fn swap(&mut self, db_index1: usize, db_index2: usize) {
        self.db.swap(db_index1, db_index2);
    }

    fn len(&self) -> usize {
        self.db.len()
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Self::Inner> {
        self.db.iter_mut()
    }
}

impl<I: Default + IMap> IDatabaseWithInner for Database<I> {}
