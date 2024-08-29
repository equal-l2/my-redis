mod map;
mod value;

pub use map::Map;

#[derive(Debug)]
pub struct Database {
    db: Vec<Map>,
}

impl Database {
    pub fn new(db_count: usize) -> Self {
        Self {
            db: std::iter::repeat_with(Map::default)
                .take(db_count)
                .collect(),
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
