use database::Map;
use smol::net::SocketAddr;

use crate::value::Value;

mod acl;
mod command;
mod connection;
mod database;

use command::{Command, CONTAINER_COMMANDS, SIMPLE_COMMANDS};
use connection::ConnectionStore;
use database::Database;

pub use connection::ConnectionId;

#[derive(Debug)]
pub struct ExecutorImpl {
    db: Database,
    cons: ConnectionStore,
}

impl ExecutorImpl {
    pub fn new(db_count: usize) -> Self {
        ExecutorImpl {
            db: Database::new(db_count),
            cons: ConnectionStore::default(),
        }
    }

    pub fn connect(&mut self, addr: SocketAddr) -> Option<ConnectionId> {
        self.cons.connect(addr)
    }

    pub fn disconnect(&mut self, con_id: ConnectionId) {
        self.cons.disconnect(&con_id);
    }

    pub fn validate_db_index_value(&self, db_index_value: usize) -> Option<usize> {
        if db_index_value < self.db.len() {
            Some(db_index_value)
        } else {
            None
        }
    }

    pub fn execute(&mut self, arr: Value, con_id: ConnectionId) -> Vec<u8> {
        assert!(matches!(arr, Value::Array(_)));
        assert!(self.cons.has(&con_id));

        let mut items = match arr {
            Value::Array(items) => items,
            _ => unreachable!(),
        };

        //println!("{:?}", items);

        let Some(name_bs) = items[0].clone().into_bulkstr() else {
            return Value::Error(b"ERR invalid request".to_vec()).to_bytes_vec();
        };

        // commands should be valid UTF-8
        let Ok(name) = std::str::from_utf8(&name_bs).map(str::to_lowercase) else {
            return Value::Error([b"ERR unknown command ", name_bs.as_slice()].concat())
                .to_bytes_vec();
        };

        if let Some(v) = SIMPLE_COMMANDS.with(|key| {
            key.get(name.as_str()).map(|command| {
                command
                    .execute(name.as_str(), self, &con_id, items.drain(1..).collect())
                    .to_bytes_vec()
            })
        }) {
            return v;
        }

        if let Some(v) = CONTAINER_COMMANDS.with(|key| {
            key.get(name.as_str()).map(|command| {
                command
                    .execute(name.as_str(), self, &con_id, items.drain(1..).collect())
                    .to_bytes_vec()
            })
        }) {
            return v;
        }

        Value::Error([b"ERR unknown command ", name_bs.as_slice()].concat()).to_bytes_vec()
    }

    pub fn get_db(&mut self, id: &ConnectionId) -> &mut Map {
        let state = self.cons.state(id);
        self.db.get(state.db)
    }

    pub fn select(&mut self, id: &ConnectionId, arg: usize) -> Value {
        match self.validate_db_index_value(arg) {
            Some(db_index) => {
                self.cons.set_db(id, db_index);
                Value::Ok
            }
            None => Value::Error(b"ERR DB index is out of range".to_vec()),
        }
    }

    pub fn swap_db(&mut self, db1: usize, db2: usize) -> Value {
        let db_index_opt1 = self.validate_db_index_value(db1);
        let db_index_opt2 = self.validate_db_index_value(db2);
        match (db_index_opt1, db_index_opt2) {
            (Some(db_index1), Some(db_index2)) => {
                self.db.swap(db_index1, db_index2);
                Value::Ok
            }
            (None, _) => Value::Error(b"ERR first DB index is out of range".to_vec()),
            (_, None) => Value::Error(b"ERR second DB index is out of range".to_vec()),
        }
    }

    pub fn flushall(&mut self) -> Value {
        for db in self.db.iter_mut() {
            db.flushdb();
        }
        Value::Ok
    }

    pub fn client_list(&self) -> Value {
        self.cons.list()
    }
}
