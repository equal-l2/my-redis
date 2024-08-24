use database::Map;
use smol::net::SocketAddr;

use crate::value::Value;

mod acl;
mod command;
mod connection;
mod database;

use command::COMMANDS;
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

        let name_bs = if let Some(bs) = items[0].clone().into_bulkstr() {
            bs
        } else {
            return Value::Error(b"ERR invalid request".to_vec()).to_bytes_vec();
        };

        // commands should be valid UTF-8
        let name_res = std::str::from_utf8(&name_bs).map(str::to_lowercase);

        let name = match name_res {
            Ok(name) if COMMANDS.with(|map| map.contains_key(name.as_str())) => name,
            _ => {
                return Value::Error([b"ERR unknown command ", name_bs.as_slice()].concat())
                    .to_bytes_vec()
            }
        };

        println!("Command: {name}");

        COMMANDS
            .with(|key| {
                let command = key.get(name.as_str()).unwrap();
                match command.execute(self, con_id, items.drain(1..).collect()) {
                    Some(res) => res.to_bytes_vec(),
                    _ => Value::Error(
                        [
                            b"ERR wrong number of arguments for command '",
                            name.as_bytes(),
                            b"'",
                        ]
                        .concat(),
                    )
                    .to_bytes_vec(),
                }
            })
            .to_vec()
    }

    pub fn get_db(&mut self, id: ConnectionId) -> &mut Map {
        let state = self.cons.state(&id);
        self.db.get(state.db)
    }

    pub fn select(&mut self, id: ConnectionId, arg: usize) -> Value {
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
