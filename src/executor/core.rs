use database::Map;
use smol::net::SocketAddr;

use crate::value::Value;

mod command;
mod connection;
mod database;
mod id;

use command::CommandError;
use command::COMMANDS;
use connection::ConnectionStore;
use database::Database;

pub use id::ConnectionId;

#[derive(Debug)]
pub struct ExecutorImpl {
    db: Database,
    cons: ConnectionStore,
}

pub enum DbIndexValidationError {
    InvalidSource,
    OutOfRange,
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

    pub fn validate_db_index_value(
        &self,
        db_index_value: &Value,
    ) -> Result<usize, DbIndexValidationError> {
        match db_index_value.to_usize() {
            Some(db_index) if db_index < self.db.len() => Ok(db_index),
            Some(_) => Err(DbIndexValidationError::OutOfRange),
            _ => Err(DbIndexValidationError::InvalidSource),
        }
    }

    pub fn execute(&mut self, arr: Value, con_id: ConnectionId) -> Vec<u8> {
        assert!(matches!(arr, Value::Array(_)));
        assert!(self.cons.has(&con_id));

        let mut items = match arr {
            Value::Array(items) => items,
            _ => unreachable!(),
        };

        println!("{:?}", items);

        let name_bs = if let Some(bs) = items[0].to_bulkstr() {
            bs
        } else {
            return b"-ERR invalid request\r\n".to_vec();
        };

        // commands should be valid UTF-8
        let name_res = std::str::from_utf8(name_bs).map(str::to_lowercase);

        let name = match name_res {
            Ok(name) if COMMANDS.with(|map| map.contains_key(name.as_str())) => name,
            _ => return [b"-ERR unknown command ", name_bs, b"\r\n"].concat(),
        };

        COMMANDS
            .with(|key| {
                let command = key.get(name.as_str()).unwrap();
                match command.execute(self, con_id, items.drain(1..).collect()) {
                    Ok(res) => res.to_bytes_vec(),
                    Err(CommandError::ArityMismatch) => [
                        b"ERR wrong number of arguments for command '",
                        name.as_bytes(),
                        b"'",
                    ]
                    .concat(),
                }
            })
            .to_vec()
    }

    pub fn get_db(&mut self, id: ConnectionId) -> &mut Map {
        let state = self.cons.state(&id);
        self.db.get(state.db)
    }

    pub fn select(&mut self, id: ConnectionId, arg: Value) -> Value {
        match self.validate_db_index_value(&arg) {
            Ok(db_index) => {
                self.cons.set_db(id, db_index);
                Value::Ok
            }
            Err(DbIndexValidationError::OutOfRange) => {
                Value::Error(b"ERR DB index is out of range".to_vec())
            }
            _ => Value::Error(b"ERR invalid DB index".to_vec()),
        }
    }

    pub fn swap_db(&mut self, db1: Value, db2: Value) -> Value {
        let db_index_opt1 = self.validate_db_index_value(&db1);
        let db_index_opt2 = self.validate_db_index_value(&db2);
        match (db_index_opt1, db_index_opt2) {
            (Ok(db_index1), Ok(db_index2)) => {
                self.db.swap(db_index1, db_index2);
                Value::Ok
            }
            (Err(e), _) => match e {
                DbIndexValidationError::OutOfRange => {
                    Value::Error(b"ERR first DB index is out of range".to_vec())
                }
                _ => Value::Error(b"ERR invalid first DB index".to_vec()),
            },
            (_, Err(e)) => match e {
                DbIndexValidationError::OutOfRange => {
                    Value::Error(b"ERR second DB index is out of range".to_vec())
                }
                _ => Value::Error(b"ERR invalid second DB index".to_vec()),
            },
        }
    }

    pub fn client_list(&self) -> Value {
        self.cons.list()
    }
}
