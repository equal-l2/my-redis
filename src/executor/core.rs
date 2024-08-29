use smol::net::SocketAddr;

use crate::bstr::BStr;

use super::command::{Command, CONTAINER_COMMANDS, SIMPLE_COMMANDS};
use super::connection::ConnectionId;
use super::connection::ConnectionStore;
use super::database::Database;
use super::database::Map;
use super::types::OutputValue;

use super::InputValue;

#[derive(Debug)]
pub struct Executor {
    db: Database,
    cons: ConnectionStore,
}

impl Executor {
    pub fn new(db_count: usize) -> Self {
        Executor {
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

    pub fn execute(&mut self, mut input: Vec<InputValue>, con_id: ConnectionId) -> Vec<u8> {
        debug_assert!(self.cons.has(&con_id));

        let name_bs = input[0].clone();

        // commands should be valid UTF-8
        let Ok(name) = std::str::from_utf8(&name_bs).map(str::to_ascii_lowercase) else {
            return [b"ERR unknown command '", name_bs.as_slice(), b"'"]
                .concat()
                .to_redis_error();
        };

        if let Some(v) = SIMPLE_COMMANDS.with(|key| {
            key.get(name.as_str()).map(|command| {
                command
                    .execute(name.as_str(), self, &con_id, input.drain(1..).collect())
                    .to_bytes_vec()
            })
        }) {
            return v;
        }

        if let Some(v) = CONTAINER_COMMANDS.with(|key| {
            key.get(name.as_str()).map(|command| {
                command
                    .execute(name.as_str(), self, &con_id, input.drain(1..).collect())
                    .to_bytes_vec()
            })
        }) {
            return v;
        }

        return [b"ERR unknown command '", name_bs.as_slice(), b"'"]
            .concat()
            .to_redis_error();
    }

    pub fn get_db(&mut self, id: &ConnectionId) -> &mut Map {
        let state = self.cons.state(id);
        self.db.get(state.db)
    }

    pub fn select(&mut self, id: &ConnectionId, arg: usize) -> OutputValue {
        match self.validate_db_index_value(arg) {
            Some(db_index) => {
                self.cons.set_db(id, db_index);
                OutputValue::Ok
            }
            None => OutputValue::Error(b"ERR DB index is out of range".to_vec()),
        }
    }

    pub fn swap_db(&mut self, db1: usize, db2: usize) -> OutputValue {
        let db_index_opt1 = self.validate_db_index_value(db1);
        let db_index_opt2 = self.validate_db_index_value(db2);
        match (db_index_opt1, db_index_opt2) {
            (Some(db_index1), Some(db_index2)) => {
                self.db.swap(db_index1, db_index2);
                OutputValue::Ok
            }
            (None, _) => OutputValue::Error(b"ERR first DB index is out of range".to_vec()),
            (_, None) => OutputValue::Error(b"ERR second DB index is out of range".to_vec()),
        }
    }

    pub fn flushall(&mut self) -> OutputValue {
        for db in self.db.iter_mut() {
            db.flushdb();
        }
        OutputValue::Ok
    }

    pub fn client_list(&self) -> OutputValue {
        self.cons.list()
    }
}
