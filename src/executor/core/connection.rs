use std::collections::BTreeMap;

use crate::value::Value;

use super::id::ConnectionId;
use super::id::ConnectionIdGenerator;
use smol::net::SocketAddr;

#[derive(Debug)]
pub struct ConnectionState {
    pub db: usize,
    pub addr: SocketAddr,
}

#[derive(Debug, Default)]
pub struct ConnectionStore {
    data: BTreeMap<ConnectionId, ConnectionState>,
    id_gen: ConnectionIdGenerator,
}

impl ConnectionState {
    fn new(addr: SocketAddr) -> Self {
        Self { db: 0, addr }
    }
}

impl ConnectionStore {
    pub fn state(&self, id: &ConnectionId) -> &ConnectionState {
        self.data.get(id).unwrap()
    }

    pub fn state_mut(&mut self, id: &ConnectionId) -> &mut ConnectionState {
        self.data.get_mut(id).unwrap()
    }

    pub fn connect(&mut self, addr: SocketAddr) -> Option<ConnectionId> {
        let first = self.id_gen.peek_id().minus_one();
        loop {
            let con_id = self.id_gen.get_id();
            if !self.data.contains_key(&con_id) {
                self.data.insert(con_id.clone(), ConnectionState::new(addr));
                return Some(con_id);
            }
            if con_id == first {
                // all id is used, try again
                return None;
            }
        }
    }

    pub fn disconnect(&mut self, id: &ConnectionId) {
        self.data.remove(id);
    }

    pub fn has(&self, id: &ConnectionId) -> bool {
        self.data.contains_key(id)
    }

    pub fn set_db(&mut self, id: ConnectionId, db_index: usize) {
        self.state_mut(&id).db = db_index;
    }
    pub fn list(&self) -> Value {
        Value::BulkString(
            self.data
                .iter()
                .flat_map(|(id, state)| {
                    let mut v = format!("id={} addr={}", id, state.addr).into_bytes();
                    v.push(b'\n');
                    v
                })
                .collect(),
        )
    }
}
