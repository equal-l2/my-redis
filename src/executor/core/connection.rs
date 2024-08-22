use std::collections::BTreeMap;

use crate::value::Value;

use num_bigint::BigUint;
use num_integer::Integer;
use smol::net::SocketAddr;

#[derive(Debug)]
pub struct ConnectionState {
    pub db: usize,
    pub addr: SocketAddr,
}

pub type ConnectionId = BigUint;

#[derive(Debug, Default)]
pub struct ConnectionStore {
    data: BTreeMap<ConnectionId, ConnectionState>,
    next_id: ConnectionId,
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
        let id = self.next_id.clone();
        self.next_id.inc();
        self.data.insert(id.clone(), ConnectionState::new(addr));
        Some(id)
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
