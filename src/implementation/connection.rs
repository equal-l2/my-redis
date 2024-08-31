use num_integer::Integer;
use smol::net::SocketAddr;
use std::collections::BTreeMap;

use crate::interface::connection::{ConnectionId, ConnectionState, IConnectionStore};
use crate::interface::types::OutputValue;

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

impl IConnectionStore for ConnectionStore {
    fn get_state(&self, id: &ConnectionId) -> &ConnectionState {
        self.data.get(id).unwrap()
    }

    fn get_state_mut(&mut self, id: &ConnectionId) -> &mut ConnectionState {
        self.data.get_mut(id).unwrap()
    }

    fn connect(&mut self, addr: SocketAddr) -> ConnectionId {
        let id = self.next_id.clone();
        self.next_id.inc();
        self.data.insert(id.clone(), ConnectionState::new(addr));
        id
    }

    fn disconnect(&mut self, id: &ConnectionId) {
        self.data.remove(id);
    }

    fn has(&self, id: &ConnectionId) -> bool {
        self.data.contains_key(id)
    }

    fn set_db(&mut self, id: &ConnectionId, db_index: usize) {
        self.get_state_mut(id).db = db_index;
    }

    fn list(&self) -> OutputValue {
        OutputValue::BulkString(
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
