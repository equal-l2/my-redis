use super::types::OutputValue;
use num_bigint::BigUint;
use smol::net::SocketAddr;

pub type ConnectionId = BigUint;

#[derive(Debug)]
pub struct ConnectionState {
    pub db: usize,
    pub addr: SocketAddr,
}

pub trait IConnectionStore {
    fn get_state(&self, id: &ConnectionId) -> &ConnectionState;
    fn get_state_mut(&mut self, id: &ConnectionId) -> &mut ConnectionState;
    fn connect(&mut self, addr: SocketAddr) -> ConnectionId;
    fn disconnect(&mut self, id: &ConnectionId);
    fn has(&self, id: &ConnectionId) -> bool;
    fn set_db(&mut self, id: &ConnectionId, db_index: usize);
    fn list(&self) -> OutputValue;
}
