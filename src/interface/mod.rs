use smol::net::SocketAddr;

pub mod connection;
pub mod database;
pub mod types;

use connection::ConnectionId;
use types::{InputValue, OutputValue};

// External interface
pub trait IController {
    fn new(db_count: usize) -> Self;
    fn connect(&mut self, addr: SocketAddr) -> ConnectionId;
    fn disconnect(&mut self, con_id: ConnectionId);
    fn execute(&mut self, input: Vec<InputValue>, con_id: ConnectionId) -> Vec<u8>;
}

// Internal interface
pub trait UseController {
    fn client_list(&self) -> OutputValue;
    fn select(&mut self, id: &ConnectionId, arg: usize) -> OutputValue;
}

pub trait UseControllerWithDb {
    fn swap_db(&mut self, db1: usize, db2: usize) -> OutputValue;
    fn flushall(&mut self) -> OutputValue;
}
