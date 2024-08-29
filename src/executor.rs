use smol::net::SocketAddr;

mod acl;
mod command;
mod connection;
mod core;
mod database;
mod glob;
mod types;

use core::Executor;
use std::ops::Deref;

pub use types::InputValue;

#[derive(Debug)]
pub struct ExecutorWrapper(std::rc::Rc<std::cell::RefCell<Executor>>);

impl Deref for ExecutorWrapper {
    type Target = std::rc::Rc<std::cell::RefCell<Executor>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for ExecutorWrapper {
    fn clone(&self) -> Self {
        ExecutorWrapper(self.0.clone())
    }
}

impl ExecutorWrapper {
    pub fn new(db_count: usize) -> Self {
        ExecutorWrapper(std::rc::Rc::new(std::cell::RefCell::new(Executor::new(
            db_count,
        ))))
    }

    pub fn connect(&self, addr: SocketAddr) -> Option<Handle> {
        self.borrow_mut().connect(addr).map(|con_id| Handle {
            ex: self.clone(),
            con_id,
        })
    }
}
pub struct Handle {
    ex: ExecutorWrapper,
    con_id: connection::ConnectionId,
}

impl Handle {
    pub fn execute(&self, input: Vec<InputValue>) -> Vec<u8> {
        self.ex.borrow_mut().execute(input, self.con_id.clone())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.ex.borrow_mut().disconnect(self.con_id.clone());
    }
}
