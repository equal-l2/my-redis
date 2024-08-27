use smol::net::SocketAddr;

mod core;

use core::{ConnectionId, ExecutorImpl};

pub type InputValue = Vec<u8>;

#[derive(Debug)]
pub struct Executor {
    ex: std::rc::Rc<std::cell::RefCell<ExecutorImpl>>,
}

pub struct Handle {
    ex: Executor,
    con_id: core::ConnectionId,
}

impl Executor {
    pub fn new(db_count: usize) -> Self {
        Executor {
            ex: std::rc::Rc::new(std::cell::RefCell::new(ExecutorImpl::new(db_count))),
        }
    }

    fn execute(&mut self, arr: Vec<InputValue>, con: ConnectionId) -> Vec<u8> {
        self.ex.borrow_mut().execute(arr, con)
    }

    pub fn connect(&self, addr: SocketAddr) -> Option<Handle> {
        self.ex.borrow_mut().connect(addr).map(|con_id| Handle {
            ex: self.clone(),
            con_id,
        })
    }

    pub fn disconnect(&mut self, con_id: ConnectionId) {
        self.ex.borrow_mut().disconnect(con_id);
    }
}

impl Clone for Executor {
    fn clone(&self) -> Self {
        Executor {
            ex: self.ex.clone(),
        }
    }
}

impl Handle {
    pub fn execute(&mut self, input: Vec<InputValue>) -> Vec<u8> {
        self.ex.execute(input, self.con_id.clone())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.ex.disconnect(self.con_id.clone());
    }
}
