use smol::net::SocketAddr;

use std::ops::Deref;

use crate::{
    implementation::{database::Map, Controller},
    interface::{types::InputValue, IController},
};

#[derive(Debug)]
pub struct ControllerWrapper(std::rc::Rc<std::cell::RefCell<Controller<Map>>>);

impl Deref for ControllerWrapper {
    type Target = std::rc::Rc<std::cell::RefCell<Controller<Map>>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for ControllerWrapper {
    fn clone(&self) -> Self {
        ControllerWrapper(self.0.clone())
    }
}

impl ControllerWrapper {
    pub fn new(db_count: usize) -> Self {
        ControllerWrapper(std::rc::Rc::new(std::cell::RefCell::new(Controller::new(
            db_count,
        ))))
    }

    pub fn connect(&self, addr: SocketAddr) -> Handle {
        let con_id = self.borrow_mut().connect(addr);
        Handle {
            ex: self.clone(),
            con_id,
        }
    }
}
pub struct Handle {
    ex: ControllerWrapper,
    con_id: crate::interface::connection::ConnectionId,
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
