use std::cell::RefCell;

use acl::AclCategory;
use command::ControllerCommand;
use smol::net::SocketAddr;

mod acl;
mod command;
mod connection;
pub mod database;
mod glob;

use crate::bstr::BStr;

use crate::interface::connection::ConnectionId;
use crate::interface::connection::IConnectionStore;
use crate::interface::database::map::MapAllCommands;
use crate::interface::database::IDatabase;
use crate::interface::database::IDatabaseWithInner;
use crate::interface::types::InputValue;
use crate::interface::types::OutputValue;
use crate::interface::IController;
use crate::interface::UseController;
use crate::interface::UseControllerWithDb;

use command::{Command, CommandStore};
use connection::ConnectionStore;
use database::Database;

#[derive(Debug)]
pub struct Controller<I: 'static + Default> {
    db: RefCell<Database<I>>,
    cons: ConnectionStore,
    commands: CommandStore<I>,
}

pub enum Interrupt {
    AclCat(Option<AclCategory>),
    ClientList,
    ClientId,
    CommandCount,
    CommandList(command::CommandListFilter),
    Select(usize),
    SwapDb(usize, usize),
    FlushAll,
}

impl<I: Default + MapAllCommands> Controller<I> {
    fn validate_db_index_value(&self, db_index_value: usize) -> Option<usize> {
        if db_index_value < self.db.borrow().len() {
            Some(db_index_value)
        } else {
            None
        }
    }

    fn acl_cat(&self, category: Option<AclCategory>) -> OutputValue {
        category
            .map(|cat| {
                self.commands
                    .commands_by_acl_category
                    .get(&cat)
                    .unwrap()
                    .clone()
            })
            .unwrap_or_else(AclCategory::array)
    }

    fn client_id(&self, con_id: &ConnectionId) -> OutputValue {
        let id_least_digit = con_id.iter_u64_digits().next();
        OutputValue::Integer(match id_least_digit {
            None => 0,
            Some(i) if i <= i64::MAX as u64 => i as i64,
            _ => i64::MAX,
        })
    }

    fn get_db_id(&self, id: &ConnectionId) -> usize {
        let state = self.cons.get_state(id);
        state.db
    }
    fn handle_interrupt(
        &mut self,
        res: Result<Interrupt, OutputValue>,
        con_id: &ConnectionId,
    ) -> OutputValue {
        match res {
            Err(e) => e,
            Ok(interrupt) => match interrupt {
                Interrupt::AclCat(cat) => self.acl_cat(cat),
                Interrupt::ClientList => self.client_list(),
                Interrupt::ClientId => self.client_id(con_id),
                Interrupt::CommandCount => self.commands.count(),
                Interrupt::CommandList(filter) => self.commands.list(filter),
                Interrupt::Select(db_index) => self.select(con_id, db_index),
                Interrupt::SwapDb(db1, db2) => self.swap_db(db1, db2),
                Interrupt::FlushAll => self.flushall(),
            },
        }
    }
}

impl<I: Default + MapAllCommands> IController for Controller<I> {
    fn new(db_count: usize) -> Self {
        Controller {
            db: RefCell::new(Database::new(db_count)),
            cons: ConnectionStore::default(),
            commands: CommandStore::default(),
        }
    }

    fn connect(&mut self, addr: SocketAddr) -> ConnectionId {
        self.cons.connect(addr)
    }

    fn disconnect(&mut self, con_id: ConnectionId) {
        self.cons.disconnect(&con_id);
    }

    fn execute(&mut self, mut input: Vec<InputValue>, con_id: ConnectionId) -> Vec<u8> {
        debug_assert!(self.cons.has(&con_id));

        let name_bs = input[0].clone();

        // commands should be valid UTF-8
        let Ok(name) = std::str::from_utf8(&name_bs).map(str::to_ascii_lowercase) else {
            return [b"ERR unknown command '", name_bs.as_slice(), b"'"]
                .concat()
                .to_redis_error();
        };

        if let Some(v) = self.commands.simple_commands.get(name.as_str()).map(|cmd| {
            let mut borrowed = self.db.borrow_mut();
            let db = borrowed.get_mut(self.get_db_id(&con_id));
            cmd.execute(name.as_str(), db, input.drain(1..).collect())
                .to_bytes_vec()
        }) {
            return v;
        }

        if let Some(v) = self
            .commands
            .container_commands
            .get(name.as_str())
            .map(|command| command.execute(name.as_str(), input.drain(1..).collect()))
        {
            return self.handle_interrupt(v, &con_id).to_bytes_vec();
        }

        if let Some(v) = self
            .commands
            .controller_commands
            .get(name.as_str())
            .map(|command| command.execute(name.as_str(), input.drain(1..).collect()))
        {
            return self.handle_interrupt(v, &con_id).to_bytes_vec();
        }

        return [b"ERR unknown command '", name_bs.as_slice(), b"'"]
            .concat()
            .to_redis_error();
    }
}

impl<I: Default + MapAllCommands> UseController for Controller<I> {
    fn client_list(&self) -> OutputValue {
        self.cons.list()
    }
    fn select(&mut self, id: &ConnectionId, arg: usize) -> OutputValue {
        match self.validate_db_index_value(arg) {
            Some(db_index) => {
                self.cons.set_db(id, db_index);
                OutputValue::Ok
            }
            None => OutputValue::Error(b"ERR DB index is out of range".to_vec()),
        }
    }
}

impl<I: Default + MapAllCommands> UseControllerWithDb for Controller<I> {
    fn swap_db(&mut self, db1: usize, db2: usize) -> OutputValue {
        let db_index_opt1 = self.validate_db_index_value(db1);
        let db_index_opt2 = self.validate_db_index_value(db2);
        match (db_index_opt1, db_index_opt2) {
            (Some(db_index1), Some(db_index2)) => {
                self.db.borrow_mut().swap(db_index1, db_index2);
                OutputValue::Ok
            }
            (None, _) => OutputValue::Error(b"ERR first DB index is out of range".to_vec()),
            (_, None) => OutputValue::Error(b"ERR second DB index is out of range".to_vec()),
        }
    }
    fn flushall(&mut self) -> OutputValue {
        self.db.borrow_mut().flushall()
    }
}
