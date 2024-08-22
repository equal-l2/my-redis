use super::ConnectionId;
use super::ExecutorImpl;
use crate::value::Value;

pub struct Command {
    pub handler: &'static (dyn Fn(&mut ExecutorImpl, ConnectionId, Vec<Value>) -> Value + 'static),
    arity_min: usize,
    arity_max: Option<usize>,
}

pub enum CommandError {
    ArityMismatch,
}

impl Command {
    fn is_arity_correct(&self, arity: usize) -> bool {
        if arity < self.arity_min {
            false
        } else if let Some(max) = self.arity_max {
            arity <= max
        } else {
            true
        }
    }

    pub fn execute(
        &self,
        ex: &mut ExecutorImpl,
        id: ConnectionId,
        input: Vec<Value>,
    ) -> Result<Value, CommandError> {
        if self.is_arity_correct(input.len()) {
            Ok((self.handler)(ex, id, input))
        } else {
            Err(CommandError::ArityMismatch)
        }
    }
}

thread_local! {
    pub static COMMANDS: std::cell::LazyCell<std::collections::HashMap<&'static str, Command>> = std::cell::LazyCell::new(|| {
        let mut map = std::collections::HashMap::<&'static str, Command>::new();
        map.insert("ping", Command {
            arity_min: 0,
            arity_max: Some(1),
            handler: &move |_, _, input| {
                if let Some(msg) = input.into_iter().next().and_then(|v| v.into_bulkstr()) {
                    Value::BulkString(msg)
                } else {
                    Value::SimpleString("PONG".as_bytes().to_owned())
                }
            }
        });
        map.insert("echo", Command {
            arity_min: 1,
            arity_max: Some(1),
            handler: &move |_, _, input| {
                if let Some(msg) = get_first(input).into_bulkstr() {
                    Value::BulkString(msg.to_owned())
                } else {
                    Value::Error(b"ERR invalid argument for 'echo'".to_vec())
                }
            }
        });
        map.insert("get", Command{
            arity_min: 1,
            arity_max: Some(1),
            handler:&move |ex, id, input| {
                if let Some(k) = get_first(input).into_bulkstr()  {
                    ex.get_db(id).get(&k)
                } else {
                    Value::Error(b"ERR invalid argument for 'get'".to_vec())
                }
            }
        });
        map.insert("set", Command{
            arity_min: 2,
            arity_max: Some(5),
            // TODO: support options
            handler:&move |ex, id, input| {
                let mut args = input.into_iter();
                let key = args.next().unwrap().into_bulkstr();
                let value = args.next().unwrap().into_bulkstr();
                if let (Some(k), Some(v)) = (key, value) {
                    ex.get_db(id).set(&k, v)
                } else {
                    Value::Error(b"ERR wrong argument type for 'set'".to_vec())
                }
            }
        });
        map.insert("command", Command {
            arity_min: 0,
            arity_max: None,
            handler: &move |_, _, input| {
                match input.len() {
                    0 => Value::Error(b"ERR unknown command 'command'".to_vec()), // TODO: implement "command" command
                    1 => {
                        let sub = get_first(input).into_bulkstr();
                        // TODO: implement other subcommands
                        match sub {
                            Some(sub) if sub == b"count" => {
                                Value::Integer(COMMANDS.with(|t| t.len() as i64))
                            }
                            _ => {
                                // other subcommands for "command"
                                Value::Error(b"ERR unknown subcommand or wrong number of arguments for 'command'".to_vec())
                            }
                        }
                    }
                    _ => Value::Error(b"ERR wrong number of arguments for 'command'".to_vec()), // TODO
                }
            }
        });
        map.insert("select", Command {
            arity_min: 1,
            arity_max: Some(1),
            handler: &move |ex, id, input| {
                if let Some(db_index) = get_first(input).to_usize() {
                    ex.select(id, db_index)
                } else {
                    Value::Error(b"ERR invalid argument for 'select'".to_vec())
                }
            }
        });
        map.insert("flushdb", Command {
            arity_min: 0,
            arity_max: Some(0),
            handler: &move |ex, id, _| {
                // TODO: support async
                ex.get_db(id).flushdb()
            }
        });
        map.insert("flushall", Command {
            arity_min: 0,
            arity_max: Some(0),
            handler: &move |ex, _, _| {
                // TODO: support async
                ex.flushall()
            }
        });
        map.insert("swapdb", Command {
            arity_min: 2,
            arity_max: Some(2),
            handler: &move |ex, _, input| {
                let (db1, db2) = get_first_two(input);
                let db1 = db1.to_usize();
                let db2 = db2.to_usize();
                match (db1, db2) {
                    (Some(db1), Some(db2)) => ex.swap_db(db1, db2),
                    (None, _) => Value::Error(b"ERR invalid first DB index".to_vec()),
                    (_, None) => Value::Error(b"ERR invalid second DB index".to_vec()),
                }
            }
        });
        map.insert("client", Command {
            arity_min: 1,
            arity_max: None,
            handler: &move |ex, id, input| {
                match input.len() {
                    0 => unreachable!(),
                    1 => {
                        let sub = get_first(input).into_bulkstr();
                        if let Some(s) = sub {
                            match s.as_slice() {
                                b"id" => {
                                    let id_least_digit = id.iter_u64_digits().next().unwrap();
                                    Value::Integer(if id_least_digit > i64::MAX as u64 {
                                        i64::MAX
                                    } else {
                                        id_least_digit as i64
                                    })
                                }
                                b"list" => {
                                // TODO: support options
                                    ex.client_list()
                                }
                                _ => {
                                    // other subcommands for "client"
                                    Value::Error(b"ERR unknown subcommand or wrong number of arguments for 'command'".to_vec())
                                }
                            }
                        } else {
                            Value::Error(b"ERR invalid argument for 'client'".to_vec())
                        }
                    }
                    _ => Value::Error(b"ERR wrong number of arguments for 'command'".to_vec()), // TODO
                }
            }
        });
        map.insert("dbsize", Command {
            arity_min: 0,
            arity_max: Some(0),
            handler: &move |ex, id, _| {
                Value::Integer(ex.get_db(id).len() as i64)
            }
        });
        map.insert("exists", Command {
            arity_min: 1,
            arity_max: None,
            handler: &move |ex, id, input| {
                let input_validated = input.into_iter().map(|v| v.into_bulkstr()).collect::<Option<Vec<_>>>();
                if let Some(keys) = input_validated {
                    ex.get_db(id).exists(keys)
                } else {
                    Value::Error(b"ERR wrong argument type for 'exists'".to_vec())
                }
            }
        });
        map.insert("append", Command {
            arity_min: 2,
            arity_max: Some(2),
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let key = key.into_bulkstr();
                let value = value.into_bulkstr();
                if let (Some(k), Some(v)) = (key, value) {
                    ex.get_db(id).append(&k, v)
                } else {
                    Value::Error(b"ERR invalid argument for 'append'".to_vec())
                }
            }
        });
        map.insert("strlen", Command {
            arity_min:1,
            arity_max: Some(1),
            handler: &move |ex, id, input| {
                let key = get_first(input).into_bulkstr();
                if let Some(k) = key {
                    ex.get_db(id).strlen(&k)
                } else {
                    Value::Error(b"ERR invalid argument for 'strlen'".to_vec())
                }
            }
        });
        map.insert("incr", Command {
            arity_min: 1,
            arity_max: Some(1),
            handler: &move |ex, id, input| {
                let key = get_first(input).into_bulkstr();
                if let Some(k) = key {
                    ex.get_db(id).incr_by(&k, 1)
                } else {
                    Value::Error(b"ERR invalid argument for 'incr'".to_vec())
                }
            }
        });
        map.insert("decr", Command {
            arity_min: 1,
            arity_max: Some(1),
            handler: &move |ex, id, input| {
                let key = get_first(input).into_bulkstr();
                if let Some(k) = key {
                    ex.get_db(id).decr_by(&k, 1)
                } else {
                    Value::Error(b"ERR invalid argument for 'decr'".to_vec())
                }
            }
        });
        map.insert("incrby", Command {
            arity_min: 2,
            arity_max: Some(2),
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let key = key.into_bulkstr();
                let value = value.to_i64();
                if let (Some(k), Some(v)) = (key, value) {
                    ex.get_db(id).incr_by(&k, v)
                } else {
                    Value::Error(b"ERR invalid argument for 'incrby'".to_vec())
                }
            }
        });
        map.insert("decrby", Command {
            arity_min: 2,
            arity_max: Some(2),
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let key = key.into_bulkstr();
                let value = value.to_i64();
                if let (Some(k), Some(v)) = (key, value) {
                    ex.get_db(id).decr_by(&k, v)
                } else {
                    Value::Error(b"ERR invalid argument for 'decrby'".to_vec())
                }
            }
        });
        map.insert("incrbyfloat", Command {
            arity_min: 2,
            arity_max: Some(2),
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let key = key.into_bulkstr();
                let value = value.to_floating();
                if let (Some(k), Some(v)) = (key, value) {
                    ex.get_db(id).incr_by_float(&k, v)
                } else {
                    Value::Error(b"ERR invalid argument for 'incrby'".to_vec())
                }
            }
        });
        map.insert("del", Command {
            arity_min: 1,
            arity_max: None,
            handler: &move |ex, id, input| {
                let input_validated = input.into_iter().map(|v| v.into_bulkstr()).collect::<Option<Vec<_>>>();
                if let Some(keys) = input_validated {
                    ex.get_db(id).del(keys)
                } else {
                    Value::Error(b"ERR wrong argument type for 'del'".to_vec())
                }
            }
        });
        map
    });
}

fn get_first(args: Vec<Value>) -> Value {
    let mut args = args.into_iter();
    args.next().unwrap()
}

fn get_first_two(args: Vec<Value>) -> (Value, Value) {
    let mut args = args.into_iter();
    let first = args.next().unwrap();
    let second = args.next().unwrap();
    (first, second)
}
