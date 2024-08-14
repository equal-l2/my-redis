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
                if let Some(msg) = input.first().and_then(|v| v.to_bulkstr()) {
                    Value::BulkString(msg.to_owned())
                } else {
                    Value::SimpleString("PONG".as_bytes().to_owned())
                }
            }
        });
        map.insert("get", Command{
            arity_min: 1,
            arity_max: Some(1),
            handler:&move |ex, id, input| ex.get_db(id).get(&input[0])
        });
        map.insert("set", Command{
            arity_min: 2,
            arity_max: Some(5),
            // TODO: support options
            handler:&move |ex, id, input| ex.get_db(id).set(&input[0], &input[1])
        });
        map.insert("command", Command {
            arity_min: 0,
            arity_max: None,
            handler: &move |_, _, input| {
                match input.len() {
                    0 => Value::Error(b"ERR unknown command 'command'".to_vec()), // TODO: implement "command" command
                    1 => {
                        let sub = input[0].to_bulkstr();
                        // TODO: implement other subcommands
                        match sub {
                            Some(sub) if sub == b"count" => {
                                Value::Integer(COMMANDS.with(|t| t.len() as u64))
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
                let mut args = input.into_iter();
                ex.select(id, args.next().unwrap())
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
        map.insert("swapdb", Command {
            arity_min: 2,
            arity_max: Some(2),
            handler: &move |ex, _, input| {
                let mut args = input.into_iter();
                ex.swap_db(args.next().unwrap(), args.next().unwrap())
            }
        });
        map.insert("client", Command {
            arity_min: 1,
            arity_max: None,
            handler: &move |ex, _, input| {
            match input.len() {
                0 => unreachable!(),
                1 => match input[0].to_bulkstr() {
                    Some(sub) if sub == b"list" => {
                        // TODO: support options
                        ex.client_list()
                    }
                    _ => {
                                // other subcommands for "command"
                                Value::Error(b"ERR unknown subcommand or wrong number of arguments for 'command'".to_vec())
                    }
                }

                _ => Value::Error(b"ERR wrong number of arguments for 'command'".to_vec()), // TODO
            }
        }});
        map
    });
}
