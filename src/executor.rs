use crate::value::Value;

struct Command {
    handler: &'static (dyn Fn(&mut ExecutorImpl, &mut HandleInfo, &[Value]) -> Value + 'static),
    arity_min: usize,
    arity_max: Option<usize>,
}

#[derive(Clone, Debug, Default)]
struct Map {
    data: std::collections::HashMap<Vec<u8>, Value>,
}

#[derive(Debug, Default)]
struct ExecutorImpl {
    db: Vec<Map>,
}

#[derive(Debug)]
pub struct Executor {
    ex: std::rc::Rc<std::cell::RefCell<ExecutorImpl>>,
}

struct HandleInfo {
    db: usize,
}

pub struct Handle {
    ex: Executor,
    info: HandleInfo,
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
}

thread_local! {
    static COMMANDS: std::cell::LazyCell<std::collections::HashMap<&'static str, Command>> = std::cell::LazyCell::new(|| {
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
            handler:&move |ex, info, input| ex.db[info.db].get(&input[0])
        });
        map.insert("set", Command{
            arity_min: 2,
            arity_max: Some(5),
            // TODO: support options
            handler:&move |ex, info, input| ex.db[info.db].set(&input[0], &input[1])
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
            handler: &move |ex, info, input| {
                match ex.validate_db_index_value(&input[0]) {
                    Ok(db_index) => {
                            info.db = db_index;
                            Value::Ok
                    },
                    Err(DbIndexValidationError::OutOfRange) => Value::Error(b"ERR DB index is out of range".to_vec()),
                    _ => Value::Error(b"ERR invalid DB index".to_vec()),
                }
            }
        });
        map.insert("flushdb", Command {
            arity_min: 0,
            arity_max: Some(0),
            handler: &move |ex, info, _| {
                // TODO: support async
                ex.db[info.db].data.clear();
                Value::Ok
            }
        });
        map.insert("swapdb", Command {
            arity_min: 2,
            arity_max: Some(2),
            handler: &move |ex, _, input| {
                let db_index_opt1 = ex.validate_db_index_value(&input[0]);
                let db_index_opt2 = ex.validate_db_index_value(&input[1]);
                match (db_index_opt1, db_index_opt2) {
                    (Ok(db_index1), Ok(db_index2)) => {
                        ex.db.swap(db_index1, db_index2);
                        Value::Ok
                    }
                    (Err(e), _) => {
                        match e {
                            DbIndexValidationError::OutOfRange => Value::Error(b"ERR first DB index is out of range".to_vec()),
                            _ => Value::Error(b"ERR invalid first DB index".to_vec()),
                        }
                    }
                    (_, Err(e)) => {
                        match e {
                            DbIndexValidationError::OutOfRange => Value::Error(b"ERR second DB index is out of range".to_vec()),
                            _ => Value::Error(b"ERR invalid second DB index".to_vec()),
                        }
                    }
                }
            }
        });
        map
    });
}

impl Map {
    fn get(&self, key: &Value) -> Value {
        if let Some(k) = key.to_bulkstr() {
            self.data.get(k).cloned().unwrap_or(Value::NullBulkString)
        } else {
            todo!()
        }
    }

    fn set(&mut self, key: &Value, value: &Value) -> Value {
        if let Some(k) = key.to_bulkstr() {
            self.data.insert(k.to_vec(), value.clone());
            Value::Ok
        } else {
            todo!()
        }
    }
}

enum DbIndexValidationError {
    InvalidSource,
    OutOfRange,
}

impl ExecutorImpl {
    fn new(db_count: usize) -> Self {
        ExecutorImpl {
            db: vec![Map::default(); db_count],
        }
    }

    fn validate_db_index_value(
        &self,
        db_index_value: &Value,
    ) -> Result<usize, DbIndexValidationError> {
        match db_index_value.to_usize() {
            Some(db_index) if db_index < self.db.len() => Ok(db_index),
            Some(_) => Err(DbIndexValidationError::OutOfRange),
            _ => Err(DbIndexValidationError::InvalidSource),
        }
    }

    fn execute(&mut self, arr: Value, handle_info: &mut HandleInfo) -> Vec<u8> {
        assert!(matches!(arr, Value::Array(_)));

        let items = match arr {
            Value::Array(items) => items,
            _ => unreachable!(),
        };

        println!("{:?}", items);

        let msg = match items[0].to_bulkstr() {
            Some(bs) => {
                // commands should be valid UTF-8
                let command_str = std::str::from_utf8(bs)
                    .expect("TODO: check UTF-8")
                    .to_lowercase();
                let result = COMMANDS.with(|key| {
                    key.get(command_str.as_str()).map(|command| {
                        if command.is_arity_correct(items.len() - 1) {
                            (command.handler)(self, handle_info, &items[1..])
                        } else {
                            Value::Error(
                                [b"ERR wrong number of arguments for command '", bs, b"'"].concat(),
                            )
                        }
                    })
                });

                if let Some(res) = result {
                    &res.to_bytes_vec()
                } else {
                    &[b"-ERR unknown command ", bs, b"\r\n"].concat()
                }
            }
            _ => b"-ERR invalid request\r\n".as_slice(),
        };
        msg.to_vec()
    }
}

impl Executor {
    pub fn new(db_count: usize) -> Self {
        Executor {
            ex: std::rc::Rc::new(std::cell::RefCell::new(ExecutorImpl::new(db_count))),
        }
    }

    fn execute(&mut self, arr: Value, handle_info: &mut HandleInfo) -> Vec<u8> {
        self.ex.borrow_mut().execute(arr, handle_info)
    }

    pub fn get_handle(&self) -> Handle {
        Handle {
            ex: self.clone(),
            info: HandleInfo::default(),
        }
    }
}

impl Clone for Executor {
    fn clone(&self) -> Self {
        Executor {
            ex: self.ex.clone(),
        }
    }
}

#[allow(clippy::derivable_impls)]
impl std::default::Default for HandleInfo {
    fn default() -> Self {
        Self { db: 0 }
    }
}

impl Handle {
    pub fn execute(&mut self, arr: Value) -> Vec<u8> {
        self.ex.execute(arr, &mut self.info)
    }
}
