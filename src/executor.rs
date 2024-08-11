use crate::value::Value;

pub struct Executor {
    data: std::collections::HashMap<Vec<u8>, Value>,
}

struct Command {
    handler: &'static (dyn Fn(&mut Executor, &[Value]) -> Value + 'static),
    arity_min: usize,
    arity_max: Option<usize>,
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
            handler: &move |_, input| {
                if let Some(msg) = input[0].as_bulkstr() {
                    Value::BulkString(msg.to_owned())
                } else {
                    Value::SimpleString("PONG".as_bytes().to_owned())
                }
            }
        });
        map.insert("get", Command{
            arity_min: 1,
            arity_max: Some(1),
            handler:&move |ex, input| ex.get(&input[0])
        });
        map.insert("set", Command{
            arity_min: 2,
            arity_max: Some(5),
            handler:&move |ex, input| ex.set(&input[0], &input[1])
        });
        map.insert("command", Command {
            arity_min: 0,
            arity_max: None,
            handler: &move |_, input| {
                match input.len() {
                    0 => Value::Error(b"ERR unknown command 'command'".to_vec()), // TODO: implement "command" command
                    1 => {
                        let sub = input[0].as_bulkstr();
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
        map
    });
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            data: std::collections::HashMap::new(),
        }
    }

    pub fn execute(&mut self, arr: Value, output: &mut impl std::io::Write) {
        assert!(matches!(arr, Value::Array(_)));

        let items = match arr {
            Value::Array(items) => items,
            _ => unreachable!(),
        };

        println!("{:?}", items);

        let msg = match items[0].as_bulkstr() {
            Some(bs) => {
                // commands should be valid UTF-8
                let command_str = std::str::from_utf8(bs)
                    .expect("TODO: check UTF-8")
                    .to_lowercase();
                let result = COMMANDS.with(|key| {
                    key.get(command_str.as_str()).map(|command| {
                        if command.is_arity_correct(items.len() - 1) {
                            (command.handler)(self, &items[1..])
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
        output.write_all(msg).unwrap();
    }

    fn get(&self, key: &Value) -> Value {
        if let Some(k) = key.as_bulkstr() {
            self.data.get(k).cloned().unwrap_or(Value::NullBulkString)
        } else {
            todo!()
        }
    }

    fn set(&mut self, key: &Value, value: &Value) -> Value {
        if let Some(k) = key.as_bulkstr() {
            self.data.insert(k.to_vec(), value.clone());
            Value::SimpleString(b"OK".to_vec())
        } else {
            todo!()
        }
    }
}
