use std::cell::LazyCell;
use std::collections::HashMap;

use super::acl::AclCategory;
use super::ConnectionId;
use super::ExecutorImpl;
use crate::value::Value;

pub struct Command {
    pub handler:
        &'static (dyn Fn(&Command, &mut ExecutorImpl, ConnectionId, Vec<Value>) -> Value + 'static),
    pub category: &'static [AclCategory],
    arity_min: usize,
    arity_max: Option<usize>,
    subcommands: Option<HashMap<&'static str, Command>>,
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
    ) -> Option<Value> {
        self.is_arity_correct(input.len())
            .then(|| (self.handler)(self, ex, id, input))
    }
}

thread_local! {
    pub static COMMANDS: LazyCell<HashMap<&'static str, Command>> = LazyCell::new(initialize_commands);

    static COMMANDS_BY_ACL_CATEGORY: LazyCell<HashMap<AclCategory, Value>> = LazyCell::new(|| {
        let mut map: HashMap<AclCategory, Vec<Vec<u8>>> = HashMap::new();
        COMMANDS.with(|commands| {
            for (name, Command {category, subcommands, .. }) in commands.iter() {
                for cat in category.iter() {
                    map.entry(*cat).or_default().push(name.bytes().collect());
                }
                if let Some(subs) = subcommands {
                    for (sub_name, Command { category, ..}) in subs.iter() {
                        for cat in category.iter() {
                            map.entry(*cat).or_default().push(format!("{}|{}", name, sub_name).into_bytes());
                        }
                    }
                }
            }
        });
        map.into_iter().map(|(k, mut v)| {
            v.sort_unstable();
            (k, Value::Array(v.into_iter().map(Value::BulkString).collect()))
        }).collect()
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

fn initialize_commands() -> HashMap<&'static str, Command> {
    let mut map = HashMap::<&'static str, Command>::new();
    map.insert(
        "acl",
        Command {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Slow],
            handler: &move |this, ex, id, mut input| match input.len() {
                0 => unreachable!(),
                _ => {
                    let rest = input.drain(1..).collect::<Vec<_>>();
                    let Some(sub) = get_first(input).into_string() else {
                        return Value::Error(b"ERR invalid arguments for 'acl'".to_vec());
                    };

                    let Some(cmd) = this.subcommands.as_ref().unwrap().get(sub.as_str()) else {
                        return Value::Error(b"ERR unknown subcommand for 'acl'".to_vec());
                    };

                    if let Some(v) = cmd.execute(ex, id, rest) {
                        v
                    } else {
                        Value::Error(
                            [
                                b"ERR wrong number of arguments for command 'acl ".as_slice(),
                                sub.as_bytes(),
                                b"'",
                            ]
                            .concat(),
                        )
                    }
                }
            },
            subcommands: Some({
                let mut map = HashMap::new();
                map.insert(
                    "cat",
                    Command {
                        arity_min: 0,
                        arity_max: Some(1),
                        category: &[AclCategory::Slow],
                        handler: &move |_, _, _, input| {
                            input
                                .into_iter()
                                .next()
                                .map(|s| {
                                    let Some(category) =
                                        s.into_string().and_then(|s| s.parse().ok())
                                    else {
                                        return Value::Error(
                                            b"ERR unknown category for 'acl cat'".to_vec(),
                                        );
                                    };

                                    COMMANDS_BY_ACL_CATEGORY
                                        .with(|map| map.get(&category).unwrap().clone())
                                })
                                .unwrap_or_else(AclCategory::array)
                        },
                        subcommands: None,
                    },
                );
                map
            }),
        },
    );
    map.insert(
        "ping",
        Command {
            arity_min: 0,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, _, _, input| {
                input
                    .into_iter()
                    .next()
                    .and_then(|v| v.into_bulkstr())
                    .map(Value::BulkString)
                    .unwrap_or_else(|| Value::SimpleString(b"PONG".as_ref().to_vec()))
            },
            subcommands: None,
        },
    );
    map.insert(
        "echo",
        Command {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, _, _, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|msg| Value::BulkString(msg.to_owned()))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'echo'".to_vec()))
            },
            subcommands: None,
        },
    );
    map.insert(
        "get",
        Command {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|k| ex.get_db(id).get(&k))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'get'".to_vec()))
            },
            subcommands: None,
        },
    );
    map.insert(
        "set",
        Command {
            arity_min: 2,
            arity_max: Some(5),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Slow],
            // TODO: support options
            handler: &move |_, ex, id, input| {
                let mut args = input.into_iter();
                let Some(key) = args.next().unwrap().into_bulkstr() else {
                    return Value::Error(b"ERR wrong key type for 'set'".to_vec());
                };
                let Some(value) = args.next().unwrap().into_bulkstr() else {
                    return Value::Error(b"ERR wrong value type for 'set'".to_vec());
                };
                ex.get_db(id).set(&key, value)
            },
            subcommands: None,
        },
    );
    map.insert("command", Command {
            arity_min: 0,
            arity_max: None,
            category: &[AclCategory::Slow, AclCategory::Connection],
            handler: &move |this, ex, id, mut input| {
                match input.len() {
                    0 => Value::Error(b"ERR 'command' is not implemented yet".to_vec()), // TODO
                    _ => {
                        let rest = input.drain(1..).collect::<Vec<_>>();
                        let Some(sub) = get_first(input).into_string() else {
                            return Value::Error(b"ERR invalid arguments for 'command'".to_vec())
                        };

                        let Some(cmd) = this.subcommands.as_ref().unwrap().get(sub.as_str()) else {
                            return Value::Error(b"ERR unknown subcommand or wrong number of arguments for 'command'".to_vec())
                        };

                        if let Some(v) = cmd.execute(ex, id, rest) {
                            v
                        } else {
                            Value::Error([ b"ERR wrong number of arguments for command 'command ".as_slice(), sub.as_bytes(), b"'"].concat())
                        }
                    }
                }
            },
            subcommands: Some({
                let mut map = HashMap::new();
                map.insert("count", Command {
                    arity_min: 0,
                    arity_max: Some(1),
                    category: &[AclCategory::Slow, AclCategory::Connection],
                    handler: &move |_, _, _, _| {
                        Value::Integer(COMMANDS.with(|t| t.len() as i64))
                    },
                    subcommands: None
                });
                // TODO: other subcommands for "command"
                map
            })
        });
    map.insert(
        "select",
        Command {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, ex, id, input| {
                get_first(input)
                    .to_usize()
                    .map(|db_index| ex.select(id, db_index))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'select'".to_vec()))
            },
            subcommands: None,
        },
    );
    map.insert(
        "flushdb",
        Command {
            arity_min: 0,
            arity_max: Some(0),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Slow,
                AclCategory::Dangerous,
            ],
            handler: &move |_, ex, id, _| {
                // TODO: support async
                ex.get_db(id).flushdb()
            },
            subcommands: None,
        },
    );
    map.insert(
        "flushall",
        Command {
            arity_min: 0,
            arity_max: Some(0),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Slow,
                AclCategory::Dangerous,
            ],
            handler: &move |_, ex, _, _| {
                // TODO: support async
                ex.flushall()
            },
            subcommands: None,
        },
    );
    map.insert(
        "swapdb",
        Command {
            arity_min: 2,
            arity_max: Some(2),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Fast,
                AclCategory::Dangerous,
            ],
            handler: &move |_, ex, _, input| {
                let (db1, db2) = get_first_two(input);
                let Some(db1) = db1.to_usize() else {
                    return Value::Error(b"ERR invalid first DB index".to_vec());
                };
                let Some(db2) = db2.to_usize() else {
                    return Value::Error(b"ERR invalid second DB index".to_vec());
                };
                ex.swap_db(db1, db2)
            },
            subcommands: None,
        },
    );
    map.insert(
        "client",
        Command {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Slow],
            handler: &move |this, ex, id, mut input| match input.len() {
                0 => unreachable!(),
                _ => {
                    let rest = input.drain(1..).collect::<Vec<_>>();
                    let Some(sub) = get_first(input).into_string() else {
                        return Value::Error(b"ERR invalid arguments for 'client'".to_vec());
                    };

                    let Some(cmd) = this.subcommands.as_ref().unwrap().get(sub.as_str()) else {
                        return Value::Error(b"ERR unknown subcommand for 'client'".to_vec());
                    };

                    if let Some(v) = cmd.execute(ex, id, rest) {
                        v
                    } else {
                        Value::Error(
                            [
                                b"ERR wrong number of arguments for command 'client ".as_slice(),
                                sub.as_bytes(),
                                b"'",
                            ]
                            .concat(),
                        )
                    }
                }
            },
            subcommands: Some({
                let mut map = HashMap::new();
                map.insert(
                    "id",
                    Command {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[AclCategory::Slow, AclCategory::Connection],
                        handler: &move |_, _, id, _| {
                            let id_least_digit = id.iter_u64_digits().next().unwrap();
                            Value::Integer(if id_least_digit > i64::MAX as u64 {
                                i64::MAX
                            } else {
                                id_least_digit as i64
                            })
                        },
                        subcommands: None,
                    },
                );
                map.insert(
                    "list",
                    Command {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[
                            AclCategory::Admin,
                            AclCategory::Slow,
                            AclCategory::Dangerous,
                            AclCategory::Connection,
                        ],
                        handler: &move |_, ex, _, _| ex.client_list(),
                        subcommands: None,
                    },
                );
                map
            }),
        },
    );
    map.insert(
        "dbsize",
        Command {
            arity_min: 0,
            arity_max: Some(0),
            category: &[AclCategory::Keyspace, AclCategory::Read, AclCategory::Fast],
            handler: &move |_, ex, id, _| Value::Integer(ex.get_db(id).len() as i64),
            subcommands: None,
        },
    );
    map.insert(
        "exists",
        Command {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Keyspace, AclCategory::Read, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                input
                    .into_iter()
                    .map(|v| v.into_bulkstr())
                    .collect::<Option<Vec<_>>>()
                    .map(|keys| ex.get_db(id).exists(keys))
                    .unwrap_or_else(|| {
                        Value::Error(b"ERR wrong argument type for 'exists'".to_vec())
                    })
            },
            subcommands: None,
        },
    );
    map.insert(
        "append",
        Command {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'append'".to_vec());
                };
                let Some(value) = value.into_bulkstr() else {
                    return Value::Error(b"ERR invalid value type for 'append'".to_vec());
                };
                ex.get_db(id).append(&key, value)
            },
            subcommands: None,
        },
    );
    map.insert(
        "strlen",
        Command {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|key| ex.get_db(id).strlen(&key))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'strlen'".to_vec()))
            },
            subcommands: None,
        },
    );
    map.insert(
        "incr",
        Command {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|key| ex.get_db(id).incr_by(&key, 1))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'incr'".to_vec()))
            },
            subcommands: None,
        },
    );
    map.insert(
        "decr",
        Command {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|key| ex.get_db(id).decr_by(&key, 1))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'decr'".to_vec()))
            },
            subcommands: None,
        },
    );
    map.insert(
        "incrby",
        Command {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'incrby'".to_vec());
                };
                let Some(value) = value.to_i64() else {
                    return Value::Error(b"ERR invalid value type for 'incrby'".to_vec());
                };
                ex.get_db(id).incr_by(&key, value)
            },
            subcommands: None,
        },
    );
    map.insert(
        "decrby",
        Command {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'decrby'".to_vec());
                };
                let Some(value) = value.to_i64() else {
                    return Value::Error(b"ERR invalid value type for 'decrby'".to_vec());
                };
                ex.get_db(id).decr_by(&key, value)
            },
            subcommands: None,
        },
    );
    map.insert(
        "incrbyfloat",
        Command {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |_, ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'incrbyfloat'".to_vec());
                };
                let Some(value) = value.to_floating() else {
                    return Value::Error(b"ERR invalid value type for 'incrbyfloat'".to_vec());
                };
                ex.get_db(id).incr_by_float(&key, value)
            },
            subcommands: None,
        },
    );
    map.insert(
        "del",
        Command {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Keyspace, AclCategory::Write, AclCategory::Slow],
            handler: &move |_, ex, id, input| {
                input
                    .into_iter()
                    .map(|v| v.into_bulkstr())
                    .collect::<Option<Vec<_>>>()
                    .map(|keys| ex.get_db(id).del(keys))
                    .unwrap_or_else(|| Value::Error(b"ERR wrong argument type for 'del'".to_vec()))
            },
            subcommands: None,
        },
    );
    map
}
