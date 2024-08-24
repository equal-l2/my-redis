use std::cell::LazyCell;
use std::collections::HashMap;

use super::acl::AclCategory;
use super::ConnectionId;
use super::ExecutorImpl;
use crate::value::Value;

type HandlerType = dyn Fn(&mut ExecutorImpl, &ConnectionId, Vec<Value>) -> Value + 'static;

pub struct SimpleCommand {
    pub handler: &'static HandlerType,
    pub category: &'static [AclCategory],
    arity_min: usize,
    arity_max: Option<usize>,
}

pub struct ContainerCommand {
    pub handler: Option<&'static HandlerType>,
    pub category: &'static [AclCategory],
    subcommands: HashMap<&'static str, SimpleCommand>,
}

pub trait Command {
    fn is_arity_correct(&self, arity: usize) -> bool;
    fn execute(
        &self,
        name: &str,
        ex: &mut ExecutorImpl,
        id: &ConnectionId,
        input: Vec<Value>,
    ) -> Value;
}

impl Command for SimpleCommand {
    fn is_arity_correct(&self, arity: usize) -> bool {
        if arity < self.arity_min {
            false
        } else if let Some(max) = self.arity_max {
            arity <= max
        } else {
            true
        }
    }

    fn execute(
        &self,
        name: &str,
        ex: &mut ExecutorImpl,
        id: &ConnectionId,
        input: Vec<Value>,
    ) -> Value {
        if !self.is_arity_correct(input.len()) {
            return Value::Error(
                format!("ERR wrong number of arguments for command '{}'", name).into_bytes(),
            );
        }
        (self.handler)(ex, id, input)
    }
}

impl Command for ContainerCommand {
    fn is_arity_correct(&self, arity: usize) -> bool {
        if self.handler.is_none() {
            // arity == 0 is disallowed
            arity >= 1
        } else {
            true
        }
    }

    fn execute(
        &self,
        name: &str,
        ex: &mut ExecutorImpl,
        id: &ConnectionId,
        mut input: Vec<Value>,
    ) -> Value {
        if !self.is_arity_correct(input.len()) {
            return Value::Error(
                format!("ERR wrong number of arguments for command '{}'", name).into_bytes(),
            );
        }
        match (input.len(), self.handler) {
            (0, Some(handler)) => (handler)(ex, id, input),
            (0, None) => unreachable!(),
            _ => {
                let rest = input.drain(1..).collect::<Vec<_>>();
                let Some(sub) = get_first(input).into_string() else {
                    return Value::Error(
                        format!("ERR unknown subcommand for '{}'", name).into_bytes(),
                    );
                };
                let Some(cmd) = self.subcommands.get(sub.as_str()) else {
                    return Value::Error(
                        format!("ERR unknown subcommand for '{}'", name).into_bytes(),
                    );
                };

                cmd.execute(format!("{} {}", name, sub).as_str(), ex, id, rest)
            }
        }
    }
}

thread_local! {
    pub static SIMPLE_COMMANDS: LazyCell<HashMap<&'static str, SimpleCommand>> = LazyCell::new(initialise_simple_commands);
    pub static CONTAINER_COMMANDS: LazyCell<HashMap<&'static str, ContainerCommand>> = LazyCell::new(initialise_container_commands);

    static COMMANDS_BY_ACL_CATEGORY: LazyCell<HashMap<AclCategory, Value>> = LazyCell::new(populate_category_map);
}

fn populate_category_map() -> HashMap<AclCategory, Value> {
    let mut map: HashMap<AclCategory, Vec<Vec<u8>>> = HashMap::new();
    SIMPLE_COMMANDS.with(|commands| {
        for (name, SimpleCommand { category, .. }) in commands.iter() {
            for cat in category.iter() {
                map.entry(*cat).or_default().push(name.bytes().collect());
            }
        }
    });

    CONTAINER_COMMANDS.with(|commands| {
        for (
            name,
            ContainerCommand {
                handler,
                category,
                subcommands,
                ..
            },
        ) in commands.iter()
        {
            if handler.is_some() {
                for cat in category.iter() {
                    map.entry(*cat).or_default().push(name.bytes().collect());
                }
            }
            for (sub_name, SimpleCommand { category, .. }) in subcommands.iter() {
                for cat in category.iter() {
                    map.entry(*cat)
                        .or_default()
                        .push(format!("{}|{}", name, sub_name).into_bytes());
                }
            }
        }
    });

    map.into_iter()
        .map(|(k, mut v)| {
            v.sort_unstable();
            (
                k,
                Value::Array(v.into_iter().map(Value::BulkString).collect()),
            )
        })
        .collect()
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

fn initialise_simple_commands() -> HashMap<&'static str, SimpleCommand> {
    let mut map = HashMap::<&'static str, SimpleCommand>::new();
    map.insert(
        "ping",
        SimpleCommand {
            arity_min: 0,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, _, input| {
                input
                    .into_iter()
                    .next()
                    .and_then(|v| v.into_bulkstr())
                    .map(Value::BulkString)
                    .unwrap_or_else(|| Value::SimpleString(b"PONG".as_ref().to_vec()))
            },
        },
    );
    map.insert(
        "echo",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, _, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|msg| Value::BulkString(msg.to_owned()))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'echo'".to_vec()))
            },
        },
    );
    map.insert(
        "get",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|k| ex.get_db(id).get(&k))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'get'".to_vec()))
            },
        },
    );
    map.insert(
        "set",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(5),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Slow],
            // TODO: support options
            handler: &move |ex, id, input| {
                let mut args = input.into_iter();
                let Some(key) = args.next().unwrap().into_bulkstr() else {
                    return Value::Error(b"ERR wrong key type for 'set'".to_vec());
                };
                let Some(value) = args.next().unwrap().into_bulkstr() else {
                    return Value::Error(b"ERR wrong value type for 'set'".to_vec());
                };
                ex.get_db(id).set(&key, value)
            },
        },
    );

    map.insert(
        "select",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |ex, id, input| {
                get_first(input)
                    .to_usize()
                    .map(|db_index| ex.select(id, db_index))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'select'".to_vec()))
            },
        },
    );
    map.insert(
        "flushdb",
        SimpleCommand {
            arity_min: 0,
            arity_max: Some(0),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Slow,
                AclCategory::Dangerous,
            ],
            handler: &move |ex, id, _| {
                // TODO: support async
                ex.get_db(id).flushdb()
            },
        },
    );
    map.insert(
        "flushall",
        SimpleCommand {
            arity_min: 0,
            arity_max: Some(0),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Slow,
                AclCategory::Dangerous,
            ],
            handler: &move |ex, _, _| {
                // TODO: support async
                ex.flushall()
            },
        },
    );
    map.insert(
        "swapdb",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Fast,
                AclCategory::Dangerous,
            ],
            handler: &move |ex, _, input| {
                let (db1, db2) = get_first_two(input);
                let Some(db1) = db1.to_usize() else {
                    return Value::Error(b"ERR invalid first DB index".to_vec());
                };
                let Some(db2) = db2.to_usize() else {
                    return Value::Error(b"ERR invalid second DB index".to_vec());
                };
                ex.swap_db(db1, db2)
            },
        },
    );

    map.insert(
        "dbsize",
        SimpleCommand {
            arity_min: 0,
            arity_max: Some(0),
            category: &[AclCategory::Keyspace, AclCategory::Read, AclCategory::Fast],
            handler: &move |ex, id, _| Value::Integer(ex.get_db(id).len() as i64),
        },
    );
    map.insert(
        "exists",
        SimpleCommand {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Keyspace, AclCategory::Read, AclCategory::Fast],
            handler: &move |ex, id, input| {
                input
                    .into_iter()
                    .map(|v| v.into_bulkstr())
                    .collect::<Option<Vec<_>>>()
                    .map(|keys| ex.get_db(id).exists(keys))
                    .unwrap_or_else(|| {
                        Value::Error(b"ERR wrong argument type for 'exists'".to_vec())
                    })
            },
        },
    );
    map.insert(
        "append",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'append'".to_vec());
                };
                let Some(value) = value.into_bulkstr() else {
                    return Value::Error(b"ERR invalid value type for 'append'".to_vec());
                };
                ex.get_db(id).append(&key, value)
            },
        },
    );
    map.insert(
        "strlen",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|key| ex.get_db(id).strlen(&key))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'strlen'".to_vec()))
            },
        },
    );
    map.insert(
        "incr",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|key| ex.get_db(id).incr_by(&key, 1))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'incr'".to_vec()))
            },
        },
    );
    map.insert(
        "decr",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                get_first(input)
                    .into_bulkstr()
                    .map(|key| ex.get_db(id).decr_by(&key, 1))
                    .unwrap_or_else(|| Value::Error(b"ERR invalid argument for 'decr'".to_vec()))
            },
        },
    );
    map.insert(
        "incrby",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'incrby'".to_vec());
                };
                let Some(value) = value.to_i64() else {
                    return Value::Error(b"ERR invalid value type for 'incrby'".to_vec());
                };
                ex.get_db(id).incr_by(&key, value)
            },
        },
    );
    map.insert(
        "decrby",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'decrby'".to_vec());
                };
                let Some(value) = value.to_i64() else {
                    return Value::Error(b"ERR invalid value type for 'decrby'".to_vec());
                };
                ex.get_db(id).decr_by(&key, value)
            },
        },
    );
    map.insert(
        "incrbyfloat",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                let (key, value) = get_first_two(input);
                let Some(key) = key.into_bulkstr() else {
                    return Value::Error(b"ERR invalid key type for 'incrbyfloat'".to_vec());
                };
                let Some(value) = value.to_floating() else {
                    return Value::Error(b"ERR invalid value type for 'incrbyfloat'".to_vec());
                };
                ex.get_db(id).incr_by_float(&key, value)
            },
        },
    );
    map.insert(
        "del",
        SimpleCommand {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Keyspace, AclCategory::Write, AclCategory::Slow],
            handler: &move |ex, id, input| {
                input
                    .into_iter()
                    .map(|v| v.into_bulkstr())
                    .collect::<Option<Vec<_>>>()
                    .map(|keys| ex.get_db(id).del(keys))
                    .unwrap_or_else(|| Value::Error(b"ERR wrong argument type for 'del'".to_vec()))
            },
        },
    );
    map
}

fn initialise_container_commands() -> HashMap<&'static str, ContainerCommand> {
    let mut map = HashMap::new();
    map.insert(
        "acl",
        ContainerCommand {
            category: &[AclCategory::Slow],
            handler: None,
            subcommands: {
                let mut map = HashMap::new();
                map.insert(
                    "cat",
                    SimpleCommand {
                        arity_min: 0,
                        arity_max: Some(1),
                        category: &[AclCategory::Slow],
                        handler: &move |_, _, input| {
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
                    },
                );
                map
            },
        },
    );
    map.insert(
        "client",
        ContainerCommand {
            handler: None,
            category: &[AclCategory::Slow],
            subcommands: {
                let mut map = HashMap::new();
                map.insert(
                    "id",
                    SimpleCommand {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[AclCategory::Slow, AclCategory::Connection],
                        handler: &move |_, id, _| {
                            let id_least_digit = id.iter_u64_digits().next();
                            Value::Integer(match id_least_digit {
                                None => 0,
                                Some(i) if i <= i64::MAX as u64 => i as i64,
                                _ => i64::MAX,
                            })
                        },
                    },
                );
                map.insert(
                    "list",
                    SimpleCommand {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[
                            AclCategory::Admin,
                            AclCategory::Slow,
                            AclCategory::Dangerous,
                            AclCategory::Connection,
                        ],
                        handler: &move |ex, _, _| ex.client_list(),
                    },
                );
                map
            },
        },
    );
    map.insert(
        "command",
        ContainerCommand {
            category: &[AclCategory::Slow, AclCategory::Connection],
            handler: Some(&move |_, _, _| {
                Value::Error(b"ERR 'command' is not implemented yet".to_vec())
            }),
            subcommands: {
                let mut map = HashMap::new();
                map.insert(
                    "count",
                    SimpleCommand {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[AclCategory::Slow, AclCategory::Connection],
                        handler: &move |_, _, _| {
                            Value::Integer(SIMPLE_COMMANDS.with(|t| t.len() as i64))
                        },
                    },
                );
                // TODO: other subcommands for "command"
                map
            },
        },
    );
    map
}
