use std::cell::LazyCell;
use std::collections::HashMap;

use super::acl::AclCategory;
use super::ConnectionId;
use super::ExecutorImpl;
use super::InputValue;
use crate::bstr::BStr;
use crate::output_value::OutputValue;

type HandlerType =
    dyn Fn(&mut ExecutorImpl, &ConnectionId, Vec<InputValue>) -> OutputValue + 'static;

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
        input: Vec<InputValue>,
    ) -> OutputValue;
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
        input: Vec<Vec<u8>>,
    ) -> OutputValue {
        if !self.is_arity_correct(input.len()) {
            return OutputValue::Error(
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
        mut input: Vec<InputValue>,
    ) -> OutputValue {
        if !self.is_arity_correct(input.len()) {
            return OutputValue::Error(
                format!("ERR wrong number of arguments for command '{}'", name).into_bytes(),
            );
        }
        match (input.len(), self.handler) {
            (0, Some(handler)) => (handler)(ex, id, input),
            (0, None) => unreachable!(),
            _ => {
                let rest = input.drain(1..).collect::<Vec<_>>();
                let sub_bytes = get_first(input);
                let Some(sub) = sub_bytes.to_str() else {
                    return OutputValue::Error(
                        format!("ERR unknown subcommand for '{}'", name).into_bytes(),
                    );
                };
                let Some(cmd) = self.subcommands.get(sub) else {
                    return OutputValue::Error(
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

    static COMMANDS_BY_ACL_CATEGORY: LazyCell<HashMap<AclCategory, OutputValue>> = LazyCell::new(populate_category_map);
}

fn populate_category_map() -> HashMap<AclCategory, OutputValue> {
    let mut map: HashMap<AclCategory, Vec<Vec<u8>>> = HashMap::new();
    SIMPLE_COMMANDS.with(|commands| {
        for (name, SimpleCommand { category, .. }) in commands.iter() {
            for cat in category.iter() {
                map.entry(*cat).or_default().push(name.as_bytes().to_vec());
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
                    map.entry(*cat).or_default().push(name.as_bytes().to_vec());
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
                OutputValue::Array(v.into_iter().map(OutputValue::BulkString).collect()),
            )
        })
        .collect()
}

fn get_first<T>(args: Vec<T>) -> T {
    let mut args = args.into_iter();
    args.next().unwrap()
}

fn get_first_two<T>(args: Vec<T>) -> (T, T) {
    let mut args = args.into_iter();
    (args.next().unwrap(), args.next().unwrap())
}

fn get_first_three<T>(args: Vec<T>) -> (T, T, T) {
    let mut args = args.into_iter();
    (
        args.next().unwrap(),
        args.next().unwrap(),
        args.next().unwrap(),
    )
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
                    .map(OutputValue::BulkString)
                    .unwrap_or_else(|| OutputValue::SimpleString(b"PONG".as_ref().to_vec()))
            },
        },
    );
    map.insert(
        "echo",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, _, input| OutputValue::BulkString(get_first(input)),
        },
    );
    map.insert(
        "get",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |ex, id, input| {
                let key = get_first(input);
                ex.get_db(id).get(key)
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
                let (key, value) = get_first_two(input);
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
                    .parse_into()
                    .map(|db_index| ex.select(id, db_index))
                    .unwrap_or_else(|| {
                        OutputValue::Error(b"ERR invalid argument for 'select'".to_vec())
                    })
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
                let Some(db1) = db1.parse_into() else {
                    return OutputValue::Error(b"ERR invalid first DB index".to_vec());
                };
                let Some(db2) = db2.parse_into() else {
                    return OutputValue::Error(b"ERR invalid second DB index".to_vec());
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
            handler: &move |ex, id, _| OutputValue::Integer(ex.get_db(id).len() as i64),
        },
    );
    map.insert(
        "exists",
        SimpleCommand {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Keyspace, AclCategory::Read, AclCategory::Fast],
            handler: &move |ex, id, input| ex.get_db(id).exists(input),
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
                let key = get_first(input);
                ex.get_db(id).strlen(key)
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
                let key = get_first(input);
                ex.get_db(id).incr_by(key, 1)
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
                let key = get_first(input);
                ex.get_db(id).decr_by(&key, 1)
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
                let Some(value) = value.parse_into() else {
                    return OutputValue::Error(b"ERR value is not an integer".to_vec());
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
                let Some(value) = value.parse_into() else {
                    return OutputValue::Error(b"ERR value is not an integer".to_vec());
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
                let Some(value) = value.parse_into() else {
                    return OutputValue::Error(b"ERR value is not an floating number".to_vec());
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
            handler: &move |ex, id, input| ex.get_db(id).del(input),
        },
    );
    map.insert(
        "keys",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Read,
                AclCategory::Slow,
                AclCategory::Dangerous,
            ],
            handler: &move |ex, id, input| {
                let pattern = get_first(input);
                ex.get_db(id).keys(&pattern)
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
                                    let Some(category) = s.parse_into() else {
                                        return OutputValue::Error(
                                            b"ERR unknown ACL category for 'acl cat'".to_vec(),
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
                            OutputValue::Integer(match id_least_digit {
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
                OutputValue::Error(b"ERR 'command' is not implemented yet".to_vec())
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
                            OutputValue::Integer(SIMPLE_COMMANDS.with(|t| t.len() as i64))
                        },
                    },
                );
                map.insert(
                    "list",
                    SimpleCommand {
                        arity_min: 0,
                        arity_max: Some(3),
                        category: &[AclCategory::Slow, AclCategory::Connection],
                        handler: &move |_, _, input| match input.len() {
                            0 => {
                                let mut names: Vec<_> = SIMPLE_COMMANDS.with(|sc| {
                                    CONTAINER_COMMANDS
                                        .with(|cc| sc.keys().chain(cc.keys()).cloned().collect())
                                });
                                names.sort_unstable();
                                OutputValue::Array(
                                    names
                                        .into_iter()
                                        .map(|s| OutputValue::BulkString(s.as_bytes().to_vec()))
                                        .collect(),
                                )
                            }
                            3 => {
                                let (filterby, filter_category, filter_pattern) =
                                    get_first_three(input);
                                if let Some("filterby") = filterby.to_lower_string().as_deref() {
                                    // OK
                                } else {
                                    return OutputValue::Error(
                                        b"ERR invalid argument for 'command list'".to_vec(),
                                    );
                                }

                                let Some(filter_category) = filter_category.to_lower_string()
                                else {
                                    return OutputValue::Error(
                                        b"ERR invalid argument for 'command list'".to_vec(),
                                    );
                                };

                                match filter_category.as_str() {
                                    "module" => OutputValue::Error(
                                        b"ERR filterby module is not implemented yet".to_vec(),
                                    ),
                                    "aclcat" => {
                                        let Some(category) = filter_pattern
                                            .to_lower_string()
                                            .and_then(|s| s.parse().ok())
                                        else {
                                            return OutputValue::Error(
                                                b"ERR unknown ACL category for 'command list'"
                                                    .to_vec(),
                                            );
                                        };
                                        COMMANDS_BY_ACL_CATEGORY
                                            .with(move |map| map.get(&category).cloned())
                                            .unwrap_or_else(|| {
                                                OutputValue::Error(
                                                    b"ERR unknown ACL category for 'command list'"
                                                        .to_vec(),
                                                )
                                            })
                                    }
                                    "pattern" => {
                                        let finder = super::glob::Finder::new(&filter_pattern);
                                        let mut names: Vec<_> = SIMPLE_COMMANDS.with(|sc| {
                                            CONTAINER_COMMANDS.with(|cc| {
                                                sc.keys()
                                                    .chain(cc.keys())
                                                    .filter(|s| finder.do_match(s.as_bytes()))
                                                    .cloned()
                                                    .collect()
                                            })
                                        });
                                        names.sort_unstable();
                                        OutputValue::Array(
                                            names
                                                .into_iter()
                                                .map(|s| {
                                                    OutputValue::BulkString(s.as_bytes().to_vec())
                                                })
                                                .collect(),
                                        )
                                    }
                                    _ => OutputValue::Error(
                                        b"ERR unknown filter for 'command list'".to_vec(),
                                    ),
                                }
                            }
                            _ => OutputValue::Error(
                                b"ERR wrong number of arguments for 'command list'".to_vec(),
                            ),
                        },
                    },
                );
                // TODO: other subcommands for "command"
                map
            },
        },
    );
    map.insert(
        "function",
        ContainerCommand {
            category: &[AclCategory::Slow],
            handler: None,
            subcommands: {
                let mut map = HashMap::new();
                map.insert(
                    "flush",
                    SimpleCommand {
                        arity_min: 0,
                        arity_max: Some(1),
                        category: &[
                            AclCategory::Write,
                            AclCategory::Slow,
                            AclCategory::Scripting,
                        ],
                        handler: &move |_, _, _| {
                            // TODO: just for test
                            OutputValue::Ok
                        },
                    },
                );
                map
            },
        },
    );
    map.insert(
        "config",
        ContainerCommand {
            category: &[AclCategory::Slow],
            handler: None,
            subcommands: {
                let mut map = HashMap::new();
                map.insert(
                    "get",
                    SimpleCommand {
                        arity_min: 1,
                        arity_max: None,
                        category: &[
                            AclCategory::Admin,
                            AclCategory::Slow,
                            AclCategory::Dangerous,
                        ],
                        handler: &move |_, _, _| {
                            // TODO: just for test
                            OutputValue::NullArray
                        },
                    },
                );
                map
            },
        },
    );
    map
}
