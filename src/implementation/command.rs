use std::collections::HashMap;

use super::acl::AclCategory;
use super::InputValue;
use super::Interrupt;
use crate::bstr::BStr;
use crate::interface::database::map::{Key, MapAllCommands, MapMiscCommands, MapStringCommands};
use crate::interface::types::OutputValue;

type CommandHandler<D> = dyn Fn(&mut D, Vec<InputValue>) -> OutputValue + 'static;
type ControllerCommandHandler = dyn Fn(Vec<InputValue>) -> Result<Interrupt, OutputValue>;

impl Key for Vec<u8> {}
impl Key for &[u8] {}

trait HashMapExt<K, V>
where
    K: Eq + std::hash::Hash,
{
    fn insert_without_duplicate(&mut self, key: K, value: V);
}

impl<K, V> HashMapExt<K, V> for HashMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    fn insert_without_duplicate(&mut self, key: K, value: V) {
        let out = self.insert(key, value);
        if out.is_some() {
            panic!("Duplicate key");
        }
    }
}

pub struct SimpleCommand<D: 'static> {
    pub handler: &'static CommandHandler<D>,
    pub category: &'static [AclCategory],
    arity_min: usize,
    arity_max: Option<usize>,
}

impl<D> std::fmt::Debug for SimpleCommand<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleCommand")
            .field("handler", &"...")
            .field("category", &self.category)
            .field("arity_min", &self.arity_min)
            .field("arity_max", &self.arity_max)
            .finish()
    }
}

pub struct ControllerCommandDefinition {
    pub handler: &'static ControllerCommandHandler,
    pub category: &'static [AclCategory],
    arity_min: usize,
    arity_max: Option<usize>,
}

impl std::fmt::Debug for ControllerCommandDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleCommand")
            .field("handler", &"...")
            .field("category", &self.category)
            .field("arity_min", &self.arity_min)
            .field("arity_max", &self.arity_max)
            .finish()
    }
}

pub struct ContainerCommand {
    pub handler: Option<&'static ControllerCommandHandler>,
    pub category: &'static [AclCategory],
    subcommands: HashMap<&'static str, ControllerCommandDefinition>,
}

impl std::fmt::Debug for ContainerCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContainerCommand")
            .field("handler", &"...")
            .field("category", &self.category)
            .field("subcommands", &self.subcommands)
            .finish()
    }
}

pub trait Command<D> {
    fn is_arity_correct(&self, arity: usize) -> bool;
    fn execute(&self, name: &str, db: &mut D, input: Vec<InputValue>) -> OutputValue;
}

impl<D> Command<D> for SimpleCommand<D> {
    fn is_arity_correct(&self, arity: usize) -> bool {
        if arity < self.arity_min {
            false
        } else if let Some(max) = self.arity_max {
            arity <= max
        } else {
            true
        }
    }

    fn execute(&self, name: &str, db: &mut D, input: Vec<Vec<u8>>) -> OutputValue {
        if !self.is_arity_correct(input.len()) {
            return OutputValue::Error(
                format!("ERR wrong number of arguments for '{}'", name).into_bytes(),
            );
        }
        (self.handler)(db, input)
    }
}

pub trait ControllerCommand {
    fn is_arity_correct(&self, arity: usize) -> bool;
    fn execute(&self, name: &str, input: Vec<InputValue>) -> Result<Interrupt, OutputValue>;
}

impl ControllerCommand for ControllerCommandDefinition {
    fn is_arity_correct(&self, arity: usize) -> bool {
        if arity < self.arity_min {
            false
        } else if let Some(max) = self.arity_max {
            arity <= max
        } else {
            true
        }
    }

    fn execute(&self, name: &str, input: Vec<Vec<u8>>) -> Result<Interrupt, OutputValue> {
        if !self.is_arity_correct(input.len()) {
            return Err(OutputValue::Error(
                format!("ERR wrong number of arguments for '{}'", name).into_bytes(),
            ));
        }
        (self.handler)(input)
    }
}

impl ControllerCommand for ContainerCommand {
    fn is_arity_correct(&self, arity: usize) -> bool {
        if self.handler.is_none() {
            // arity == 0 is disallowed
            arity >= 1
        } else {
            true
        }
    }

    fn execute(&self, name: &str, mut input: Vec<InputValue>) -> Result<Interrupt, OutputValue> {
        if !self.is_arity_correct(input.len()) {
            return Err(OutputValue::Error(
                format!("ERR wrong number of arguments for '{}'", name).into_bytes(),
            ));
        }
        match (input.len(), self.handler) {
            (0, Some(handler)) => (handler)(input),
            (0, None) => unreachable!(),
            _ => {
                let rest = input.drain(1..).collect::<Vec<_>>();
                let sub_bytes = get_first(input);
                let Some(sub) = sub_bytes.to_str() else {
                    return Err(OutputValue::Error(
                        format!("ERR unknown subcommand for '{}'", name).into_bytes(),
                    ));
                };
                let Some(cmd) = self.subcommands.get(sub) else {
                    return Err(OutputValue::Error(
                        format!("ERR unknown subcommand for '{}'", name).into_bytes(),
                    ));
                };

                cmd.execute(format!("{} {}", name, sub).as_str(), rest)
            }
        }
    }
}

#[derive(Debug)]
pub struct CommandStore<D: 'static> {
    pub simple_commands: HashMap<&'static str, SimpleCommand<D>>,
    pub container_commands: HashMap<&'static str, ContainerCommand>,
    pub controller_commands: HashMap<&'static str, ControllerCommandDefinition>,
    pub names: OutputValue,
    pub commands_by_acl_category: HashMap<AclCategory, OutputValue>,
}

impl<D: 'static + MapAllCommands> Default for CommandStore<D> {
    fn default() -> Self {
        let simple_commands = initialise_simple_commands();
        let container_commands = initialise_container_commands();
        let controller_commands = initialise_controller_commands();
        let name_to_category = generate_command_name_to_category(
            &simple_commands,
            &container_commands,
            &controller_commands,
        );
        let names = initialise_command_names(&name_to_category);
        let commands_by_acl_category = initialise_category_map(&name_to_category);

        Self {
            simple_commands,
            container_commands,
            controller_commands,
            names,
            commands_by_acl_category,
        }
    }
}

pub enum CommandListFilter {
    All,
    Category(AclCategory),
    Pattern(Vec<u8>),
    // module is not supported yet
}

impl<D> CommandStore<D> {
    pub fn count(&self) -> OutputValue {
        let simple_counts = self.simple_commands.len();
        let controller_counts = self.controller_commands.len();
        let container_counts: usize = self
            .container_commands
            .values()
            .map(|c| c.subcommands.len() + c.handler.is_some() as usize)
            .sum();
        // i64 should be enough for counting commands
        OutputValue::Integer((simple_counts + controller_counts + container_counts) as i64)
    }

    pub fn list(&self, filter: CommandListFilter) -> OutputValue {
        match filter {
            CommandListFilter::All => self.names.clone(),
            CommandListFilter::Category(cat) => self
                .commands_by_acl_category
                .get(&cat)
                .expect("all category should be in the hashmap")
                .clone(),
            CommandListFilter::Pattern(pattern) => {
                let finder = super::glob::Finder::new(&pattern);
                let OutputValue::Array(vs) = &self.names else {
                    unreachable!()
                };
                OutputValue::Array(
                    vs.iter()
                        .filter(|v| {
                            let OutputValue::BulkString(i) = v else {
                                unreachable!()
                            };
                            finder.it_matches(i)
                        })
                        .cloned()
                        .collect(),
                )
            }
        }
    }
}

fn generate_command_name_to_category<D>(
    simple_commands: &HashMap<&'static str, SimpleCommand<D>>,
    container_commands: &HashMap<&'static str, ContainerCommand>,
    controller_commands: &HashMap<&'static str, ControllerCommandDefinition>,
) -> HashMap<String, &'static [AclCategory]> {
    let mut ret = HashMap::new();
    for (k, v) in simple_commands.iter() {
        ret.insert(k.to_string(), v.category);
    }

    for (k, v) in container_commands.iter() {
        ret.insert(k.to_string(), v.category);
        for (k2, v2) in v.subcommands.iter() {
            ret.insert(format!("{k}|{k2}"), v2.category);
        }
    }

    for (k, v) in controller_commands.iter() {
        ret.insert(k.to_string(), v.category);
    }

    ret
}

fn initialise_command_names(
    name_to_category: &HashMap<String, &'static [AclCategory]>,
) -> OutputValue {
    let mut names = name_to_category
        .keys()
        .map(|s| s.as_bytes().to_vec())
        .collect::<Vec<_>>();
    names.sort_unstable();
    OutputValue::Array(names.into_iter().map(OutputValue::BulkString).collect())
}

fn initialise_category_map(
    name_to_category: &HashMap<String, &'static [AclCategory]>,
) -> HashMap<AclCategory, OutputValue> {
    let mut map: HashMap<AclCategory, Vec<Vec<u8>>> = HashMap::new();
    for (name, categories) in name_to_category.iter() {
        for cat in categories.iter() {
            map.entry(*cat).or_default().push(name.as_bytes().to_vec());
        }
    }
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

fn initialise_simple_commands<D: MapAllCommands>() -> HashMap<&'static str, SimpleCommand<D>> {
    let mut map = HashMap::<&'static str, SimpleCommand<D>>::new();
    map.insert_without_duplicate(
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
            handler: &move |db, _| {
                // TODO: support async
                db.flushdb()
            },
        },
    );
    map.extend(initialise_ping_echo());
    map.extend(initialise_misc_commands());
    map.extend(initialise_string_commands());
    map.extend(initialise_list_commands());
    map.extend(initialise_hash_commands());
    map.extend(initialise_set_commands());
    map
}

fn initialise_ping_echo<D>() -> HashMap<&'static str, SimpleCommand<D>> {
    let mut map = HashMap::new();
    map.insert_without_duplicate(
        "ping",
        SimpleCommand {
            arity_min: 0,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, input| {
                input
                    .into_iter()
                    .next()
                    .map(OutputValue::BulkString)
                    .unwrap_or_else(|| OutputValue::SimpleString(b"PONG".as_ref().to_vec()))
            },
        },
    );
    map.insert_without_duplicate(
        "echo",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |_, input| OutputValue::BulkString(get_first(input)),
        },
    );
    map
}

fn initialise_misc_commands<D: MapMiscCommands>() -> HashMap<&'static str, SimpleCommand<D>> {
    let mut map = HashMap::<&'static str, SimpleCommand<D>>::new();
    map.insert_without_duplicate(
        "dbsize",
        SimpleCommand {
            arity_min: 0,
            arity_max: Some(0),
            category: &[AclCategory::Keyspace, AclCategory::Read, AclCategory::Fast],
            handler: &move |db, _| OutputValue::Integer(db.len() as i64),
        },
    );
    map.insert_without_duplicate(
        "exists",
        SimpleCommand {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Keyspace, AclCategory::Read, AclCategory::Fast],
            handler: &move |db, input| db.exists(input),
        },
    );
    map.insert_without_duplicate(
        "del",
        SimpleCommand {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Keyspace, AclCategory::Write, AclCategory::Slow],
            handler: &move |db, input| db.del(input),
        },
    );
    map.insert_without_duplicate(
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
            handler: &move |db, input| {
                let pattern = get_first(input);
                db.keys(pattern)
            },
        },
    );
    map
}

fn initialise_controller_commands() -> HashMap<&'static str, ControllerCommandDefinition> {
    let mut map = HashMap::new();
    map.insert_without_duplicate(
        "select",
        ControllerCommandDefinition {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Fast, AclCategory::Connection],
            handler: &move |input| {
                get_first(input)
                    .parse_into()
                    .map(Interrupt::Select)
                    .ok_or_else(|| {
                        OutputValue::Error(b"ERR invalid argument for 'select'".to_vec())
                    })
            },
        },
    );

    map.insert_without_duplicate(
        "flushall",
        ControllerCommandDefinition {
            arity_min: 0,
            arity_max: Some(0),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Slow,
                AclCategory::Dangerous,
            ],
            handler: &move |_| {
                // TODO: support async
                Ok(Interrupt::FlushAll)
            },
        },
    );
    map.insert_without_duplicate(
        "swapdb",
        ControllerCommandDefinition {
            arity_min: 2,
            arity_max: Some(2),
            category: &[
                AclCategory::Keyspace,
                AclCategory::Write,
                AclCategory::Fast,
                AclCategory::Dangerous,
            ],
            handler: &move |input| {
                let (db1, db2) = get_first_two(input);
                let Some(db1) = db1.parse_into() else {
                    return Err(OutputValue::Error(b"ERR invalid first DB index".to_vec()));
                };
                let Some(db2) = db2.parse_into() else {
                    return Err(OutputValue::Error(b"ERR invalid second DB index".to_vec()));
                };
                Ok(Interrupt::SwapDb(db1, db2))
            },
        },
    );
    map
}

fn initialise_string_commands<T: MapStringCommands>() -> HashMap<&'static str, SimpleCommand<T>> {
    let mut map = HashMap::<_, SimpleCommand<T>>::new();
    map.insert_without_duplicate(
        "get",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let key = get_first(input);
                db.get(key)
            },
        },
    );
    map.insert_without_duplicate(
        "set",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(5),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Slow],
            handler: &move |db, input| {
                // TODO: support options
                let (key, value) = get_first_two(input);
                db.set(key, value)
            },
        },
    );
    map.insert_without_duplicate(
        "mget",
        SimpleCommand {
            arity_min: 1,
            arity_max: None,
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| db.mget(input),
        },
    );
    map.insert_without_duplicate(
        "mset",
        SimpleCommand {
            arity_min: 2,
            arity_max: None,
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Slow],
            handler: &move |db, input| {
                if input.len() % 2 != 0 {
                    return OutputValue::Error(
                        b"ERR wrong number of arguments for 'mget'".to_vec(),
                    );
                }
                db.mset(input)
            },
        },
    );
    map.insert_without_duplicate(
        "msetnx",
        SimpleCommand {
            arity_min: 2,
            arity_max: None,
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Slow],
            handler: &move |db, input| {
                if input.len() % 2 != 0 {
                    return OutputValue::Error(
                        b"ERR wrong number of arguments for 'mget'".to_vec(),
                    );
                }
                db.msetnx(input)
            },
        },
    );
    map.insert_without_duplicate(
        "append",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let (key, value) = get_first_two(input);
                db.append(key, value)
            },
        },
    );
    map.insert_without_duplicate(
        "strlen",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Read, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let key = get_first(input);
                db.strlen(key)
            },
        },
    );
    map.insert_without_duplicate(
        "incr",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let key = get_first(input);
                db.incr(key)
            },
        },
    );
    map.insert_without_duplicate(
        "decr",
        SimpleCommand {
            arity_min: 1,
            arity_max: Some(1),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let key = get_first(input);
                db.decr(key)
            },
        },
    );
    map.insert_without_duplicate(
        "incrby",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let (key, value) = get_first_two(input);
                let Some(value) = value.parse_into() else {
                    return OutputValue::Error(b"ERR value is not an integer".to_vec());
                };
                db.incrby(key, value)
            },
        },
    );
    map.insert_without_duplicate(
        "decrby",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let (key, value) = get_first_two(input);
                let Some(value) = value.parse_into() else {
                    return OutputValue::Error(b"ERR value is not an integer".to_vec());
                };
                db.decrby(key, value)
            },
        },
    );
    map.insert_without_duplicate(
        "incrbyfloat",
        SimpleCommand {
            arity_min: 2,
            arity_max: Some(2),
            category: &[AclCategory::Write, AclCategory::String, AclCategory::Fast],
            handler: &move |db, input| {
                let (key, value) = get_first_two(input);
                let Some(value) = value.parse_into() else {
                    return OutputValue::Error(b"ERR value is not an floating number".to_vec());
                };
                db.incrbyfloat(key, value)
            },
        },
    );
    map
}

fn initialise_list_commands<T>() -> HashMap<&'static str, SimpleCommand<T>> {
    let mut map = HashMap::new();
    // TODO
    map
}

fn initialise_hash_commands<T>() -> HashMap<&'static str, SimpleCommand<T>> {
    let mut map = HashMap::new();
    // TODO
    map
}

fn initialise_set_commands<T>() -> HashMap<&'static str, SimpleCommand<T>> {
    let mut map = HashMap::new();
    // TODO
    map
}

fn initialise_container_commands() -> HashMap<&'static str, ContainerCommand> {
    let mut map = HashMap::new();
    map.insert_without_duplicate(
        "acl",
        ContainerCommand {
            category: &[AclCategory::Slow],
            handler: None,
            subcommands: {
                let mut map = HashMap::new();
                map.insert_without_duplicate(
                    "cat",
                    ControllerCommandDefinition {
                        arity_min: 0,
                        arity_max: Some(1),
                        category: &[AclCategory::Slow],
                        handler: &move |input| match input.len() {
                            0 => Ok(Interrupt::AclCat(None)),
                            1 => {
                                let Some(category) = get_first(input).parse_into() else {
                                    return Err(OutputValue::Error(
                                        b"ERR unknown ACL category for 'acl cat'".to_vec(),
                                    ));
                                };
                                Ok(Interrupt::AclCat(Some(category)))
                            }
                            _ => unreachable!(),
                        },
                    },
                );
                map
            },
        },
    );
    map.insert_without_duplicate(
        "client",
        ContainerCommand {
            handler: None,
            category: &[AclCategory::Slow],
            subcommands: {
                let mut map = HashMap::new();
                map.insert_without_duplicate(
                    "id",
                    ControllerCommandDefinition {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[AclCategory::Slow, AclCategory::Connection],
                        handler: &move |_| Ok(Interrupt::ClientId),
                    },
                );
                map.insert_without_duplicate(
                    "list",
                    ControllerCommandDefinition {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[
                            AclCategory::Admin,
                            AclCategory::Slow,
                            AclCategory::Dangerous,
                            AclCategory::Connection,
                        ],
                        handler: &move |_| Ok(Interrupt::ClientList),
                    },
                );
                map
            },
        },
    );
    map.insert_without_duplicate(
        "command",
        ContainerCommand {
            category: &[AclCategory::Slow, AclCategory::Connection],
            handler: Some(&move |_| {
                Err(OutputValue::Error(
                    b"ERR 'command' is not implemented yet".to_vec(),
                ))
            }),
            subcommands: {
                let mut map = HashMap::new();
                map.insert_without_duplicate(
                    "count",
                    ControllerCommandDefinition {
                        arity_min: 0,
                        arity_max: Some(0),
                        category: &[AclCategory::Slow, AclCategory::Connection],
                        handler: &move |_| Ok(Interrupt::CommandCount),
                    },
                );
                map.insert_without_duplicate(
                    "list",
                    ControllerCommandDefinition {
                        arity_min: 0,
                        arity_max: Some(3),
                        category: &[AclCategory::Slow, AclCategory::Connection],
                        handler: &move |input| match input.len() {
                            0 => Ok(Interrupt::CommandList(CommandListFilter::All)),
                            3 => {
                                let (filterby, filter_category, filter_pattern) =
                                    get_first_three(input);
                                if let Some("filterby") = filterby.to_lower_string().as_deref() {
                                    // OK
                                } else {
                                    return Err(OutputValue::Error(
                                        b"ERR invalid argument for 'command list'".to_vec(),
                                    ));
                                }

                                let Some(filter_category) = filter_category.to_lower_string()
                                else {
                                    return Err(OutputValue::Error(
                                        b"ERR invalid argument for 'command list'".to_vec(),
                                    ));
                                };

                                match filter_category.as_str() {
                                    "module" => Err(OutputValue::Error(
                                        b"ERR filterby module is not implemented yet".to_vec(),
                                    )),
                                    "aclcat" => {
                                        let Some(category) = filter_pattern
                                            .to_lower_string()
                                            .and_then(|s| s.parse().ok())
                                        else {
                                            return Err(OutputValue::Error(
                                                b"ERR unknown ACL category for 'command list'"
                                                    .to_vec(),
                                            ));
                                        };
                                        Ok(Interrupt::CommandList(CommandListFilter::Category(
                                            category,
                                        )))
                                    }
                                    "pattern" => Ok(Interrupt::CommandList(
                                        CommandListFilter::Pattern(filter_pattern),
                                    )),
                                    _ => Err(OutputValue::Error(
                                        b"ERR unknown filter for 'command list'".to_vec(),
                                    )),
                                }
                            }
                            _ => Err(OutputValue::Error(
                                b"ERR wrong number of arguments for 'command list'".to_vec(),
                            )),
                        },
                    },
                );
                // TODO: other subcommands for "command"
                map
            },
        },
    );
    map.insert_without_duplicate(
        "function",
        ContainerCommand {
            category: &[AclCategory::Slow],
            handler: None,
            subcommands: {
                let mut map = HashMap::new();
                map.insert_without_duplicate(
                    "flush",
                    ControllerCommandDefinition {
                        arity_min: 0,
                        arity_max: Some(1),
                        category: &[
                            AclCategory::Write,
                            AclCategory::Slow,
                            AclCategory::Scripting,
                        ],
                        handler: &move |_| {
                            // TODO: just for test
                            Err(OutputValue::Ok)
                        },
                    },
                );
                map
            },
        },
    );
    map.insert_without_duplicate(
        "config",
        ContainerCommand {
            category: &[AclCategory::Slow],
            handler: None,
            subcommands: {
                let mut map = HashMap::new();
                map.insert_without_duplicate(
                    "get",
                    ControllerCommandDefinition {
                        arity_min: 1,
                        arity_max: None,
                        category: &[
                            AclCategory::Admin,
                            AclCategory::Slow,
                            AclCategory::Dangerous,
                        ],
                        handler: &move |_| {
                            // TODO: just for test
                            Err(OutputValue::NullArray)
                        },
                    },
                );
                map
            },
        },
    );
    map
}
