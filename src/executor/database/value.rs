use std::collections::HashMap;

type RedisString = Vec<u8>;

#[derive(Clone, Debug)]
pub enum Value {
    String(RedisString),
    Hash(HashMap<RedisString, RedisString>),
    List(Vec<RedisString>),
}
