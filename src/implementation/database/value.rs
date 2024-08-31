use std::collections::HashMap;
use std::collections::VecDeque;

type RedisString = Vec<u8>;

#[derive(Clone, Debug)]
pub enum Value {
    String(RedisString),
    Hash(HashMap<RedisString, RedisString>),
    List(VecDeque<RedisString>),
}
