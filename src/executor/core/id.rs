#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnectionId(u8);

impl ConnectionId {
    pub fn plus_one(&self) -> Self {
        Self(self.0.wrapping_add(1))
    }
    pub fn minus_one(&self) -> Self {
        Self(self.0.wrapping_sub(1))
    }
}

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct ConnectionIdGenerator {
    next_id: ConnectionId,
}

impl Default for ConnectionIdGenerator {
    fn default() -> Self {
        Self {
            next_id: ConnectionId(0),
        }
    }
}

impl ConnectionIdGenerator {
    pub fn get_id(&mut self) -> ConnectionId {
        let res = self.next_id.clone();
        self.next_id = self.next_id.plus_one();
        res
    }

    pub fn peek_id(&self) -> ConnectionId {
        self.next_id.clone()
    }
}
