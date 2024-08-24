use strum::IntoEnumIterator;

use crate::value::Value;

#[derive(Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum AclCategory {
    Admin,
    Connection,
    Dangerous,
    Fast,
    Keyspace,
    Read,
    Slow,
    String,
    Write,
}

impl AclCategory {
    pub const fn as_bytes(&self) -> &'static [u8] {
        match self {
            AclCategory::Admin => b"admin".as_slice(),
            AclCategory::Connection => b"connection".as_slice(),
            AclCategory::Dangerous => b"dangerous".as_slice(),
            AclCategory::Fast => b"fast".as_slice(),
            AclCategory::Keyspace => b"keyspace".as_slice(),
            AclCategory::Read => b"read".as_slice(),
            AclCategory::Slow => b"slow".as_slice(),
            AclCategory::String => b"string".as_slice(),
            AclCategory::Write => b"write".as_slice(),
        }
    }

    pub fn array() -> Value {
        Value::Array(
            AclCategory::iter()
                .map(|c| Value::BulkString(c.as_bytes().to_vec()))
                .collect(),
        )
    }
}

impl std::str::FromStr for AclCategory {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "admin" => Ok(AclCategory::Admin),
            "connection" => Ok(AclCategory::Connection),
            "dangerous" => Ok(AclCategory::Dangerous),
            "fast" => Ok(AclCategory::Fast),
            "keyspace" => Ok(AclCategory::Keyspace),
            "read" => Ok(AclCategory::Read),
            "slow" => Ok(AclCategory::Slow),
            "string" => Ok(AclCategory::String),
            "write" => Ok(AclCategory::Write),
            _ => Err(()),
        }
    }
}
