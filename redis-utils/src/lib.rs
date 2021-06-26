use redis_module::{Context, RedisError, RedisResult, RedisValue};
use std::fmt;

enum ArgValue<'a> {
    Str(&'a str),
    String(String),
}

pub struct Cmd<'a> {
    name: &'a str,
    args: Vec<ArgValue<'a>>,
    arg_count: usize,
}

#[derive(Debug)]
pub struct ReError {
    e: RedisError,
}

impl fmt::Display for ReError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.e)
    }
}
impl std::error::Error for ReError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub struct ReResult {
    r: Result<ReValue, ReError>,
}

impl ReResult {
    pub fn new(r: RedisResult) -> Self {
        Self {
            r: r.map(|r| ReValue { r }).map_err(|e| ReError { e }),
        }
    }
    pub fn check(self) -> std::result::Result<(), ReError> {
        self.r.map(|_| ())
    }
    pub fn inner(self) -> Result<ReValue, ReError> {
        self.r
    }
}

pub struct ReValue {
    r: RedisValue,
}

impl ReValue {
    pub fn new(r: RedisValue) -> Self {
        Self { r }
    }

    pub fn iter(self) -> Box<dyn Iterator<Item = ReValue>> {
        if let RedisValue::Array(r) = self.r {
            Box::new(r.into_iter().map(|r| ReValue { r }))
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn str(self) -> Option<String> {
        match self.r {
            RedisValue::SimpleString(r) => Some(r),
            RedisValue::BulkString(r) => Some(r),
            RedisValue::SimpleStringStatic(r) => Some(String::from(r)),
            _ => None,
        }
    }

    pub fn f64(self) -> Option<f64> {
        if let RedisValue::Float(r) = self.r {
            return Some(r);
        }
        None
    }

    pub fn i64(self) -> Option<i64> {
        if let RedisValue::Integer(r) = self.r {
            return Some(r);
        }
        None
    }
}

impl<'a> Cmd<'a> {
    pub fn new(name: &'a str, key: String) -> Self {
        Self {
            name,
            args: vec![ArgValue::String(key)],
            arg_count: 0,
        }
    }

    pub fn is_modified(&self) -> bool {
        self.arg_count > 0
    }

    pub fn arg<T: std::string::ToString>(mut self, arg: T) -> Self {
        self.arg_count += 1;
        self.args.push(ArgValue::String(arg.to_string()));
        self
    }

    pub fn arg_p<T: std::string::ToString>(mut self, prefix: &'a str, arg: T) -> Self {
        self.arg_count += 1;
        self.args.push(ArgValue::Str(prefix));
        self.args.push(ArgValue::String(arg.to_string()));
        self
    }

    pub fn arg_s(mut self, arg: &'a str) -> Self {
        self.arg_count += 1;
        self.args.push(ArgValue::Str(arg));
        self
    }

    pub fn args_s<I: Iterator<Item = &'a str>>(mut self, args: I) -> Self {
        for arg in args {
            self.arg_count += 1;
            self.args.push(ArgValue::Str(arg));
        }
        self
    }

    pub fn exec(self, ctx: &Context) -> ReResult {
        ReResult::new(
            ctx.call(
                self.name,
                &self
                    .args
                    .iter()
                    .map(|a| match a {
                        ArgValue::Str(s) => *s,
                        ArgValue::String(s) => s.as_str(),
                    })
                    .collect::<Vec<_>>(),
            ),
        )
    }
}
