use redis_module::{Context, RedisError, RedisResult, RedisValue};

enum ArgValue<'a> {
    Str(&'a str),
    String(String),
}

pub(crate) struct Cmd<'a> {
    name: &'a str,
    args: Vec<ArgValue<'a>>,
    arg_count: usize,
}

pub(crate) struct CmdResult {
    r: RedisResult,
}

impl CmdResult {
    pub(crate) fn new(r: RedisResult) -> Self {
        Self { r }
    }
    pub(crate) fn str(self, default: &str) -> std::result::Result<String, RedisError> {
        if let RedisValue::SimpleString(r) = self.r? {
            return Ok(r);
        }
        Ok(String::from(default))
    }
    pub(crate) fn strs(self, default: Vec<&str>) -> std::result::Result<Vec<String>, RedisError> {
        if let RedisValue::Array(r) = self.r? {
            return Ok(r
                .into_iter()
                .map(|v| {
                    if let RedisValue::SimpleString(r) = v {
                        return r;
                    }
                    String::from("")
                })
                .collect());
        }
        Ok(default.into_iter().map(String::from).collect())
    }
    pub(crate) fn i64(self, default: i64) -> std::result::Result<i64, RedisError> {
        if let RedisValue::Integer(r) = self.r? {
            return Ok(r);
        }
        Ok(default)
    }
    pub(crate) fn check(self) -> std::result::Result<(), RedisError> {
        self.r.map(|_| ())
    }
}

impl<'a> Cmd<'a> {
    pub(crate) fn new(name: &'a str, key: String) -> Self {
        Self {
            name,
            args: vec![ArgValue::String(key)],
            arg_count: 0,
        }
    }

    pub(crate) fn is_modified(&self) -> bool {
        self.arg_count > 0
    }

    pub(crate) fn arg<T: std::string::ToString>(mut self, arg: T) -> Self {
        self.arg_count += 1;
        self.args.push(ArgValue::String(arg.to_string()));
        self
    }

    pub(crate) fn arg_p<T: std::string::ToString>(mut self, prefix: &'a str, arg: T) -> Self {
        self.arg_count += 1;
        self.args.push(ArgValue::Str(prefix));
        self.args.push(ArgValue::String(arg.to_string()));
        self
    }

    pub(crate) fn arg_s(mut self, arg: &'a str) -> Self {
        self.arg_count += 1;
        self.args.push(ArgValue::Str(arg));
        self
    }

    pub(crate) fn args_s<I: Iterator<Item = &'a str>>(mut self, args: I) -> Self {
        for arg in args {
            self.arg_count += 1;
            self.args.push(ArgValue::Str(arg));
        }
        self
    }

    pub(crate) fn exec(self, ctx: &Context) -> CmdResult {
        CmdResult::new(
            ctx.call(
                &self.name,
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
