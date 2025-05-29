use crate::store::StoreManager;
use log::{debug, error};
use std::thread;

// 表示解析后的命令
#[derive(Debug, Clone)]
pub enum Command {
    // 字符串命令
    Set(String, String),
    Get(String),
    Del(String),

    // 列表命令
    LPush(String, String),
    RPush(String, String),
    Range(String, isize, isize),
    Len(String),
    LPop(String),
    RPop(String),
    LDel(String),

    // 哈希命令
    HSet(String, String, String),
    HGet(String, String),
    HDel(String, String),
    HDelKey(String),

    // 集合命令
    SAdd(String, Vec<String>),
    SMembers(String),
    SIsMember(String, String),
    SRem(String, String),

    // 持久化
    Save,
    BgSave,
    FlushDB,

    // 过期
    Expire(String, u64),
    DDL(String),
    
    // 事务命令
    Begin,               // 开始事务
    Commit,              // 提交事务
    Rollback,            // 回滚事务
    Checkpoint,          // 创建检查点
    CompactWal,          // 压缩WAL日志
    ListTransactions,    // 列出所有活跃事务
    
    // 其他命令
    Ping,
    Help,
    HelpCommand(String),

    // 无效命令
    Invalid(String),
}

// 命令处理器
pub struct CommandHandler {
    store_manager: StoreManager,
    data_file: String,
}

impl CommandHandler {
    pub fn new(store_manager: StoreManager, data_file: String) -> Self {
        CommandHandler {
            store_manager,
            data_file,
        }
    }

    // 解析命令字符串
    pub fn parse_command(&self, input: &str) -> Command {
        let input = input.trim();
        let parts: Vec<&str> = input.split_whitespace().collect();

        if parts.is_empty() {
            return Command::Invalid("Empty command".to_string());
        }

        match parts[0].to_lowercase().as_str() {
            // 事务命令
            "begin" | "multi" => Command::Begin,
            "commit" | "exec" => Command::Commit,
            "rollback" | "discard" => Command::Rollback,
            "checkpoint" => Command::Checkpoint,
            "compactwal" => Command::CompactWal,
            "transactions" | "listtx" => Command::ListTransactions,
            
            // 字符串命令
            "set" => {
                if parts.len() < 3 {
                    Command::Invalid("Usage: SET key value [EX seconds]".to_string())
                } else {
                    let key = parts[1].to_string();

                    // 检查是否有EX选项
                    if parts.len() >= 5 && parts[parts.len() - 2].to_uppercase() == "EX" {
                        if let Ok(seconds) = parts[parts.len() - 1].parse::<u64>() {
                            // 如果有EX选项，value是除了key、EX和seconds之外的所有部分
                            let value = parts[2..parts.len() - 2].join(" ");
                            return Command::Set(key, value + " EX " + &seconds.to_string());
                        }
                    }

                    // 没有EX选项或EX选项无效
                    let value = parts[2..].join(" ");
                    Command::Set(key, value)
                }
            }
            "get" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: GET key".to_string())
                } else {
                    Command::Get(parts[1].to_string())
                }
            }
            "del" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: DEL key".to_string())
                } else {
                    Command::Del(parts[1].to_string())
                }
            }

            // 列表命令
            "lpush" => {
                if parts.len() < 3 {
                    Command::Invalid("Usage: LPUSH key value".to_string())
                } else {
                    let key = parts[1].to_string();
                    let value = parts[2..].join(" ");
                    Command::LPush(key, value)
                }
            }
            "rpush" => {
                if parts.len() < 3 {
                    Command::Invalid("Usage: RPUSH key value".to_string())
                } else {
                    let key = parts[1].to_string();
                    let value = parts[2..].join(" ");
                    Command::RPush(key, value)
                }
            }
            "range" => {
                if parts.len() != 4 {
                    Command::Invalid("Usage: RANGE key start end".to_string())
                } else {
                    let key = parts[1].to_string();
                    match (parts[2].parse::<isize>(), parts[3].parse::<isize>()) {
                        (Ok(start), Ok(end)) => Command::Range(key, start, end),
                        _ => Command::Invalid("Start and end must be integers".to_string()),
                    }
                }
            }
            "len" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: LEN key".to_string())
                } else {
                    Command::Len(parts[1].to_string())
                }
            }
            "lpop" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: LPOP key".to_string())
                } else {
                    Command::LPop(parts[1].to_string())
                }
            }
            "rpop" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: RPOP key".to_string())
                } else {
                    Command::RPop(parts[1].to_string())
                }
            }
            "ldel" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: LDEL key".to_string())
                } else {
                    Command::LDel(parts[1].to_string())
                }
            }

            // 哈希命令
            "hset" => {
                if parts.len() < 4 {
                    Command::Invalid("Usage: HSET key field value".to_string())
                } else {
                    let key = parts[1].to_string();
                    let field = parts[2].to_string();
                    let value = parts[3..].join(" ");
                    Command::HSet(key, field, value)
                }
            }
            "hget" => {
                if parts.len() != 3 {
                    Command::Invalid("Usage: HGET key field".to_string())
                } else {
                    Command::HGet(parts[1].to_string(), parts[2].to_string())
                }
            }
            "hdel" => {
                if parts.len() == 2 {
                    Command::HDelKey(parts[1].to_string())
                } else if parts.len() == 3 {
                    Command::HDel(parts[1].to_string(), parts[2].to_string())
                } else {
                    Command::Invalid("Usage: HDEL key [field]".to_string())
                }
            }
            "sadd"=>{
                if parts.len() < 3 {
                    Command::Invalid("Usage: SADD key value1 [value2 ...]".to_string())
                } else {
                    let key = parts[1].to_string();
                    let values = parts[2..].iter().map(|s| s.to_string()).collect();
                    Command::SAdd(key, values)
                }
            }
            "smembers" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: SMEMBERS key".to_string())
                } else {
                    Command::SMembers(parts[1].to_string())
                }
            }   
            "sismember" => {
                if parts.len() != 3 {
                    Command::Invalid("Usage: SISMEMBER key value".to_string())
                } else {
                    Command::SIsMember(parts[1].to_string(), parts[2].to_string())
                }
            }
            "srem" => {
                if parts.len() != 3 {
                    Command::Invalid("Usage: SREM key value".to_string())
                } else {
                    Command::SRem(parts[1].to_string(), parts[2].to_string())
                }
            }
            "save" => Command::Save,
            "bgsave" => Command::BgSave,
            "flushdb" => Command::FlushDB,
            "expire" => {
                if parts.len() != 3 {
                    Command::Invalid("Usage: EXPIRE key seconds".to_string())
                } else {
                    let key = parts[1].to_string();
                    match parts[2].parse::<u64>() {
                        Ok(seconds) => Command::Expire(key, seconds),
                        Err(_) => {
                            Command::Invalid("Seconds must be a positive integer".to_string())
                        }
                    }
                }
            }
            "ddl" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: DDL key".to_string())
                } else {
                    Command::DDL(parts[1].to_string())
                }
            }
            // 其他命令
            "ping" => Command::Ping,
            "help" => {
                if parts.len() == 1 {
                    Command::Help
                } else {
                    Command::HelpCommand(parts[1].to_string())
                }
            }
            _ => Command::Invalid(format!("Unknown command: {}", parts[0])),
        }
    }

    // 执行命令
    pub fn execute_command(&self, command: Command) -> String {
        // 确定WAL日志路径
        let wal_path = std::path::Path::new(&self.data_file)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("wal.log");
            
        // 尝试使用事务处理器
        let use_transaction_handler = |f: fn(&crate::transaction_cmd::TransactionCommandHandler) -> Result<String, String>| -> String {
            // 创建事务处理器
            let handler = crate::transaction_cmd::TransactionCommandHandler::new(&wal_path);
            match f(&handler) {
                Ok(result) => result,
                Err(e) => format!("ERROR: {}", e)
            }
        };
        
        match command {
            // 事务命令
            Command::Begin => use_transaction_handler(|h| h.begin()),
            Command::Commit => use_transaction_handler(|h| h.commit()),
            Command::Rollback => use_transaction_handler(|h| h.rollback()),
            Command::Checkpoint => use_transaction_handler(|h| h.checkpoint()),
            Command::CompactWal => use_transaction_handler(|h| h.compact()),
            Command::ListTransactions => use_transaction_handler(|h| h.list_transactions()),
            
            // 字符串命令 - 使用新的StoreManager API
            Command::Set(key, value) => {
                match self.store_manager.set_string(key, value) {
                    Ok(result) => result,
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::Get(key) => {
                match self.store_manager.get_string(&key) {
                    Ok(Some(value)) => value,
                    Ok(None) => "(nil)".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::Del(key) => {
                match self.store_manager.del_key(&key) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }

            // 列表命令 - 使用新的StoreManager API
            Command::LPush(key, value) => {
                match self.store_manager.lpush(key, value) {
                    Ok(len) => len.to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::RPush(key, value) => {
                match self.store_manager.rpush(key, value) {
                    Ok(len) => len.to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::Range(key, start, end) => {
                match self.store_manager.range(&key, start, end) {
                    Ok(values) => {
                        if values.is_empty() {
                            "(empty list)".to_string()
                        } else {
                            values.join("\n")
                        }
                    },
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::Len(key) => {
                match self.store_manager.llen(&key) {
                    Ok(len) => len.to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::LPop(key) => {
                match self.store_manager.lpop(&key) {
                    Ok(Some(value)) => value,
                    Ok(None) => "(nil)".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::RPop(key) => {
                match self.store_manager.rpop(&key) {
                    Ok(Some(value)) => value,
                    Ok(None) => "(nil)".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::LDel(key) => {
                match self.store_manager.ldel(&key) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }

            // 哈希命令 - 使用新的StoreManager API
            Command::HSet(key, field, value) => {
                match self.store_manager.hset(key, field, value) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::HGet(key, field) => {
                match self.store_manager.hget(&key, &field) {
                    Ok(Some(value)) => value,
                    Ok(None) => "(nil)".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::HDel(key, field) => {
                match self.store_manager.hdel_field(&key, &field) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::HDelKey(key) => {
                match self.store_manager.hdel_key(&key) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            
            // 集合命令 - 使用新的StoreManager API
            Command::SAdd(key, value) => {
                match self.store_manager.sadd(key, value) {
                    Ok(count) => count.to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::SMembers(key) => {
                match self.store_manager.smembers(&key) {
                    Ok(members) if !members.is_empty() => {
                        members.into_iter().collect::<Vec<String>>().join("\n")
                    },
                    Ok(_) => "(empty set)".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::SIsMember(key, value) => {
                match self.store_manager.smember_query(&key, &value) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::SRem(key, value) => {
                match self.store_manager.srem(&key, &value) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::Save => {
                match self.store_manager.save_to_file(&self.data_file) {
                    Ok(_) => "Saved".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::BgSave => {
                let data_file = self.data_file.clone();
                let store_manager = self.store_manager.clone();
                thread::spawn(move || {
                    if let Err(e) = store_manager.save_to_file(&data_file) {
                        error!("Background save failed: {}", e);
                    } else {
                        debug!("Background save completed");
                    }
                });
                "Background save started".to_string()
            }
            Command::FlushDB => {
                // 创建新的空Store并替换现有的
                let store_guard = self.store_manager.get_store();
                let mut store = store_guard.lock().unwrap();
                *store = crate::store::Store::new();
                
                // 保存空状态
                match self.store_manager.save_to_file(&self.data_file) {
                    Ok(_) => "OK".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::Expire(key, seconds) => {
                match self.store_manager.expire(&key, seconds) {
                    Ok(true) => "1".to_string(),
                    Ok(false) => "0".to_string(),
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            Command::DDL(key) => {
                match self.store_manager.ttl(&key) {
                    Ok(ttl) => {
                        if ttl == -2 {
                            "Key does not exist".to_string()
                        } else if ttl == -1 {
                            "No expiration".to_string()
                        } else {
                            format!("TTL: {} seconds", ttl)
                        }
                    },
                    Err(e) => format!("ERROR: {}", e)
                }
            }
            // 其他命令
            Command::Ping => "PONG".to_string(),
            Command::Help => self.get_help(),
            Command::HelpCommand(cmd) => self.get_command_help(&cmd),
            Command::Invalid(msg) => format!("ERROR: {}", msg),
        }
    }

    // 持久化数据方法已经被移除，改为直接调用 store_manager 的 save_to_file 方法

    // 获取帮助信息
    fn get_help(&self) -> String {
        let help = r"可用命令:
字符串类型命令:
  set [key] [value] - 存储key-value类型数据
  get [key] - 获取key对应的value
  del [key] - 删除key对应的value

双向链表类型命令:
  lpush [key] [value] - 在链表左端添加数据
  rpush [key] [value] - 在链表右端添加数据
  range [key] [start] [end] - 获取start到end位置的数据
  len [key] - 获取链表长度
  lpop [key] - 获取并删除左端数据
  rpop [key] - 获取并删除右端数据
  ldel [key] - 删除整个链表

哈希类型命令:
  hset [key] [field] [value] - 存储哈希表字段
  hget [key] [field] - 获取哈希表字段值
  hdel [key] [field] - 删除哈希表字段
  hdel [key] - 删除整个哈希表

其他命令:
  ping - 测试服务器连接
  help - 获取所有命令帮助
  help [command] - 获取特定命令帮助";

        help.to_string()
    }

    // 获取特定命令的帮助信息
    fn get_command_help(&self, command: &str) -> String {
        match command.to_lowercase().as_str() {
            "set" => "set [key] [value] - 存储key-value类型数据".to_string(),
            "get" => "get [key] - 获取key对应的value".to_string(),
            "del" => "del [key] - 删除key对应的value".to_string(),
            "lpush" => "lpush [key] [value] - 在链表左端添加数据".to_string(),
            "rpush" => "rpush [key] [value] - 在链表右端添加数据".to_string(),
            "range" => "range [key] [start] [end] - 获取start到end位置的数据".to_string(),
            "len" => "len [key] - 获取链表长度".to_string(),
            "lpop" => "lpop [key] - 获取并删除左端数据".to_string(),
            "rpop" => "rpop [key] - 获取并删除右端数据".to_string(),
            "ldel" => "ldel [key] - 删除整个链表".to_string(),
            "hset" => "hset [key] [field] [value] - 存储哈希表字段".to_string(),
            "hget" => "hget [key] [field] - 获取哈希表字段值".to_string(),
            "hdel" => {
                "hdel [key] [field] - 删除哈希表字段\nhdel [key] - 删除整个哈希表".to_string()
            }
            "ping" => "ping - 测试服务器连接".to_string(),
            "help" => "help - 获取所有命令帮助\nhelp [command] - 获取特定命令帮助".to_string(),
            _ => format!("Unknown command: {}", command),
        }
    }
}
