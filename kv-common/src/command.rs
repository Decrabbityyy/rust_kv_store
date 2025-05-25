use crate::store::StoreManager;
use log::{debug, error};

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
            // 字符串命令
            "set" => {
                if parts.len() < 3 {
                    Command::Invalid("Usage: SET key value [EX seconds]".to_string())
                } else {
                    let key = parts[1].to_string();
                    
                    // 检查是否有EX选项
                    if parts.len() >= 5 && parts[parts.len()-2].to_uppercase() == "EX" {
                        if let Ok(seconds) = parts[parts.len()-1].parse::<u64>() {
                            // 如果有EX选项，value是除了key、EX和seconds之外的所有部分
                            let value = parts[2..parts.len()-2].join(" ");
                            return Command::Set(key, value + " EX " + &seconds.to_string());
                        }
                    }
                    
                    // 没有EX选项或EX选项无效
                    let value = parts[2..].join(" ");
                    Command::Set(key, value)
                }
            },
            "get" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: GET key".to_string())
                } else {
                    Command::Get(parts[1].to_string())
                }
            },
            "del" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: DEL key".to_string())
                } else {
                    Command::Del(parts[1].to_string())
                }
            },
            
            // 列表命令
            "lpush" => {
                if parts.len() < 3 {
                    Command::Invalid("Usage: LPUSH key value".to_string())
                } else {
                    let key = parts[1].to_string();
                    let value = parts[2..].join(" ");
                    Command::LPush(key, value)
                }
            },
            "rpush" => {
                if parts.len() < 3 {
                    Command::Invalid("Usage: RPUSH key value".to_string())
                } else {
                    let key = parts[1].to_string();
                    let value = parts[2..].join(" ");
                    Command::RPush(key, value)
                }
            },
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
            },
            "len" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: LEN key".to_string())
                } else {
                    Command::Len(parts[1].to_string())
                }
            },
            "lpop" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: LPOP key".to_string())
                } else {
                    Command::LPop(parts[1].to_string())
                }
            },
            "rpop" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: RPOP key".to_string())
                } else {
                    Command::RPop(parts[1].to_string())
                }
            },
            "ldel" => {
                if parts.len() != 2 {
                    Command::Invalid("Usage: LDEL key".to_string())
                } else {
                    Command::LDel(parts[1].to_string())
                }
            },
            
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
            },
            "hget" => {
                if parts.len() != 3 {
                    Command::Invalid("Usage: HGET key field".to_string())
                } else {
                    Command::HGet(parts[1].to_string(), parts[2].to_string())
                }
            },
            "hdel" => {
                if parts.len() == 2 {
                    Command::HDelKey(parts[1].to_string())
                } else if parts.len() == 3 {
                    Command::HDel(parts[1].to_string(), parts[2].to_string())
                } else {
                    Command::Invalid("Usage: HDEL key [field]".to_string())
                }
            },
            
            // 其他命令
            "ping" => Command::Ping,
            "help" => {
                if parts.len() == 1 {
                    Command::Help
                } else {
                    Command::HelpCommand(parts[1].to_string())
                }
            },
            _ => Command::Invalid(format!("Unknown command: {}", parts[0])),
        }
    }
    
    // 执行命令
    pub fn execute_command(&self, command: Command) -> String {
        match command {
            // 字符串命令
            Command::Set(key, value) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    store.set_string(key, value)
                };
                self.persist_data();
                result
            },
            Command::Get(key) => {
                let store_guard = self.store_manager.get_store();
                // 需要可变引用以更新访问统计
                let mut store = store_guard.lock().unwrap();
                store.get_string(&key).unwrap_or_else(|| "(nil)".to_string())
            },
            Command::Del(key) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    if store.del_key(&key) {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                };
                self.persist_data();
                result
            },
            
            // 列表命令
            Command::LPush(key, value) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    store.lpush(key, value).to_string()
                };
                self.persist_data();
                result
            },
            Command::RPush(key, value) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    store.rpush(key, value).to_string()
                };
                self.persist_data();
                result
            },
            Command::Range(key, start, end) => {
                let store_guard = self.store_manager.get_store();
                // 需要可变引用以更新访问统计
                let mut store = store_guard.lock().unwrap();
                let values = store.range(&key, start, end);
                if values.is_empty() {
                    "(empty list)".to_string()
                } else {
                    values.join("\n")
                }
            },
            Command::Len(key) => {
                let store_guard = self.store_manager.get_store();
                // 需要可变引用以更新访问统计
                let mut store = store_guard.lock().unwrap();
                store.llen(&key).to_string()
            },
            Command::LPop(key) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    store.lpop(&key).unwrap_or_else(|| "(nil)".to_string())
                };
                self.persist_data();
                result
            },
            Command::RPop(key) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    store.rpop(&key).unwrap_or_else(|| "(nil)".to_string())
                };
                self.persist_data();
                result
            },
            Command::LDel(key) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    if store.ldel(&key) {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                };
                self.persist_data();
                result
            },
            
            // 哈希命令
            Command::HSet(key, field, value) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    if store.hset(key, field, value) {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                };
                self.persist_data();
                result
            },
            Command::HGet(key, field) => {
                let store_guard = self.store_manager.get_store();
                // 需要可变引用以更新访问统计
                let mut store = store_guard.lock().unwrap();
                store.hget(&key, &field).unwrap_or_else(|| "(nil)".to_string())
            },
            Command::HDel(key, field) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    if store.hdel_field(&key, &field) {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                };
                self.persist_data();
                result
            },
            Command::HDelKey(key) => {
                let result = {
                    let store_guard = self.store_manager.get_store();
                    let mut store = store_guard.lock().unwrap();
                    if store.hdel_key(&key) {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                };
                self.persist_data();
                result
            },
            
            // 其他命令
            Command::Ping => "PONG".to_string(),
            Command::Help => self.get_help(),
            Command::HelpCommand(cmd) => self.get_command_help(&cmd),
            Command::Invalid(msg) => format!("ERROR: {}", msg),
        }
    }
    
    // 持久化数据
    fn persist_data(&self) {
        if let Err(e) = self.store_manager.save_to_file(&self.data_file) {
            error!("Failed to persist data: {}", e);
        } else {
            debug!("Data persisted to {}", self.data_file);
        }
    }
    
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
            "hdel" => "hdel [key] [field] - 删除哈希表字段\nhdel [key] - 删除整个哈希表".to_string(),
            "ping" => "ping - 测试服务器连接".to_string(),
            "help" => "help - 获取所有命令帮助\nhelp [command] - 获取特定命令帮助".to_string(),
            _ => format!("Unknown command: {}", command),
        }
    }
}