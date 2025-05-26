use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::fs;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceMode {
    None,
    OnChange,
    Interval,
}
#[derive(Debug, Deserialize)]
pub struct PersistenceConfig {
    pub data_file: String,
    pub mode: PersistenceMode,
    pub interval_seconds: u64,
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub enable_default_expiry: bool,
    pub default_expiry_seconds: i64,
}

#[derive(Debug, Deserialize)]
pub struct MemoryConfig {
    pub enable_memory_optimization: bool,
    pub low_frequency_check_interval: u64,     // 秒
    pub access_threshold: u64,                // 访问次数阈值
    pub idle_time_threshold: u64,             // 闲置时间阈值(秒)
    pub max_memory_keys: usize,               // 内存中保留的最大键数
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub log_file: String,
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub server: ServerConfig,
    pub persistence: PersistenceConfig,
    pub storage: StorageConfig,
    pub memory: MemoryConfig,
    pub logging: LoggingConfig,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let config_dir = "config";
        let default_config_path = Path::new(config_dir).join("default.toml");

        // 确保配置目录存在
        if !Path::new(config_dir).exists() {
            fs::create_dir_all(config_dir).map_err(|e| {
                ConfigError::Message(format!("无法创建配置目录: {}", e))
            })?;
        }

        // 检查配置文件是否存在，如果不存在则创建默认配置
        if !default_config_path.exists() {
            let default_config_content = r#"[server]
# 服务器监听端口
port = 6379
# 服务器IP地址
host = "127.0.0.1"

[persistence]
# 数据持久化文件路径
data_file = "data/storage.dat"
# 持久化方式: "none", "on_change", "interval"
mode = "on_change"
# 定时持久化的时间间隔(秒)，仅当mode为interval时有效
interval_seconds = 300

[storage]
# 是否默认启用键过期
enable_default_expiry = false
# 默认键过期时间(秒)
default_expiry_seconds = 3600

[memory]
# 是否启用内存优化
enable_memory_optimization = true
# 低频检查时间间隔(秒)
low_frequency_check_interval = 60
# 访问次数阈值
access_threshold = 100
# 闲置时间阈值(秒)
idle_time_threshold = 600
# 内存中保留的最大键数
max_memory_keys = 1000

[logging]
# 日志文件路径
log_file = "logs/server.log"
# 日志级别: "error", "warn", "info", "debug", "trace"
level = "info"
"#;
            let mut file = fs::File::create(&default_config_path).map_err(|e| {
                ConfigError::Message(format!("无法创建配置文件: {}", e))
            })?;
            
            file.write_all(default_config_content.as_bytes()).map_err(|e| {
                ConfigError::Message(format!("无法写入配置文件: {}", e))
            })?;
        }

        let settings = Config::builder()
            .add_source(File::from(default_config_path))
            .build()?;

        settings.try_deserialize()
    }
}