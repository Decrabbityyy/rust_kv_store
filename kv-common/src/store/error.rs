use std::fmt;

/// 存储操作错误类型
#[derive(Debug, Clone)]
pub enum StoreError {
    /// 键不存在
    KeyNotFound(String),
    /// 类型不匹配
    TypeMismatch { key: String, expected: String, found: String },
    /// 序列化错误
    SerializationError(String),
    /// 反序列化错误
    DeserializationError(String),
    /// 文件IO错误
    IoError(String),
    /// 内存不足
    OutOfMemory,
    /// 键已过期
    KeyExpired(String),
    /// 事务错误
    TransactionError(String),
    /// WAL错误
    WalError(String),
    /// 配置错误
    ConfigError(String),
    /// 通用错误
    General(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::KeyNotFound(key) => write!(f, "键 '{}' 不存在", key),
            StoreError::TypeMismatch { key, expected, found } => {
                write!(f, "键 '{}' 类型不匹配: 期望 {}, 实际 {}", key, expected, found)
            }
            StoreError::SerializationError(msg) => write!(f, "序列化错误: {}", msg),
            StoreError::DeserializationError(msg) => write!(f, "反序列化错误: {}", msg),
            StoreError::IoError(msg) => write!(f, "IO错误: {}", msg),
            StoreError::OutOfMemory => write!(f, "内存不足"),
            StoreError::KeyExpired(key) => write!(f, "键 '{}' 已过期", key),
            StoreError::TransactionError(msg) => write!(f, "事务错误: {}", msg),
            StoreError::WalError(msg) => write!(f, "WAL错误: {}", msg),
            StoreError::ConfigError(msg) => write!(f, "配置错误: {}", msg),
            StoreError::General(msg) => write!(f, "错误: {}", msg),
        }
    }
}

impl std::error::Error for StoreError {}

/// 存储操作结果类型
pub type StoreResult<T> = Result<T, StoreError>;

/// 将标准错误转换为存储错误的辅助函数
impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        StoreError::IoError(error.to_string())
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        StoreError::SerializationError(error.to_string())
    }
}
