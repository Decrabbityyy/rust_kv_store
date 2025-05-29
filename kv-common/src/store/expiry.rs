use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use super::error::StoreResult;

/// 过期时间管理器
#[derive(Debug, Clone)]
pub struct ExpiryManager {
    expire_times: HashMap<String, u64>, // 键过期时间 (Unix时间戳)
}

impl ExpiryManager {
    pub fn new() -> Self {
        Self {
            expire_times: HashMap::new(),
        }
    }

    /// 从现有的过期时间映射创建管理器
    pub fn from_map(expire_times: HashMap<String, u64>) -> Self {
        Self { expire_times }
    }

    /// 获取当前时间戳
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// 设置键的过期时间
    pub fn set_expire(&mut self, key: &str, seconds: u64) -> StoreResult<()> {
        let expire_time = Self::current_timestamp() + seconds;
        self.expire_times.insert(key.to_string(), expire_time);
        Ok(())
    }

    /// 设置键的绝对过期时间
    pub fn set_expire_at(&mut self, key: &str, timestamp: u64) -> StoreResult<()> {
        self.expire_times.insert(key.to_string(), timestamp);
        Ok(())
    }

    /// 检查键是否已过期
    pub fn is_expired(&self, key: &str) -> bool {
        if let Some(expire_time) = self.expire_times.get(key) {
            Self::current_timestamp() >= *expire_time
        } else {
            false
        }
    }

    /// 获取键的剩余生存时间（秒）
    pub fn get_ttl(&self, key: &str) -> i64 {
        if let Some(expire_time) = self.expire_times.get(key) {
            let current_time = Self::current_timestamp();
            if current_time >= *expire_time {
                -2 // 已过期
            } else {
                (*expire_time - current_time) as i64
            }
        } else {
            -1 // 永不过期
        }
    }

    /// 移除键的过期时间
    pub fn persist(&mut self, key: &str) -> bool {
        self.expire_times.remove(key).is_some()
    }

    /// 移除键的过期时间 (别名方法)
    pub fn remove_expire(&mut self, key: &str) -> bool {
        self.persist(key)
    }

    /// 清理所有过期的键，返回过期的键列表
    pub fn find_expired_keys(&self) -> Vec<String> {
        let current_time = Self::current_timestamp();
        
        self.expire_times
            .iter()
            .filter_map(|(key, expire_time)| {
                if current_time >= *expire_time {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// 批量移除过期的键
    pub fn remove_expired_keys(&mut self, expired_keys: &[String]) {
        for key in expired_keys {
            self.expire_times.remove(key);
        }
    }

    /// 检查并返回需要清理的过期键数量
    pub fn count_expired_keys(&self) -> usize {
        let current_time = Self::current_timestamp();
        
        self.expire_times
            .values()
            .filter(|&&expire_time| current_time >= expire_time)
            .count()
    }

    /// 删除键的过期设置
    pub fn remove_key(&mut self, key: &str) {
        self.expire_times.remove(key);
    }

    /// 检查键是否设置了过期时间
    pub fn has_expiry(&self, key: &str) -> bool {
        self.expire_times.contains_key(key)
    }

    /// 获取所有有过期时间的键
    pub fn get_keys_with_expiry(&self) -> Vec<String> {
        self.expire_times.keys().cloned().collect()
    }

    /// 获取即将过期的键（在指定秒数内过期）
    pub fn get_expiring_soon(&self, within_seconds: u64) -> Vec<String> {
        let current_time = Self::current_timestamp();
        let threshold = current_time + within_seconds;
        
        self.expire_times
            .iter()
            .filter_map(|(key, expire_time)| {
                if *expire_time <= threshold && *expire_time > current_time {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// 获取过期时间统计信息
    pub fn get_expiry_stats(&self) -> ExpiryStats {
        let current_time = Self::current_timestamp();
        let mut expired_count = 0;
        let mut expiring_soon_count = 0; // 1小时内过期
        let total_with_expiry = self.expire_times.len();
        
        let one_hour = 3600; // 1小时的秒数
        
        for expire_time in self.expire_times.values() {
            if current_time >= *expire_time {
                expired_count += 1;
            } else if *expire_time <= current_time + one_hour {
                expiring_soon_count += 1;
            }
        }
        
        ExpiryStats {
            total_with_expiry,
            expired_count,
            expiring_soon_count,
            current_timestamp: current_time,
        }
    }

    /// 导出过期时间映射（用于序列化）
    pub fn export_expire_times(&self) -> &HashMap<String, u64> {
        &self.expire_times
    }

    /// 导入过期时间映射（用于反序列化）
    pub fn import_expire_times(&mut self, expire_times: HashMap<String, u64>) {
        self.expire_times = expire_times;
    }

    /// 清空所有过期时间设置
    pub fn clear(&mut self) {
        self.expire_times.clear();
    }

    /// 重命名键的过期时间设置
    pub fn rename_key(&mut self, old_key: &str, new_key: &str) -> bool {
        if let Some(expire_time) = self.expire_times.remove(old_key) {
            self.expire_times.insert(new_key.to_string(), expire_time);
            true
        } else {
            false
        }
    }
}

impl Default for ExpiryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 过期时间统计信息
#[derive(Debug, Clone)]
pub struct ExpiryStats {
    pub total_with_expiry: usize,    // 设置了过期时间的键总数
    pub expired_count: usize,        // 已过期的键数量
    pub expiring_soon_count: usize,  // 即将过期的键数量（1小时内）
    pub current_timestamp: u64,      // 当前时间戳
}

impl std::fmt::Display for ExpiryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "过期时间统计:")?;
        writeln!(f, "  设置过期时间的键: {}", self.total_with_expiry)?;
        writeln!(f, "  已过期的键: {}", self.expired_count)?;
        writeln!(f, "  即将过期的键(1小时内): {}", self.expiring_soon_count)?;
        writeln!(f, "  当前时间戳: {}", self.current_timestamp)?;
        Ok(())
    }
}
