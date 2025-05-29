use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

use crate::config::Settings;
use super::data_types::DataType;
use super::metadata::{DataMetadata, MemoryPressure};
use super::memory::{MemoryManager, OptimizationStats, OptimizationStrategy};
use super::expiry::{ExpiryManager, ExpiryStats};
use super::error::{StoreError, StoreResult};
use super::traits::*;
use super::string_ops::StringHandler;
use super::list_ops::ListHandler;
use super::hash_ops::HashHandler;
use super::set_ops::SetHandler;

/// 重构后的核心存储结构
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Store {
    pub(crate) data: HashMap<String, DataType>,
    #[serde(skip)]
    metadata: HashMap<String, DataMetadata>,
    #[serde(skip)]
    pub(crate) disk_keys: BTreeMap<String, bool>, // 记录存储在磁盘上的键
    #[serde(skip)]
    memory_pressure: MemoryPressure, // 内存压力监控
    #[serde(skip)]
    expiry_manager: ExpiryManager, // 过期时间管理
    #[serde(skip)]
    memory_manager: Option<MemoryManager>, // 内存管理器
    #[serde(skip)]
    settings: Option<Arc<Settings>>, // 配置引用
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            metadata: HashMap::new(),
            disk_keys: BTreeMap::new(),
            memory_pressure: MemoryPressure::new(),
            expiry_manager: ExpiryManager::new(),
            memory_manager: None,
            settings: None,
        }
    }

    /// 设置配置
    pub fn with_settings(mut self, settings: Arc<Settings>) -> Self {
        self.settings = Some(settings);
        self
    }

    /// 设置内存管理器
    pub fn with_memory_manager(mut self, memory_manager: MemoryManager) -> Self {
        self.memory_manager = Some(memory_manager);
        self
    }

    /// 应用默认过期时间
    fn apply_default_expiry(&mut self, key: &str) {
        if let Some(settings) = &self.settings {
            if settings.storage.enable_default_expiry {
                let default_ttl = settings.storage.default_expiry_seconds as u64;
                let _ = self.expiry_manager.set_expire(key, default_ttl);
            }
        }
    }

    /// 记录访问统计
    fn record_access(&mut self, key: &str) {
        // 更新元数据
        self.metadata
            .entry(key.to_string())
            .or_insert_with(|| DataMetadata::new(0))
            .access();

        // 更新内存压力统计
        if self.data.contains_key(key) {
            self.memory_pressure.record_cache_hit();
        } else if self.disk_keys.contains_key(key) {
            self.memory_pressure.record_cache_miss();
        }
    }

    /// 记录数据修改
    fn record_modification(&mut self, key: &str, new_size: usize) {
        self.metadata
            .entry(key.to_string())
            .or_insert_with(|| DataMetadata::new(new_size))
            .modify(new_size);
    }

    /// 清理过期键
    pub fn clean_expired_keys(&mut self) -> usize {
        let expired_keys = self.expiry_manager.find_expired_keys();
        let count = expired_keys.len();

        for key in &expired_keys {
            self.data.remove(key);
            self.metadata.remove(key);
            self.disk_keys.remove(key);
        }

        self.expiry_manager.remove_expired_keys(&expired_keys);
        count
    }

    /// 检查内存优化需求
    pub fn should_optimize_memory(&self) -> bool {
        if let Some(memory_manager) = &self.memory_manager {
            memory_manager.should_optimize(&self.memory_pressure, self.data.len())
        } else {
            false
        }
    }

    /// 执行内存优化
    pub fn optimize_memory(&mut self) -> StoreResult<usize> {
        if let Some(memory_manager) = &self.memory_manager {
            let low_freq_keys = memory_manager.get_low_frequency_keys(&self.data, &self.metadata);
            let count = low_freq_keys.len();
            
            for key in &low_freq_keys {
                self.mark_as_disk_stored(key);
            }
            
            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// 标记键为磁盘存储
    pub fn mark_as_disk_stored(&mut self, key: &str) {
        if self.data.contains_key(key) {
            self.disk_keys.insert(key.to_string(), true);
            self.data.remove(key);
            self.memory_pressure.record_offload();
        }
    }

    /// 获取优化统计信息
    pub fn get_optimization_stats(&self) -> OptimizationStats {
        let memory_usage = MemoryManager::calculate_memory_usage(&self.data);
        
        let (strategy, max_memory_keys, access_threshold, idle_time_threshold) = 
            if let Some(memory_manager) = &self.memory_manager {
                let pressure_level = self.memory_pressure.calculate_pressure_level(
                    self.data.len(),
                    memory_manager.max_memory_keys,
                );
                let strategy = memory_manager.select_optimization_strategy(
                    pressure_level,
                    self.memory_pressure.cache_hit_ratio(),
                );
                (strategy, memory_manager.max_memory_keys, memory_manager.access_threshold, memory_manager.idle_time_threshold)
            } else {
                (OptimizationStrategy::None, 0, 0, 0)
            };

        OptimizationStats {
            memory_keys_count: self.data.len(),
            disk_keys_count: self.disk_keys.len(),
            total_keys_count: self.data.len() + self.disk_keys.len(),
            memory_optimization_enabled: self.memory_manager.is_some(),
            max_memory_keys,
            access_threshold,
            idle_time_threshold,
            memory_pressure_level: self.memory_pressure.last_pressure_level,
            cache_hit_ratio: self.memory_pressure.cache_hit_ratio(),
            memory_usage_bytes: memory_usage,
            optimization_strategy: strategy,
        }
    }

    /// 获取过期统计信息
    pub fn get_expiry_stats(&self) -> ExpiryStats {
        self.expiry_manager.get_expiry_stats()
    }

    /// 序列化单个键的数据
    pub fn serialize_key(&self, key: &str) -> StoreResult<Option<String>> {
        if !self.data.contains_key(key) {
            return Ok(None);
        }
        
        match self.data.get(key) {
            Some(value) => {
                let encoded = serde_json::to_string(value)?;
                Ok(Some(encoded))
            },
            None => Ok(None),
        }
    }
    
    /// 反序列化单个键的数据
    pub fn deserialize_key(&mut self, key: &str, data: &str) -> StoreResult<()> {
        let value: DataType = serde_json::from_str(data)?;
        let size = value.estimated_size();
        
        self.data.insert(key.to_string(), value);
        self.disk_keys.remove(key);
        self.record_modification(key, size);
        self.memory_pressure.record_load();
        
        Ok(())
    }

    /// 序列化整个存储
    pub fn serialize(&self) -> StoreResult<String> {
        let serialized = serde_json::to_string(self)?;
        Ok(serialized)
    }
    
    /// 反序列化整个存储
    pub fn deserialize(&mut self, data: &str) -> StoreResult<()> {
        let store: Store = serde_json::from_str(data)?;
        self.data = store.data;
        // 重新构建元数据
        for (key, value) in &self.data {
            let metadata = DataMetadata::new(value.estimated_size());
            self.metadata.insert(key.clone(), metadata);
        }
        Ok(())
    }

    /// 获取所有键
    pub fn get_all_keys(&self) -> Vec<String> {
        let mut all_keys: Vec<String> = self.data.keys().cloned().collect();
        all_keys.extend(self.disk_keys.keys().cloned());
        all_keys
    }

    /// 获取磁盘键
    pub fn get_disk_keys(&self) -> Vec<String> {
        self.disk_keys.keys().cloned().collect()
    }

    /// 获取内存键
    pub fn get_memory_keys(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }
}

// 实现存储操作 trait
impl StoreOperations for Store {
    fn exists(&self, key: &str) -> bool {
        !self.expiry_manager.is_expired(key) && self.data.contains_key(key)
    }
    
    fn delete(&mut self, key: &str) -> StoreResult<bool> {
        let existed = self.data.remove(key).is_some();
        self.metadata.remove(key);
        self.disk_keys.remove(key);
        self.expiry_manager.remove_expire(key);
        Ok(existed)
    }
    
    fn get_type(&self, key: &str) -> StoreResult<String> {
        if self.expiry_manager.is_expired(key) {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }
        
        match self.data.get(key) {
            Some(DataType::String(_)) => Ok("string".to_string()),
            Some(DataType::List(_)) => Ok("list".to_string()),
            Some(DataType::Hash(_)) => Ok("hash".to_string()),
            Some(DataType::Set(_)) => Ok("set".to_string()),
            None => Err(StoreError::KeyNotFound(key.to_string())),
        }
    }
    
    fn is_expired(&self, key: &str) -> bool {
        self.expiry_manager.is_expired(key)
    }
    
    fn set_expire(&mut self, key: &str, seconds: u64) -> StoreResult<bool> {
        if !self.data.contains_key(key) {
            return Ok(false);
        }
        let _ = self.expiry_manager.set_expire(key, seconds);
        Ok(true)
    }
    
    fn get_ttl(&self, key: &str) -> StoreResult<i64> {
        if !self.data.contains_key(key) {
            return Ok(-2); // Key does not exist
        }
        Ok(self.expiry_manager.get_ttl(key))
    }
    
    fn persist_key(&mut self, key: &str) -> StoreResult<bool> {
        if !self.data.contains_key(key) {
            return Ok(false);
        }
        self.expiry_manager.remove_expire(key);
        Ok(true)
    }
}

// 实现字符串操作 trait
impl StringOperations for Store {
    fn set(&mut self, key: String, value: String) -> StoreResult<String> {
        self.set_string(key, value.clone());
        Ok(value)
    }
    
    fn get(&self, key: &str) -> StoreResult<Option<String>> {
        Ok(self.get_string(key))
    }
    
    fn append(&mut self, key: &str, value: &str) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(key) {
            self.delete(key)?;
        }
        
        self.record_access(key);
        let result = StringHandler::append_internal(&mut self.data, key, value)?;
        self.apply_default_expiry(key);
        Ok(result)
    }
    
    fn strlen(&self, key: &str) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(key) {
            return Ok(0);
        }
        StringHandler::strlen_internal(&self.data, key)
    }
}

// 实现列表操作 trait  
impl ListOperations for Store {
    fn lpush(&mut self, key: String, value: String) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(&key) {
            self.delete(&key)?;
        }
        
        self.record_access(&key);
        let result = ListHandler::lpush_internal(&mut self.data, key.clone(), value)?;
        self.apply_default_expiry(&key);
        Ok(result)
    }
    
    fn rpush(&mut self, key: String, value: String) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(&key) {
            self.delete(&key)?;
        }
        
        self.record_access(&key);
        let result = ListHandler::rpush_internal(&mut self.data, key.clone(), value)?;
        self.apply_default_expiry(&key);
        Ok(result)
    }
    
    fn lpop(&mut self, key: &str) -> StoreResult<Option<String>> {
        if self.expiry_manager.is_expired(key) {
            self.delete(key)?;
            return Ok(None);
        }
        
        self.record_access(key);
        ListHandler::lpop_internal(&mut self.data, key)
    }
    
    fn rpop(&mut self, key: &str) -> StoreResult<Option<String>> {
        if self.expiry_manager.is_expired(key) {
            self.delete(key)?;
            return Ok(None);
        }
        
        self.record_access(key);
        ListHandler::rpop_internal(&mut self.data, key)
    }
    
    fn lrange(&self, key: &str, start: isize, stop: isize) -> StoreResult<Vec<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(vec![]);
        }
        
        ListHandler::lrange_internal(&self.data, key, start, stop)
    }
    
    fn llen(&self, key: &str) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(key) {
            return Ok(0);
        }
        
        ListHandler::llen_internal(&self.data, key)
    }
    
    fn lindex(&self, key: &str, index: isize) -> StoreResult<Option<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(None);
        }
        
        ListHandler::lindex_internal(&self.data, key, index)
    }
    
    fn lset(&mut self, key: &str, index: isize, value: String) -> StoreResult<bool> {
        if self.expiry_manager.is_expired(key) {
            return Ok(false);
        }
        
        self.record_access(key);
        ListHandler::lset_internal(&mut self.data, key, index, value)
    }
}

// 实现哈希操作 trait
impl HashOperations for Store {
    fn hset(&mut self, key: String, field: String, value: String) -> StoreResult<bool> {
        if self.expiry_manager.is_expired(&key) {
            self.delete(&key)?;
        }
        
        self.record_access(&key);
        let result = HashHandler::hset_internal(&mut self.data, key.clone(), field.clone(), value)?;
        self.apply_default_expiry(&key);
        Ok(result)
    }
    
    fn hget(&self, key: &str, field: &str) -> StoreResult<Option<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(None);
        }
        
        HashHandler::hget_internal(&self.data, key, field)
    }
    
    fn hdel(&mut self, key: &str, field: &str) -> StoreResult<bool> {
        if self.expiry_manager.is_expired(key) {
            self.delete(key)?;
            return Ok(false);
        }
        
        self.record_access(key);
        HashHandler::hdel_internal(&mut self.data, key, field)
    }
    
    fn hkeys(&self, key: &str) -> StoreResult<Vec<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(vec![]);
        }
        
        HashHandler::hkeys_internal(&self.data, key)
    }
    
    fn hvals(&self, key: &str) -> StoreResult<Vec<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(vec![]);
        }
        
        HashHandler::hvals_internal(&self.data, key)
    }
    
    fn hgetall(&self, key: &str) -> StoreResult<Vec<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(vec![]);
        }
        
        let hash_map = HashHandler::hgetall_internal(&self.data, key)?;
        let mut result = Vec::new();
        for (field, value) in hash_map {
            result.push(field);
            result.push(value);
        }
        Ok(result)
    }
    
    fn hexists(&self, key: &str, field: &str) -> StoreResult<bool> {
        if self.expiry_manager.is_expired(key) {
            return Ok(false);
        }
        
        HashHandler::hexists_internal(&self.data, key, field)
    }
    
    fn hlen(&self, key: &str) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(key) {
            return Ok(0);
        }
        
        HashHandler::hlen_internal(&self.data, key)
    }
}

// 实现集合操作 trait
impl SetOperations for Store {
    fn sadd(&mut self, key: String, values: Vec<String>) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(&key) {
            self.delete(&key)?;
        }
        
        self.record_access(&key);
        let result = SetHandler::sadd_internal(&mut self.data, key.clone(), values)?;
        self.apply_default_expiry(&key);
        Ok(result)
    }
    
    fn srem(&mut self, key: &str, value: &str) -> StoreResult<bool> {
        if self.expiry_manager.is_expired(key) {
            self.delete(key)?;
            return Ok(false);
        }
        
        self.record_access(key);
        SetHandler::srem_internal(&mut self.data, key, value)
    }
    
    fn smembers(&self, key: &str) -> StoreResult<Vec<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(vec![]);
        }
        
        SetHandler::smembers_internal(&self.data, key)
    }
    
    fn sismember(&self, key: &str, value: &str) -> StoreResult<bool> {
        if self.expiry_manager.is_expired(key) {
            return Ok(false);
        }
        
        SetHandler::sismember_internal(&self.data, key, value)
    }
    
    fn scard(&self, key: &str) -> StoreResult<usize> {
        if self.expiry_manager.is_expired(key) {
            return Ok(0);
        }
        
        SetHandler::scard_internal(&self.data, key)
    }
    
    fn srandmember(&self, key: &str, count: Option<isize>) -> StoreResult<Vec<String>> {
        if self.expiry_manager.is_expired(key) {
            return Ok(vec![]);
        }
        
        SetHandler::srandmember_internal(&self.data, key, count)
    }
    
    fn spop(&mut self, key: &str, count: Option<usize>) -> StoreResult<Vec<String>> {
        if self.expiry_manager.is_expired(key) {
            self.delete(key)?;
            return Ok(vec![]);
        }
        
        self.record_access(key);
        SetHandler::spop_internal(&mut self.data, key, count)
    }
}

// 为 Store 添加一些需要的辅助方法
impl Store {
    /// 设置字符串值
    pub fn set_string(&mut self, key: String, value: String) {
        self.record_access(&key);
        self.data.insert(key.clone(), DataType::String(value.clone()));
        self.record_modification(&key, value.len());
        self.apply_default_expiry(&key);
    }
    
    /// 获取字符串值
    pub fn get_string(&self, key: &str) -> Option<String> {
        if self.expiry_manager.is_expired(key) {
            return None;
        }
        
        match self.data.get(key) {
            Some(DataType::String(value)) => Some(value.clone()),
            _ => None,
        }
    }
    
    /// 删除键（别名）
    pub fn del_key(&mut self, key: &str) -> bool {
        self.delete(key).unwrap_or(false)
    }
    
    /// 列表删除操作
    pub fn ldel(&mut self, key: &str) -> bool {
        self.delete(key).unwrap_or(false)
    }
    
    /// 哈希字段删除
    pub fn hdel_field(&mut self, key: &str, field: &str) -> bool {
        self.hdel(key, field).unwrap_or(false)
    }
    
    /// 哈希键删除
    pub fn hdel_key(&mut self, key: &str) -> bool {
        self.delete(key).unwrap_or(false)
    }
    
    /// 列表范围查询（别名）
    pub fn range(&self, key: &str, start: isize, stop: isize) -> Vec<String> {
        self.lrange(key, start, stop).unwrap_or_default()
    }
    
    /// 集合成员查询（别名）
    pub fn smember_query(&self, key: &str, value: &str) -> bool {
        self.sismember(key, value).unwrap_or(false)
    }
    
    /// 获取所有键值对
    pub fn get_all_key_values(&self) -> std::collections::HashMap<String, String> {
        let mut result = std::collections::HashMap::new();
        
        for (key, value) in &self.data {
            if !self.expiry_manager.is_expired(key) {
                match value {
                    DataType::String(s) => {
                        result.insert(key.clone(), s.clone());
                    },
                    DataType::List(list) => {
                        let serialized = serde_json::to_string(list).unwrap_or_default();
                        result.insert(key.clone(), serialized);
                    },
                    DataType::Hash(hash) => {
                        let serialized = serde_json::to_string(hash).unwrap_or_default();
                        result.insert(key.clone(), serialized);
                    },
                    DataType::Set(set) => {
                        let serialized = serde_json::to_string(set).unwrap_or_default();
                        result.insert(key.clone(), serialized);
                    }
                }
            }
        }
        
        result
    }
    
    /// 获取内存使用情况
    pub fn memory_usage(&self) -> usize {
        MemoryManager::calculate_memory_usage(&self.data)
    }
    
    /// 获取低频访问键
    pub fn get_low_frequency_keys(&self, count: usize) -> Vec<String> {
        if let Some(memory_manager) = &self.memory_manager {
            memory_manager.get_low_frequency_keys(&self.data, &self.metadata)
        } else {
            // 简单实现：按访问计数排序，返回访问次数最少的键
            let mut key_counts: Vec<(String, u64)> = self.metadata
                .iter()
                .filter(|(key, _)| self.data.contains_key(*key))
                .map(|(key, metadata)| (key.clone(), metadata.access_count))
                .collect();
            
            key_counts.sort_by_key(|(_, count)| *count);
            key_counts.into_iter()
                .take(count)
                .map(|(key, _)| key)
                .collect()
        }
    }
    
    /// 获取低频访问键 (兼容性方法 - 忽略额外参数)
    pub fn get_low_frequency_keys_compat(&self, count: usize, _threshold: u64, _idle_time: u64) -> Vec<String> {
        self.get_low_frequency_keys(count)
    }
    
    /// 兼容性方法：设置过期时间 (与测试兼容)
    pub fn expire(&mut self, key: &str, seconds: u64) -> bool {
        self.set_expire(key, seconds).unwrap_or(false)
    }
    
    /// 兼容性方法：获取TTL (与测试兼容)  
    pub fn ttl(&self, key: &str) -> i64 {
        self.get_ttl(key).unwrap_or(-1)
    }
    
    /// 兼容性方法：移除过期时间 (与测试兼容)
    pub fn persist(&mut self, key: &str) -> bool {
        self.persist_key(key).unwrap_or(false)
    }
}
