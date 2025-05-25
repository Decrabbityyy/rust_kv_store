use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use crate::config::Settings;
use base64::prelude::*;
// 存储系统中支持的数据类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    // 字符串类型
    String(String),
    // 双向链表类型
    List(VecDeque<String>),
    // 哈希类型
    Hash(HashMap<String, String>),
}

// 数据项元信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMetadata {
    pub last_access_time: u64,   // 最后访问时间（Unix时间戳）
    pub access_count: u64,       // 访问次数
}

impl Default for DataMetadata {
    fn default() -> Self {
        Self {
            last_access_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            access_count: 0,
        }
    }
}

impl DataMetadata {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn access(&mut self) {
        self.last_access_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.access_count += 1;
    }
}

// 内存压力监控结构
#[derive(Debug, Clone, Default)]
pub struct MemoryPressure {
    pub total_keys_processed: u64,       // 总处理的键数
    pub offload_operations: u64,         // 转移到磁盘操作次数
    pub load_operations: u64,            // 从磁盘加载操作次数
    pub cache_hits: u64,                 // 内存缓存命中次数
    pub cache_misses: u64,               // 内存缓存未命中次数
    pub last_pressure_level: u8,         // 上次内存压力等级 (0-10)
    pub last_adjustment_time: u64,       // 上次调整时间
}

impl MemoryPressure {
    pub fn new() -> Self {
        Self {
            total_keys_processed: 0,
            offload_operations: 0,
            load_operations: 0,
            cache_hits: 0,
            cache_misses: 0,
            last_pressure_level: 0,
            last_adjustment_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
    
    pub fn record_cache_hit(&mut self) {
        self.cache_hits += 1;
        self.total_keys_processed += 1;
    }
    
    pub fn record_cache_miss(&mut self) {
        self.cache_misses += 1;
        self.total_keys_processed += 1;
    }
    
    pub fn record_offload(&mut self) {
        self.offload_operations += 1;
    }
    
    pub fn record_load(&mut self) {
        self.load_operations += 1;
    }
    
    pub fn cache_hit_ratio(&self) -> f64 {
        if self.total_keys_processed == 0 {
            return 0.0;
        }
        self.cache_hits as f64 / self.total_keys_processed as f64
    }
    
    pub fn calculate_pressure_level(&self, memory_keys: usize, max_memory_keys: usize) -> u8 {
        let mut pressure = (memory_keys as f64 / max_memory_keys as f64 * 10.0) as u8;
        
        // 缓存命中率低表示压力大
        let hit_ratio = self.cache_hit_ratio();
        if hit_ratio < 0.7 {
            pressure = pressure.saturating_add(1);
        }
        if hit_ratio < 0.5 {
            pressure = pressure.saturating_add(1);
        }
        
        pressure.min(10)
    }
}

// 数据存储结构
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Store {
    data: HashMap<String, DataType>,
    #[serde(skip)]
    metadata: HashMap<String, DataMetadata>,
    #[serde(skip)]
    disk_keys: BTreeMap<String, bool>, // 记录存储在磁盘上的键
    #[serde(skip)]
    memory_pressure: MemoryPressure, // 内存压力监控
    #[serde(skip)]
    expire_times: HashMap<String, u64>, // 键过期时间 (Unix时间戳)
    #[serde(skip)]
    settings: Option<Arc<Settings>>, // 配置引用
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: HashMap::new(),
            metadata: HashMap::new(),
            disk_keys: BTreeMap::new(),
            memory_pressure: MemoryPressure::new(),
            expire_times: HashMap::new(),
            settings: None,
        }
    }

    // 设置配置
    pub fn with_settings(mut self, settings: Arc<Settings>) -> Self {
        self.settings = Some(settings);
        self
    }

    // 应用默认过期时间
    fn apply_default_expiry(&mut self, key: &str) {
        // 只有当配置存在且启用过期功能时才应用
        if let Some(settings) = &self.settings {
            if settings.storage.enable_expiry && settings.storage.default_expiry_seconds > 0 {
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                
                let expire_time = current_time + settings.storage.default_expiry_seconds as u64;
                self.expire_times.insert(key.to_string(), expire_time);
            }
        }
    }

    // 记录访问统计
    fn record_access(&mut self, key: &str) {
        // 首先检查键是否已过期
        if self.is_expired(key) {
            self.del_key(key);
            return;
        }
        
        if let Some(meta) = self.metadata.get_mut(key) {
            meta.access();
        } else {
            self.metadata.insert(key.to_string(), DataMetadata::new());
        }
    }

    // 检查键是否已过期
    fn is_expired(&self, key: &str) -> bool {
        if let Some(expire_time) = self.expire_times.get(key) {
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            return current_time > *expire_time;
        }
        false
    }

    // 设置键的过期时间
    pub fn expire(&mut self, key: &str, seconds: u64) -> bool {
        // 如果键不存在，返回 false
        if !self.data.contains_key(key) && !self.disk_keys.contains_key(key) {
            return false;
        }
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let expire_time = current_time + seconds;
        self.expire_times.insert(key.to_string(), expire_time);
        true
    }

    // 获取键的剩余生存时间（秒）
    pub fn ttl(&self, key: &str) -> i64 {
        // 如果键不存在，返回 -2
        if !self.data.contains_key(key) && !self.disk_keys.contains_key(key) {
            return -2;
        }
        
        // 如果键存在但没有设置过期时间，返回 -1
        if let Some(expire_time) = self.expire_times.get(key) {
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            
            if current_time > *expire_time {
                return 0; // 已过期
            }
            
            return (*expire_time - current_time) as i64;
        }
        
        -1 // 存在但永不过期
    }

    // 移除键的过期时间
    pub fn persist(&mut self, key: &str) -> bool {
        self.expire_times.remove(key).is_some()
    }

    // 清理所有过期的键
    pub fn clean_expired_keys(&mut self) -> usize {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let expired_keys: Vec<String> = self.expire_times.iter()
            .filter(|(_, &expire_time)| current_time > expire_time)
            .map(|(key, _)| key.clone())
            .collect();
        
        let count = expired_keys.len();
        
        for key in expired_keys {
            self.del_key(&key);
            self.expire_times.remove(&key);
        }
        
        count
    }

    // 设置字符串值
    pub fn set_string(&mut self, key: String, value: String) -> String {
        // 检查值中是否包含 EX 参数
        let parts: Vec<&str> = value.split(" EX ").collect();
        let actual_value = parts[0].to_string();
        
        // 如果有 EX 参数，则设置过期时间
        if parts.len() > 1 {
            if let Ok(seconds) = parts[1].parse::<u64>() {
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                
                let expire_time = current_time + seconds;
                self.expire_times.insert(key.clone(), expire_time);
            }
        } else {
            // 如果没有指定过期时间，应用默认过期时间
            self.apply_default_expiry(&key);
        }
        
        // 记录访问
        self.record_access(&key);
        
        // 检查数据类型并设置值
        if let Some(data_type) = self.data.get_mut(&key) {
            match data_type {
                DataType::String(s) => {
                    *s = actual_value.clone();
                    "OK".to_string()
                },
                _ => {
                    "ERROR: Key exists but is not a string type".to_string()
                }
            }
        } else {
            // 新键，直接设置
            self.data.insert(key, DataType::String(actual_value));
            "OK".to_string()
        }
    }

    pub fn get_string(&mut self, key: &str) -> Option<String> {
        self.record_access(key);
        match self.data.get(key) {
            Some(DataType::String(value)) => Some(value.clone()),
            _ => None,
        }
    }

    pub fn del_key(&mut self, key: &str) -> bool {
        self.metadata.remove(key);
        self.disk_keys.remove(key);
        self.expire_times.remove(key);
        self.data.remove(key).is_some()
    }

    // 双向链表操作
    pub fn lpush(&mut self, key: String, value: String) -> usize {
        self.record_access(&key);
        // 如果键在磁盘上，先移除标记
        self.disk_keys.remove(&key);
        
        // 如果键是新的，应用默认过期时间
        if !self.data.contains_key(&key) && !self.expire_times.contains_key(&key) {
            self.apply_default_expiry(&key);
        }
        
        match self.data.get_mut(&key) {
            Some(DataType::List(list)) => {
                list.push_front(value);
                list.len()
            }
            Some(_) => {
                // 已存在但类型不是列表，替换为新的列表
                let mut list = VecDeque::new();
                list.push_front(value);
                self.data.insert(key, DataType::List(list));
                1
            }
            None => {
                // 不存在，创建新列表
                let mut list = VecDeque::new();
                list.push_front(value);
                self.data.insert(key, DataType::List(list));
                1
            }
        }
    }

    pub fn rpush(&mut self, key: String, value: String) -> usize {
        self.record_access(&key);
        self.disk_keys.remove(&key);

        // 如果键是新的，应用默认过期时间
        if !self.data.contains_key(&key) && !self.expire_times.contains_key(&key) {
            self.apply_default_expiry(&key);
        }

        match self.data.get_mut(&key) {
            Some(DataType::List(list)) => {
                list.push_back(value);
                list.len()
            }
            Some(_) => {
                let mut list = VecDeque::new();
                list.push_back(value);
                self.data.insert(key, DataType::List(list));
                1
            }
            None => {
                let mut list = VecDeque::new();
                list.push_back(value);
                self.data.insert(key, DataType::List(list));
                1
            }
        }
    }

    pub fn range(&mut self, key: &str, start: isize, end: isize) -> Vec<String> {
        self.record_access(key);
        match self.data.get(key) {
            Some(DataType::List(list)) => {
                let len = list.len() as isize;
                if len == 0 {
                    return vec![];
                }

                let start_idx = if start < 0 { (len + start).max(0) } else { start.min(len - 1) } as usize;
                let end_idx = if end < 0 { (len + end).max(0) } else { end.min(len - 1) } as usize;

                if start_idx > end_idx {
                    return vec![];
                }

                list.iter().skip(start_idx).take(end_idx - start_idx + 1).cloned().collect()
            }
            _ => vec![],
        }
    }

    pub fn llen(&mut self, key: &str) -> usize {
        self.record_access(key);
        match self.data.get(key) {
            Some(DataType::List(list)) => list.len(),
            _ => 0,
        }
    }

    pub fn lpop(&mut self, key: &str) -> Option<String> {
        self.record_access(key);
        match self.data.get_mut(key) {
            Some(DataType::List(list)) => list.pop_front(),
            _ => None,
        }
    }

    pub fn rpop(&mut self, key: &str) -> Option<String> {
        self.record_access(key);
        match self.data.get_mut(key) {
            Some(DataType::List(list)) => list.pop_back(),
            _ => None,
        }
    }

    pub fn ldel(&mut self, key: &str) -> bool {
        self.metadata.remove(key);
        self.disk_keys.remove(key);
        self.expire_times.remove(key);
        match self.data.get(key) {
            Some(DataType::List(_)) => self.data.remove(key).is_some(),
            _ => false,
        }
    }

    // 哈希表操作
    pub fn hset(&mut self, key: String, field: String, value: String) -> bool {
        self.record_access(&key);
        self.disk_keys.remove(&key);

        match self.data.get_mut(&key) {
            Some(DataType::Hash(hash)) => {
                let is_new = !hash.contains_key(&field);
                hash.insert(field, value);
                is_new
            }
            Some(_) => {
                let mut hash = HashMap::new();
                hash.insert(field, value);
                self.data.insert(key, DataType::Hash(hash));
                true
            }
            None => {
                let mut hash = HashMap::new();
                hash.insert(field, value);
                self.data.insert(key, DataType::Hash(hash));
                true
            }
        }
    }

    pub fn hget(&mut self, key: &str, field: &str) -> Option<String> {
        self.record_access(key);
        match self.data.get(key) {
            Some(DataType::Hash(hash)) => hash.get(field).cloned(),
            _ => None,
        }
    }

    pub fn hdel_field(&mut self, key: &str, field: &str) -> bool {
        self.record_access(key);
        match self.data.get_mut(key) {
            Some(DataType::Hash(hash)) => hash.remove(field).is_some(),
            _ => false,
        }
    }

    pub fn hdel_key(&mut self, key: &str) -> bool {
        self.metadata.remove(key);
        self.disk_keys.remove(key);
        self.expire_times.remove(key);
        match self.data.get(key) {
            Some(DataType::Hash(_)) => self.data.remove(key).is_some(),
            _ => false,
        }
    }

    // 低频数据管理方法
    pub fn get_low_frequency_keys(&self, access_threshold: u64, idle_time_threshold: u64, max_memory_keys: usize) -> Vec<String> {
        if self.data.len() <= max_memory_keys {
            return vec![];
        }
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        let mut candidates: Vec<(String, &DataMetadata)> = self.metadata.iter()
            .filter(|(key, meta)| {
                self.data.contains_key(*key) && 
                (meta.access_count < access_threshold || 
                 (current_time - meta.last_access_time) > idle_time_threshold)
            })
            .map(|(key, meta)| (key.clone(), meta))
            .collect();
            
        candidates.sort_by(|a, b| {
            let count_cmp = a.1.access_count.cmp(&b.1.access_count);
            if count_cmp != std::cmp::Ordering::Equal {
                return count_cmp;
            }
            a.1.last_access_time.cmp(&b.1.last_access_time)
        });
        
        let keys_to_remove = (self.data.len() - max_memory_keys).min(candidates.len());
        
        candidates.into_iter()
            .take(keys_to_remove)
            .map(|(key, _)| key)
            .collect()
    }
    
    pub fn mark_as_disk_stored(&mut self, key: &str) {
        if self.data.contains_key(key) {
            self.disk_keys.insert(key.to_string(), true);
            self.data.remove(key);
        }
    }
    
    pub fn key_exists(&self, key: &str) -> bool {
        self.data.contains_key(key) || self.disk_keys.contains_key(key)
    }
    
    pub fn get_all_keys(&self) -> Vec<String> {
        let mut all_keys: Vec<String> = self.data.keys().cloned().collect();
        all_keys.extend(self.disk_keys.keys().cloned());
        all_keys
    }
    
    pub fn get_disk_keys(&self) -> Vec<String> {
        self.disk_keys.keys().cloned().collect()
    }

    // 持久化相关方法
    pub fn serialize(&self) -> Result<String, String> {
        serde_json::to_string(&self.data)
            .map_err(|e| format!("序列化错误: {}", e))
    }
    
    pub fn serialize_key(&self, key: &str) -> Result<Option<String>, String> {
        match self.data.get(key) {
            Some(value) => {
                let mut single_data = HashMap::new();
                single_data.insert(key.to_string(), value.clone());
                serde_json::to_string(&single_data)
                    .map(Some)
                    .map_err(|e| format!("序列化单个键错误: {}", e))
            },
            None => Ok(None)
        }
    }

    pub fn deserialize(&mut self, data: &str) -> Result<(), String> {
        match serde_json::from_str::<HashMap<String, DataType>>(data) {
            Ok(parsed_data) => {
                self.data = parsed_data;
                for key in self.data.keys() {
                    if !self.metadata.contains_key(key) {
                        self.metadata.insert(key.clone(), DataMetadata::new());
                    }
                }
                Ok(())
            }
            Err(e) => Err(format!("反序列化错误: {}", e)),
        }
    }
    
    pub fn deserialize_key(&mut self, key: &str, data: &str) -> Result<(), String> {
        match serde_json::from_str::<HashMap<String, DataType>>(data) {
            Ok(parsed_data) => {
                if let Some(value) = parsed_data.get(key) {
                    self.data.insert(key.to_string(), value.clone());
                    if !self.metadata.contains_key(key) {
                        self.metadata.insert(key.to_string(), DataMetadata::new());
                    }
                    self.disk_keys.remove(key);
                    Ok(())
                } else {
                    Err(format!("反序列化的数据中不包含键: {}", key))
                }
            }
            Err(e) => Err(format!("反序列化单个键错误: {}", e)),
        }
    }
}

// 线程安全的存储管理器
#[derive(Debug, Clone)]
pub struct StoreManager {
    store: Arc<Mutex<Store>>,
    disk_base_path: String,
    enable_memory_optimization: bool,
    low_frequency_check_interval: u64,
    access_threshold: u64,
    idle_time_threshold: u64,
    max_memory_keys: usize,
    last_check_time: Arc<Mutex<Instant>>,
    settings: Option<Arc<Settings>>, // 新增: 存储配置引用
}

impl Default for StoreManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreManager {
    pub fn new() -> Self {
        StoreManager {
            store: Arc::new(Mutex::new(Store::new())),
            disk_base_path: "data/low_freq".to_string(),
            enable_memory_optimization: false,
            low_frequency_check_interval: 300,
            access_threshold: 5,
            idle_time_threshold: 3600,
            max_memory_keys: 1000,
            last_check_time: Arc::new(Mutex::new(Instant::now())),
            settings: None, // 新增
        }
    }
    
    // 新增: 添加设置配置的方法
    pub fn with_settings(mut self, settings: Arc<Settings>) -> Self {
        self.settings = Some(settings);
        self
    }
    
    pub fn with_memory_config(
        mut self,
        enable_memory_optimization: bool,
        check_interval: u64,
        access_threshold: u64,
        idle_time_threshold: u64,
        max_memory_keys: usize,
        disk_base_path: &str,
    ) -> Self {
        self.enable_memory_optimization = enable_memory_optimization;
        self.low_frequency_check_interval = check_interval;
        self.access_threshold = access_threshold;
        self.idle_time_threshold = idle_time_threshold;
        self.max_memory_keys = max_memory_keys;
        self.disk_base_path = disk_base_path.to_string();
        
        if enable_memory_optimization && std::fs::create_dir_all(&self.disk_base_path).is_err() {
            eprintln!("警告: 无法创建低频数据目录: {}", &self.disk_base_path);
        }
        
        self
    }

    pub fn get_store(&self) -> Arc<Mutex<Store>> {
        Arc::clone(&self.store)
    }
    
    fn get_key_file_path(&self, key: &str) -> String {
        format!("{}/{}.json", self.disk_base_path, BASE64_STANDARD.encode(key))
    }
    
    pub fn should_check_low_frequency(&self) -> bool {
        if !self.enable_memory_optimization {
            return false;
        }
        
        let elapsed = self.last_check_time.lock().unwrap().elapsed().as_secs();
        elapsed >= self.low_frequency_check_interval
    }
    
    pub fn check_and_offload_low_frequency_data(&self) -> Result<usize, String> {
        if !self.enable_memory_optimization {
            return Ok(0);
        }
        
        *self.last_check_time.lock().unwrap() = Instant::now();
        
        let mut offloaded_count = 0;
        
        let low_freq_keys = {
            let store = self.store.lock().unwrap();
            store.get_low_frequency_keys(
                self.access_threshold,
                self.idle_time_threshold,
                self.max_memory_keys
            )
        };
        
        for key in &low_freq_keys {
            if let Err(err) = self.offload_key_to_disk(key) {
                eprintln!("将键 '{}' 转移到磁盘时出错: {}", key, err);
                continue;
            }
            offloaded_count += 1;
        }
        
        Ok(offloaded_count)
    }
    
    fn offload_key_to_disk(&self, key: &str) -> Result<(), String> {
        let serialized_data = {
            let store = self.store.lock().unwrap();
            
            match store.serialize_key(key)? {
                Some(data) => data,
                None => return Ok(())
            }
        };
        
        let file_path = self.get_key_file_path(key);
        std::fs::write(&file_path, serialized_data)
            .map_err(|e| format!("写入文件错误: {}", e))?;
            
        let mut store = self.store.lock().unwrap();
        store.mark_as_disk_stored(key);
        
        Ok(())
    }
    
    pub fn load_key_from_disk(&self, key: &str) -> Result<bool, String> {
        let mut store = self.store.lock().unwrap();
        
        if store.data.contains_key(key) {
            return Ok(false);
        }
        
        if !store.disk_keys.contains_key(key) {
            return Ok(false);
        }
        
        let file_path = self.get_key_file_path(key);
        let content = std::fs::read_to_string(&file_path)
            .map_err(|e| format!("读取文件错误: {}", e))?;
            
        store.deserialize_key(key, &content)?;
        
        Ok(true)
    }
    
    pub fn ensure_key_loaded(&self, key: &str) -> Result<bool, String> {
        if !self.enable_memory_optimization {
            return Ok(true);
        }
        
        let needs_loading = {
            let store = self.store.lock().unwrap();
            !store.data.contains_key(key) && store.disk_keys.contains_key(key)
        };
        
        if needs_loading {
            self.load_key_from_disk(key)?;
            return Ok(true);
        }
        
        Ok(false)
    }

    pub fn load_from_file(&self, file_path: &str) -> Result<(), String> {
        match std::fs::read_to_string(file_path) {
            Ok(content) if !content.is_empty() => {
                let mut store = self.store.lock().unwrap();
                store.deserialize(&content)
            }
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    std::fs::create_dir_all(std::path::Path::new(file_path).parent().unwrap())
                        .map_err(|e| format!("创建目录失败: {}", e))?;
                    std::fs::write(file_path, "")
                        .map_err(|e| format!("创建文件失败: {}", e))?;
                    Ok(())
                } else {
                    Err(format!("读取文件错误: {}", e))
                }
            }
        }
    }

    pub fn save_to_file(&self, file_path: &str) -> Result<(), String> {
        if self.enable_memory_optimization {
            let _ = self.check_and_offload_low_frequency_data();
        }
        
        let store = self.store.lock().unwrap();
        let data = store.serialize()?;
        std::fs::write(file_path, data)
            .map_err(|e| format!("写入文件错误: {}", e))
    }

    // 获取内存优化统计信息
    pub fn get_optimization_stats(&self) -> OptimizationStats {
        let store = self.store.lock().unwrap();
        OptimizationStats {
            memory_keys_count: store.data.len(),
            disk_keys_count: store.disk_keys.len(),
            total_keys_count: store.data.len() + store.disk_keys.len(),
            memory_optimization_enabled: self.enable_memory_optimization,
            max_memory_keys: self.max_memory_keys,
            access_threshold: self.access_threshold,
            idle_time_threshold: self.idle_time_threshold,
            memory_pressure_level: store.memory_pressure.last_pressure_level,
            cache_hit_ratio: store.memory_pressure.cache_hit_ratio(),
        }
    }
    
    // 启动后台定期检查任务
    pub fn start_background_check(&self, check_interval: u64) -> std::thread::JoinHandle<()> {
        let store_manager = self.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(check_interval));
                
                if !store_manager.enable_memory_optimization {
                    continue;
                }
                
                if let Err(e) = store_manager.check_and_offload_low_frequency_data() {
                    eprintln!("后台内存优化检查失败: {}", e);
                }
            }
        })
    }
    
    // 批量预加载低频率使用的键
    pub fn preload_keys(&self, keys: &[String]) -> Result<usize, String> {
        if !self.enable_memory_optimization {
            return Ok(0);
        }
        
        let mut loaded_count = 0;
        for key in keys {
            if self.load_key_from_disk(key).unwrap_or(false) {
                loaded_count += 1;
            }
        }
        Ok(loaded_count)
    }
    
    // 优化多个键的批量转移到磁盘
    pub fn offload_keys_to_disk(&self, keys: &[String]) -> Result<usize, String> {
        if !self.enable_memory_optimization {
            return Ok(0);
        }
        
        let mut offloaded_count = 0;
        for key in keys {
            if self.offload_key_to_disk(key).is_ok() {
                offloaded_count += 1;
            }
        }
        Ok(offloaded_count)
    }
}

// 内存优化统计结构
#[derive(Debug, Clone)]
pub struct OptimizationStats {
    pub memory_keys_count: usize,     // 内存中的键数量
    pub disk_keys_count: usize,       // 存储在磁盘上的键数量
    pub total_keys_count: usize,      // 总键数量
    pub memory_optimization_enabled: bool, // 是否启用内存优化
    pub max_memory_keys: usize,       // 内存中允许的最大键数量
    pub access_threshold: u64,        // 访问次数阈值
    pub idle_time_threshold: u64,     // 闲置时间阈值（秒）
    pub memory_pressure_level: u8,    // 当前内存压力等级 (0-10)
    pub cache_hit_ratio: f64,         // 缓存命中率
}

impl std::fmt::Display for OptimizationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "内存优化统计:")?;
        writeln!(f, "  启用状态: {}", if self.memory_optimization_enabled { "已启用" } else { "未启用" })?;
        writeln!(f, "  内存中键数量: {}", self.memory_keys_count)?;
        writeln!(f, "  磁盘中键数量: {}", self.disk_keys_count)?;
        writeln!(f, "  总键数量: {}", self.total_keys_count)?;
        writeln!(f, "  内存压力等级: {}/10", self.memory_pressure_level)?;
        writeln!(f, "  缓存命中率: {:.2}%", self.cache_hit_ratio * 100.0)?;
        writeln!(f, "  内存键上限: {}", self.max_memory_keys)?;
        writeln!(f, "  访问阈值: {} 次", self.access_threshold)?;
        writeln!(f, "  闲置阈值: {} 秒", self.idle_time_threshold)?;
        Ok(())
    }
}