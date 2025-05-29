use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::path::Path;
use base64::prelude::*;

use crate::config::Settings;
use super::store_core::Store;
use super::memory::{MemoryManager, OptimizationStats};
use super::error::{StoreError, StoreResult};
use super::store_transaction::TransactionStoreManager;
use super::traits::*;

/// 重构后的线程安全存储管理器
#[derive(Debug, Clone)]
pub struct StoreManager {
    store: Arc<Mutex<Store>>,
    disk_base_path: String,
    last_check_time: Arc<Mutex<Instant>>,
    settings: Option<Arc<Settings>>,
    transaction_manager: Option<Arc<TransactionStoreManager>>,
    use_wal: bool,
    background_optimization_enabled: bool,
    optimization_interval: u64,
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
            last_check_time: Arc::new(Mutex::new(Instant::now())),
            settings: None,
            transaction_manager: None,
            use_wal: false,
            background_optimization_enabled: false,
            optimization_interval: 300, // 5分钟
        }
    }

    /// 使用配置构建
    pub fn with_settings(mut self, settings: Arc<Settings>) -> Self {
        // 将设置传递给 Store
        {
            let mut store = self.store.lock().unwrap();
            *store = store.clone().with_settings(Arc::clone(&settings));
        }
        self.settings = Some(settings);
        self
    }

    /// 启用内存优化功能
    pub fn with_memory_optimization(
        mut self,
        enable: bool,
        access_threshold: u64,
        idle_time_threshold: u64,
        max_memory_keys: usize,
        disk_base_path: &str,
    ) -> Self {
        if enable {
            let memory_manager = MemoryManager::new(
                access_threshold,
                idle_time_threshold,
                max_memory_keys,
                true,
            );

            // 设置存储的内存管理器
            {
                let mut store = self.store.lock().unwrap();
                *store = store.clone().with_memory_manager(memory_manager);
            }

            self.disk_base_path = disk_base_path.to_string();
            
            // 创建磁盘目录
            if std::fs::create_dir_all(&self.disk_base_path).is_err() {
                eprintln!("警告: 无法创建低频数据目录: {}", &self.disk_base_path);
            }
        }
        
        self
    }

    /// 启用 WAL 功能
    pub fn with_wal(mut self, _wal_path: &Path) -> Self {
        let txn_manager = TransactionStoreManager::new();
        self.transaction_manager = Some(Arc::new(txn_manager));
        self.use_wal = true;
        self
    }

    /// 启用后台优化
    pub fn with_background_optimization(mut self, enabled: bool, interval_seconds: u64) -> Self {
        self.background_optimization_enabled = enabled;
        self.optimization_interval = interval_seconds;
        self
    }

    /// 获取存储的引用
    pub fn get_store(&self) -> Arc<Mutex<Store>> {
        Arc::clone(&self.store)
    }

    /// 获取键的磁盘文件路径
    fn get_key_file_path(&self, key: &str) -> String {
        format!("{}/{}.json", self.disk_base_path, BASE64_STANDARD.encode(key))
    }

    /// 检查是否应该执行低频数据检查
    pub fn should_check_low_frequency(&self) -> bool {
        let elapsed = self.last_check_time.lock().unwrap().elapsed().as_secs();
        elapsed >= self.optimization_interval
    }

    /// 执行低频数据转移
    pub fn check_and_offload_low_frequency_data(&self) -> StoreResult<usize> {
        *self.last_check_time.lock().unwrap() = Instant::now();
        
        let mut offloaded_count = 0;
        
        // 首先清理过期键
        {
            let mut store = self.store.lock().unwrap();
            let expired_count = store.clean_expired_keys();
            if expired_count > 0 {
                log::info!("清理了 {} 个过期键", expired_count);
            }
        }

        // 检查是否需要内存优化
        let should_optimize = {
            let store = self.store.lock().unwrap();
            store.should_optimize_memory()
        };

        if should_optimize {
            // 获取需要转移的键
            let low_freq_keys = {
                let store = self.store.lock().unwrap();
                store.get_low_frequency_keys(100) // 一次最多转移100个键
            };

            // 转移键到磁盘
            for key in &low_freq_keys {
                if let Err(err) = self.offload_key_to_disk(key) {
                    log::error!("将键 '{}' 转移到磁盘时出错: {}", key, err);
                    continue;
                }
                offloaded_count += 1;
            }

            if offloaded_count > 0 {
                log::info!("成功转移 {} 个键到磁盘", offloaded_count);
            }
        }
        
        Ok(offloaded_count)
    }

    /// 将键转移到磁盘
    fn offload_key_to_disk(&self, key: &str) -> StoreResult<()> {
        let serialized_data = {
            let store = self.store.lock().unwrap();
            match store.serialize_key(key)? {
                Some(data) => data,
                None => return Ok(()),
            }
        };

        let file_path = self.get_key_file_path(key);
        std::fs::write(&file_path, serialized_data)?;

        {
            let mut store = self.store.lock().unwrap();
            store.mark_as_disk_stored(key);
        }

        Ok(())
    }

    /// 从磁盘加载键
    pub fn load_key_from_disk(&self, key: &str) -> StoreResult<bool> {
        let needs_loading = {
            let store = self.store.lock().unwrap();
            !store.data.contains_key(key) && store.disk_keys.contains_key(key)
        };

        if !needs_loading {
            return Ok(false);
        }

        let file_path = self.get_key_file_path(key);
        let content = std::fs::read_to_string(&file_path)?;

        {
            let mut store = self.store.lock().unwrap();
            store.deserialize_key(key, &content)?;
        }

        Ok(true)
    }

    /// 确保键已加载到内存
    pub fn ensure_key_loaded(&self, key: &str) -> StoreResult<bool> {
        self.load_key_from_disk(key)
    }

    /// 从文件加载整个存储
    pub fn load_from_file(&self, file_path: &str) -> StoreResult<()> {
        match std::fs::read_to_string(file_path) {
            Ok(content) if !content.is_empty() => {
                let mut store = self.store.lock().unwrap();
                store.deserialize(&content)
            }
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // 创建目录和文件
                    if let Some(parent) = Path::new(file_path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(file_path, "")?;
                    Ok(())
                } else {
                    Err(StoreError::IoError(e.to_string()))
                }
            }
        }
    }

    /// 保存到文件
    pub fn save_to_file(&self, file_path: &str) -> StoreResult<()> {
        // 如果启用了内存优化，先执行优化
        if self.background_optimization_enabled {
            let _ = self.check_and_offload_low_frequency_data();
        }

        // 如果使用WAL，创建检查点
        if self.use_wal {
            if let Some(txn_manager) = &self.transaction_manager {
                txn_manager
                    .create_checkpoint()
                    .map_err(|e| StoreError::WalError(format!("创建检查点失败: {}", e)))?;
            }
        }

        let store = self.store.lock().unwrap();
        let data = store.serialize()?;
        std::fs::write(file_path, data)?;
        Ok(())
    }

    /// 从WAL恢复数据
    pub fn recover_from_wal(&self) -> StoreResult<()> {
        if !self.use_wal {
            return Ok(());
        }

        if let Some(txn_manager) = &self.transaction_manager {
            txn_manager
                .recover_from_wal()
                .map_err(StoreError::WalError)
        } else {
            Err(StoreError::WalError("事务管理器未初始化".to_string()))
        }
    }

    /// 获取优化统计信息
    pub fn get_optimization_stats(&self) -> OptimizationStats {
        let store = self.store.lock().unwrap();
        store.get_optimization_stats()
    }

    /// 启动后台优化任务
    pub fn start_background_optimization(&self) -> Option<std::thread::JoinHandle<()>> {
        if !self.background_optimization_enabled {
            return None;
        }

        let store_manager = self.clone();
        Some(std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(store_manager.optimization_interval));

                if let Err(e) = store_manager.check_and_offload_low_frequency_data() {
                    log::error!("后台内存优化检查失败: {}", e);
                }
            }
        }))
    }

    /// 批量预加载键
    pub fn preload_keys(&self, keys: &[String]) -> StoreResult<usize> {
        let mut loaded_count = 0;
        for key in keys {
            if self.load_key_from_disk(key).unwrap_or(false) {
                loaded_count += 1;
            }
        }
        Ok(loaded_count)
    }

    /// 批量转移键到磁盘
    pub fn offload_keys_to_disk(&self, keys: &[String]) -> StoreResult<usize> {
        let mut offloaded_count = 0;
        for key in keys {
            if self.offload_key_to_disk(key).is_ok() {
                offloaded_count += 1;
            }
        }
        Ok(offloaded_count)
    }

    /// 执行内存优化
    pub fn optimize_memory(&self) -> StoreResult<usize> {
        let mut store = self.store.lock().unwrap();
        store.optimize_memory()
    }

    /// 获取内存使用统计
    pub fn get_memory_usage(&self) -> usize {
        let store = self.store.lock().unwrap();
        store.memory_usage()
    }

    /// 获取所有键
    pub fn get_all_keys(&self) -> Vec<String> {
        let store = self.store.lock().unwrap();
        store.get_all_keys()
    }

    /// 获取磁盘键
    pub fn get_disk_keys(&self) -> Vec<String> {
        let store = self.store.lock().unwrap();
        store.get_disk_keys()
    }

    /// 获取内存键
    pub fn get_memory_keys(&self) -> Vec<String> {
        let store = self.store.lock().unwrap();
        store.get_memory_keys()
    }
}

// 为 StoreManager 实现操作代理方法
impl StoreManager {
    /// 字符串操作
    pub fn set_string(&self, key: String, value: String) -> StoreResult<String> {
        self.ensure_key_loaded(&key)?;
        let mut store = self.store.lock().unwrap();
        store.set(key, value)
    }

    pub fn get_string(&self, key: &str) -> StoreResult<Option<String>> {
        self.ensure_key_loaded(key)?;
        let store = self.store.lock().unwrap();
        store.get(key)
    }

    /// 列表操作
    pub fn lpush(&self, key: String, value: String) -> StoreResult<usize> {
        self.ensure_key_loaded(&key)?;
        let mut store = self.store.lock().unwrap();
        store.lpush(key, value)
    }

    pub fn rpush(&self, key: String, value: String) -> StoreResult<usize> {
        self.ensure_key_loaded(&key)?;
        let mut store = self.store.lock().unwrap();
        store.rpush(key, value)
    }

    pub fn lpop(&self, key: &str) -> StoreResult<Option<String>> {
        self.ensure_key_loaded(key)?;
        let mut store = self.store.lock().unwrap();
        store.lpop(key)
    }

    pub fn rpop(&self, key: &str) -> StoreResult<Option<String>> {
        self.ensure_key_loaded(key)?;
        let mut store = self.store.lock().unwrap();
        store.rpop(key)
    }

    pub fn lrange(&self, key: &str, start: isize, end: isize) -> StoreResult<Vec<String>> {
        self.ensure_key_loaded(key)?;
        let store = self.store.lock().unwrap();
        store.lrange(key, start, end)
    }

    pub fn llen(&self, key: &str) -> StoreResult<usize> {
        self.ensure_key_loaded(key)?;
        let store = self.store.lock().unwrap();
        store.llen(key)
    }

    /// 哈希表操作
    pub fn hset(&self, key: String, field: String, value: String) -> StoreResult<bool> {
        self.ensure_key_loaded(&key)?;
        let mut store = self.store.lock().unwrap();
        store.hset(key, field, value)
    }

    pub fn hget(&self, key: &str, field: &str) -> StoreResult<Option<String>> {
        self.ensure_key_loaded(key)?;
        let store = self.store.lock().unwrap();
        store.hget(key, field)
    }

    pub fn hdel(&self, key: &str, field: &str) -> StoreResult<bool> {
        self.ensure_key_loaded(key)?;
        let mut store = self.store.lock().unwrap();
        store.hdel(key, field)
    }

    /// 集合操作
    pub fn sadd(&self, key: String, members: Vec<String>) -> StoreResult<usize> {
        self.ensure_key_loaded(&key)?;
        let mut store = self.store.lock().unwrap();
        store.sadd(key, members)
    }

    pub fn smembers(&self, key: &str) -> StoreResult<Vec<String>> {
        self.ensure_key_loaded(key)?;
        let store = self.store.lock().unwrap();
        store.smembers(key)
    }

    pub fn sismember(&self, key: &str, member: &str) -> StoreResult<bool> {
        self.ensure_key_loaded(key)?;
        let store = self.store.lock().unwrap();
        store.sismember(key, member)
    }

    pub fn srem(&self, key: &str, member: &str) -> StoreResult<bool> {
        self.ensure_key_loaded(key)?;
        let mut store = self.store.lock().unwrap();
        store.srem(key, member)
    }

    /// 通用操作
    pub fn exists(&self, key: &str) -> bool {
        let store = self.store.lock().unwrap();
        store.exists(key)
    }

    pub fn delete_key(&self, key: &str) -> StoreResult<bool> {
        // 删除磁盘文件（如果存在）
        let file_path = self.get_key_file_path(key);
        let _ = std::fs::remove_file(file_path);
        
        let mut store = self.store.lock().unwrap();
        store.delete(key)
    }

    pub fn set_expire(&self, key: &str, seconds: u64) -> StoreResult<bool> {
        let mut store = self.store.lock().unwrap();
        store.set_expire(key, seconds)
    }

    pub fn get_ttl(&self, key: &str) -> StoreResult<i64> {
        let store = self.store.lock().unwrap();
        store.get_ttl(key)
    }

    pub fn persist_key(&self, key: &str) -> StoreResult<bool> {
        let mut store = self.store.lock().unwrap();
        store.persist_key(key)
    }

    // 命令处理器需要的额外方法别名
    
    /// 删除键（别名方法）
    pub fn del_key(&self, key: &str) -> StoreResult<bool> {
        self.delete_key(key)
    }
    
    /// 列表范围查询（别名方法）
    pub fn range(&self, key: &str, start: isize, end: isize) -> StoreResult<Vec<String>> {
        self.lrange(key, start, end)
    }
    
    /// 列表删除操作（别名方法）
    pub fn ldel(&self, key: &str) -> StoreResult<bool> {
        self.delete_key(key)
    }
    
    /// 哈希字段删除
    pub fn hdel_field(&self, key: &str, field: &str) -> StoreResult<bool> {
        self.hdel(key, field)
    }
    
    /// 哈希键删除
    pub fn hdel_key(&self, key: &str) -> StoreResult<bool> {
        self.delete_key(key)
    }
    
    /// 集合成员查询
    pub fn smember_query(&self, key: &str, value: &str) -> StoreResult<bool> {
        self.sismember(key, value)
    }
    
    /// 过期设置（别名方法）
    pub fn expire(&self, key: &str, seconds: u64) -> StoreResult<bool> {
        self.set_expire(key, seconds)
    }
    
    /// TTL查询（别名方法）
    pub fn ttl(&self, key: &str) -> StoreResult<i64> {
        self.get_ttl(key)
    }
}
