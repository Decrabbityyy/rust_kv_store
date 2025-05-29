use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

/// 数据项元信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMetadata {
    /// 访问次数
    pub access_count: u64,
    /// 上次访问时间（Unix时间戳）
    pub last_access_time: u64,
    /// 创建时间（Unix时间戳）
    pub created_time: u64,
    /// 最后修改时间（Unix时间戳）
    pub modified_time: u64,
    /// 数据大小（字节）
    pub size: usize,
}

impl Default for DataMetadata {
    fn default() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            access_count: 0,
            last_access_time: now,
            created_time: now,
            modified_time: now,
            size: 0,
        }
    }
}

impl DataMetadata {
    /// 创建新的元数据
    pub fn new(size: usize) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            access_count: 1,
            last_access_time: now,
            created_time: now,
            modified_time: now,
            size,
        }
    }

    /// 记录访问
    pub fn access(&mut self) {
        self.access_count += 1;
        self.last_access_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// 记录修改
    pub fn modify(&mut self, new_size: usize) {
        self.modified_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.size = new_size;
        self.access();
    }

    /// 获取闲置时间（秒）
    pub fn idle_time(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(self.last_access_time)
    }
}

/// 内存压力监控结构
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
        Self::default()
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
        if max_memory_keys == 0 {
            return 0;
        }
        
        let usage_ratio = memory_keys as f64 / max_memory_keys as f64;
        let hit_ratio = self.cache_hit_ratio();
        
        // 基于内存使用率和缓存命中率计算压力等级
        let base_level = (usage_ratio * 10.0) as u8;
        let hit_adjustment = if hit_ratio < 0.8 { 2 } else { 0 };
        
        (base_level + hit_adjustment).min(10)
    }
}
