use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use super::data_types::DataType;
use super::metadata::{DataMetadata, MemoryPressure};

/// 内存管理器
#[derive(Debug, Clone)]
pub struct MemoryManager {
    pub access_threshold: u64,
    pub idle_time_threshold: u64,
    pub max_memory_keys: usize,
    pub enable_optimization: bool,
}

impl MemoryManager {
    pub fn new(
        access_threshold: u64,
        idle_time_threshold: u64,
        max_memory_keys: usize,
        enable_optimization: bool,
    ) -> Self {
        Self {
            access_threshold,
            idle_time_threshold,
            max_memory_keys,
            enable_optimization,
        }
    }

    /// 获取低频访问的键
    pub fn get_low_frequency_keys(
        &self,
        data: &HashMap<String, DataType>,
        metadata: &HashMap<String, DataMetadata>,
    ) -> Vec<String> {
        if !self.enable_optimization || data.len() <= self.max_memory_keys {
            return vec![];
        }

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut candidates: Vec<(String, &DataMetadata)> = metadata
            .iter()
            .filter(|(key, meta)| {
                data.contains_key(*key)
                    && (meta.access_count < self.access_threshold
                        || (current_time - meta.last_access_time) > self.idle_time_threshold)
            })
            .map(|(key, meta)| (key.clone(), meta))
            .collect();

        // 按访问次数排序，然后按最后访问时间排序
        candidates.sort_by(|a, b| {
            let count_cmp = a.1.access_count.cmp(&b.1.access_count);
            if count_cmp != std::cmp::Ordering::Equal {
                return count_cmp;
            }
            a.1.last_access_time.cmp(&b.1.last_access_time)
        });

        let keys_to_remove = (data.len() - self.max_memory_keys).min(candidates.len());

        candidates
            .into_iter()
            .take(keys_to_remove)
            .map(|(key, _)| key)
            .collect()
    }

    /// 计算内存使用量
    pub fn calculate_memory_usage(data: &HashMap<String, DataType>) -> usize {
        data.iter()
            .map(|(key, value)| key.len() + value.estimated_size())
            .sum()
    }

    /// 检查是否应该执行内存优化
    pub fn should_optimize(
        &self,
        memory_pressure: &MemoryPressure,
        current_memory_keys: usize,
    ) -> bool {
        if !self.enable_optimization {
            return false;
        }

        // 如果超过最大内存键数量
        if current_memory_keys > self.max_memory_keys {
            return true;
        }

        // 如果内存压力等级过高
        let pressure_level = memory_pressure.calculate_pressure_level(
            current_memory_keys,
            self.max_memory_keys,
        );

        pressure_level >= 8 // 高压力阈值
    }

    /// 更新内存压力统计
    pub fn update_memory_pressure(
        &self,
        memory_pressure: &mut MemoryPressure,
        current_memory_keys: usize,
    ) {
        let new_level = memory_pressure.calculate_pressure_level(
            current_memory_keys,
            self.max_memory_keys,
        );

        memory_pressure.last_pressure_level = new_level;
        memory_pressure.last_adjustment_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// 优化内存使用的策略选择
    pub fn select_optimization_strategy(
        &self,
        pressure_level: u8,
        cache_hit_ratio: f64,
    ) -> OptimizationStrategy {
        match pressure_level {
            0..=3 => OptimizationStrategy::None,
            4..=6 => {
                if cache_hit_ratio < 0.7 {
                    OptimizationStrategy::Moderate
                } else {
                    OptimizationStrategy::Light
                }
            }
            7..=8 => OptimizationStrategy::Moderate,
            9..=10 => OptimizationStrategy::Aggressive,
            _ => OptimizationStrategy::None,
        }
    }

    /// 根据策略计算要移除的键数量
    pub fn calculate_keys_to_remove(
        &self,
        strategy: OptimizationStrategy,
        current_keys: usize,
    ) -> usize {
        match strategy {
            OptimizationStrategy::None => 0,
            OptimizationStrategy::Light => {
                // 移除超出部分的 10%
                let excess = current_keys.saturating_sub(self.max_memory_keys);
                (excess as f64 * 0.1).ceil() as usize
            }
            OptimizationStrategy::Moderate => {
                // 移除超出部分的 25%
                let excess = current_keys.saturating_sub(self.max_memory_keys);
                (excess as f64 * 0.25).ceil() as usize
            }
            OptimizationStrategy::Aggressive => {
                // 移除超出部分的 50%
                let excess = current_keys.saturating_sub(self.max_memory_keys);
                (excess as f64 * 0.5).ceil() as usize
            }
        }
    }
}

/// 内存优化策略
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OptimizationStrategy {
    None,       // 不优化
    Light,      // 轻度优化
    Moderate,   // 中度优化
    Aggressive, // 激进优化
}

/// 内存优化统计
#[derive(Debug, Clone)]
pub struct OptimizationStats {
    pub memory_keys_count: usize,         // 内存中的键数量
    pub disk_keys_count: usize,           // 存储在磁盘上的键数量
    pub total_keys_count: usize,          // 总键数量
    pub memory_optimization_enabled: bool, // 是否启用内存优化
    pub max_memory_keys: usize,           // 内存中允许的最大键数量
    pub access_threshold: u64,            // 访问次数阈值
    pub idle_time_threshold: u64,         // 闲置时间阈值（秒）
    pub memory_pressure_level: u8,        // 当前内存压力等级 (0-10)
    pub cache_hit_ratio: f64,             // 缓存命中率
    pub memory_usage_bytes: usize,        // 内存使用量（字节）
    pub optimization_strategy: OptimizationStrategy, // 当前优化策略
}

impl std::fmt::Display for OptimizationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "内存优化统计:")?;
        writeln!(f, "  内存键数量: {}", self.memory_keys_count)?;
        writeln!(f, "  磁盘键数量: {}", self.disk_keys_count)?;
        writeln!(f, "  总键数量: {}", self.total_keys_count)?;
        writeln!(f, "  内存优化: {}", if self.memory_optimization_enabled { "启用" } else { "禁用" })?;
        writeln!(f, "  最大内存键数: {}", self.max_memory_keys)?;
        writeln!(f, "  访问阈值: {}", self.access_threshold)?;
        writeln!(f, "  闲置时间阈值: {}秒", self.idle_time_threshold)?;
        writeln!(f, "  内存压力等级: {}/10", self.memory_pressure_level)?;
        writeln!(f, "  缓存命中率: {:.2}%", self.cache_hit_ratio * 100.0)?;
        writeln!(f, "  内存使用量: {} bytes", self.memory_usage_bytes)?;
        writeln!(f, "  优化策略: {:?}", self.optimization_strategy)?;
        Ok(())
    }
}
