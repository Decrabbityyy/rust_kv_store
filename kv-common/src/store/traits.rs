use super::error::StoreResult;

/// 存储操作的通用 trait
pub trait StoreOperations {
    /// 检查键是否存在
    fn exists(&self, key: &str) -> bool;
    
    /// 删除键
    fn delete(&mut self, key: &str) -> StoreResult<bool>;
    
    /// 获取键的数据类型
    fn get_type(&self, key: &str) -> StoreResult<String>;
    
    /// 检查键是否已过期
    fn is_expired(&self, key: &str) -> bool;
    
    /// 设置键的过期时间
    fn set_expire(&mut self, key: &str, seconds: u64) -> StoreResult<bool>;
    
    /// 获取键的剩余生存时间
    fn get_ttl(&self, key: &str) -> StoreResult<i64>;
    
    /// 移除键的过期时间
    fn persist_key(&mut self, key: &str) -> StoreResult<bool>;
}

/// 字符串操作 trait
pub trait StringOperations {
    /// 设置字符串值
    fn set(&mut self, key: String, value: String) -> StoreResult<String>;
    
    /// 获取字符串值
    fn get(&self, key: &str) -> StoreResult<Option<String>>;
    
    /// 追加字符串
    fn append(&mut self, key: &str, value: &str) -> StoreResult<usize>;
    
    /// 获取字符串长度
    fn strlen(&self, key: &str) -> StoreResult<usize>;
}

/// 列表操作 trait
pub trait ListOperations {
    /// 从左侧推入元素
    fn lpush(&mut self, key: String, value: String) -> StoreResult<usize>;
    
    /// 从右侧推入元素
    fn rpush(&mut self, key: String, value: String) -> StoreResult<usize>;
    
    /// 从左侧弹出元素
    fn lpop(&mut self, key: &str) -> StoreResult<Option<String>>;
    
    /// 从右侧弹出元素
    fn rpop(&mut self, key: &str) -> StoreResult<Option<String>>;
    
    /// 获取列表长度
    fn llen(&self, key: &str) -> StoreResult<usize>;
    
    /// 获取列表范围内的元素
    fn lrange(&self, key: &str, start: isize, end: isize) -> StoreResult<Vec<String>>;
    
    /// 根据索引获取元素
    fn lindex(&self, key: &str, index: isize) -> StoreResult<Option<String>>;
    
    /// 根据索引设置元素
    fn lset(&mut self, key: &str, index: isize, value: String) -> StoreResult<bool>;
}

/// 哈希表操作 trait
pub trait HashOperations {
    /// 设置哈希字段
    fn hset(&mut self, key: String, field: String, value: String) -> StoreResult<bool>;
    
    /// 获取哈希字段值
    fn hget(&self, key: &str, field: &str) -> StoreResult<Option<String>>;
    
    /// 删除哈希字段
    fn hdel(&mut self, key: &str, field: &str) -> StoreResult<bool>;
    
    /// 检查哈希字段是否存在
    fn hexists(&self, key: &str, field: &str) -> StoreResult<bool>;
    
    /// 获取所有哈希字段
    fn hkeys(&self, key: &str) -> StoreResult<Vec<String>>;
    
    /// 获取所有哈希值
    fn hvals(&self, key: &str) -> StoreResult<Vec<String>>;
    
    /// 获取哈希字段数量
    fn hlen(&self, key: &str) -> StoreResult<usize>;
    
    /// 获取所有哈希字段和值
    fn hgetall(&self, key: &str) -> StoreResult<Vec<String>>;
}

/// 集合操作 trait
pub trait SetOperations {
    /// 添加集合成员
    fn sadd(&mut self, key: String, members: Vec<String>) -> StoreResult<usize>;
    
    /// 移除集合成员
    fn srem(&mut self, key: &str, member: &str) -> StoreResult<bool>;
    
    /// 检查成员是否存在
    fn sismember(&self, key: &str, member: &str) -> StoreResult<bool>;
    
    /// 获取所有集合成员
    fn smembers(&self, key: &str) -> StoreResult<Vec<String>>;
    
    /// 获取集合大小
    fn scard(&self, key: &str) -> StoreResult<usize>;
    
    /// 随机获取集合成员
    fn srandmember(&self, key: &str, count: Option<isize>) -> StoreResult<Vec<String>>;
    
    /// 随机弹出集合成员
    fn spop(&mut self, key: &str, count: Option<usize>) -> StoreResult<Vec<String>>;
}

/// 内存管理 trait
pub trait MemoryManager {
    /// 获取内存使用统计
    fn memory_usage(&self) -> usize;
    
    /// 执行内存优化
    fn optimize_memory(&mut self) -> StoreResult<usize>;
    
    /// 检查是否需要内存优化
    fn should_optimize(&self) -> bool;
    
    /// 获取低频访问的键
    fn get_low_frequency_keys(&self, count: usize) -> Vec<String>;
}
