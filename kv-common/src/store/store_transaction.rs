use super::{Transaction, StoreOperation, Store};
use super::traits::{ListOperations, HashOperations, SetOperations};

/// 事务相关的存储管理器
#[derive(Debug)]
pub struct TransactionStoreManager {
    /// 内部存储实例
    pub store: Store,
}

impl TransactionStoreManager {
    /// 创建新的事务存储管理器
    pub fn new() -> Self {
        TransactionStoreManager {
            store: Store::new(),
        }
    }
    
    /// 创建检查点（简化实现）
    pub fn create_checkpoint(&self) -> Result<u64, String> {
        // 简化的检查点实现
        Ok(0)
    }
    
    /// 从WAL恢复数据（简化实现）
    pub fn recover_from_wal(&self) -> Result<(), String> {
        // 简化的WAL恢复实现
        Ok(())
    }
}
impl Default for TransactionStoreManager {
    fn default() -> Self {
        TransactionStoreManager::new()
    }
}
/// StoreManager 的事务扩展实现
pub trait StoreTransactionExt {
    /// 应用单个事务操作到存储
    fn apply_transaction_operation(&mut self, operation: &StoreOperation) -> bool;
    
    /// 应用整个事务的所有操作到存储
    fn apply_transaction(&mut self, transaction: &Transaction) -> bool;
    
    /// 回滚事务操作
    fn rollback_transaction_operation(&mut self, operation: &StoreOperation) -> bool;
    
    /// 获取操作的旧值（前镜像），用于事务回滚
    fn get_operation_old_value(&mut self, operation: &StoreOperation) -> Option<String>;
}

impl StoreTransactionExt for Store {
    fn apply_transaction_operation(&mut self, operation: &StoreOperation) -> bool {
        match operation {
            StoreOperation::Set(key, value) => {
                self.set_string(key.clone(), value.clone());
                true
            },
            StoreOperation::Delete(key) => self.del_key(key),
            StoreOperation::LPush(key, value) => {
                self.lpush(key.clone(), value.clone()).is_ok()
            },
            StoreOperation::RPush(key, value) => {
                self.rpush(key.clone(), value.clone()).is_ok()
            },
            StoreOperation::LPop(key) => {
                self.lpop(key).is_ok() 
            },
            StoreOperation::RPop(key) => {
                self.rpop(key).is_ok()
            },
            StoreOperation::LDel(key) => self.ldel(key),
            StoreOperation::HSet(key, field, value) => {
                self.hset(key.clone(), field.clone(), value.clone()).is_ok()
            },
            StoreOperation::HDel(key, field) => {
                self.hdel_field(key, field)
            },
            StoreOperation::HDelKey(key) => self.hdel_key(key),
            StoreOperation::SAdd(key, value) => {
                match self.sadd(key.clone(), vec![value.clone()]) {
                    Ok(count) => count > 0,
                    Err(_) => false,
                }
            },
            StoreOperation::SRem(key, value) => {
                self.srem(key, value).unwrap_or(false)
            },
        }
    }
    
    fn apply_transaction(&mut self, transaction: &Transaction) -> bool {
        if transaction.state != super::TransactionState::Committed {
            return false;
        }
        
        // 应用事务中的所有操作
        let mut all_succeeded = true;
        for operation in &transaction.operations {
            if !self.apply_transaction_operation(operation) {
                all_succeeded = false;
                // 不提前返回，尝试应用尽可能多的操作
            }
        }
        
        all_succeeded
    }
    
    fn rollback_transaction_operation(&mut self, operation: &StoreOperation) -> bool {
        // 简化的回滚实现
        match operation {
            StoreOperation::Set(key, _) => {
                // 简单删除键（实际应用中应恢复旧值）
                self.del_key(key)
            },
            StoreOperation::Delete(_) => {
                // 无法简单回滚删除操作，需要旧值信息
                false
            },
            StoreOperation::LPush(key, _) => {
                // 尝试移除最后推入的元素
                self.lpop(key) .is_ok()
            },
            StoreOperation::RPush(key, _) => {
                // 尝试移除最后推入的元素
                self.rpop(key).is_ok()
            },
            _ => false, // 其他操作暂不支持回滚
        }
    }
    
    fn get_operation_old_value(&mut self, operation: &StoreOperation) -> Option<String> {
        // 简化实现 - 返回当前值作为"旧值"
        match operation {
            StoreOperation::Set(key, _) => self.get_string(key),
            StoreOperation::Delete(key) => self.get_string(key),
            _ => None, // 其他操作类型暂不支持
        }
    }
}
