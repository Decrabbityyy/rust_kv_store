use crate::store::{TransactionManager, StoreOperation};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// 事务命令处理器
pub struct TransactionCommandHandler {
    /// 事务管理器
    txn_manager: Arc<TransactionManager>,
    /// 当前活跃的事务ID，如果为None则表示不在事务中
    current_transaction_id: Arc<Mutex<Option<u64>>>,
}

impl TransactionCommandHandler {
    /// 创建新的事务命令处理器
    pub fn new(wal_path: &Path) -> Self {
        let txn_manager = match TransactionManager::new(wal_path) {
            Ok(manager) => Arc::new(manager),
            Err(e) => {
                // 创建失败，打印错误后使用默认设置
                eprintln!("创建事务管理器失败: {}", e);
                Arc::new(TransactionManager::new(wal_path).unwrap())
            }
        };
        
        TransactionCommandHandler {
            txn_manager,
            current_transaction_id: Arc::new(Mutex::new(None)),
        }
    }
    
    /// 从现有的事务管理器创建命令处理器
    pub fn from_manager(txn_manager: Arc<TransactionManager>) -> Self {
        TransactionCommandHandler {
            txn_manager,
            current_transaction_id: Arc::new(Mutex::new(None)),
        }
    }
    
    /// 开始事务
    pub fn begin(&self) -> Result<String, String> {
        let mut current_txn = self.current_transaction_id.lock().unwrap();
        
        if current_txn.is_some() {
            return Err("已在事务中，不能嵌套事务".to_string());
        }
        
        match self.txn_manager.begin_transaction() {
            Ok(txn_id) => {
                *current_txn = Some(txn_id);
                Ok(format!("事务{}已开始", txn_id))
            },
            Err(e) => Err(format!("开始事务失败: {}", e)),
        }
    }
    
    /// 提交事务
    pub fn commit(&self) -> Result<String, String> {
        let mut current_txn = self.current_transaction_id.lock().unwrap();
        
        match *current_txn {
            Some(txn_id) => {
                match self.txn_manager.commit_transaction(txn_id) {
                    Ok(_) => {
                        *current_txn = None;
                        Ok(format!("事务{}已提交", txn_id))
                    },
                    Err(e) => Err(format!("提交事务失败: {}", e)),
                }
            },
            None => Err("不在事务中，无法提交".to_string()),
        }
    }
    
    /// 回滚事务
    pub fn rollback(&self) -> Result<String, String> {
        let mut current_txn = self.current_transaction_id.lock().unwrap();
        
        match *current_txn {
            Some(txn_id) => {
                match self.txn_manager.rollback_transaction(txn_id) {
                    Ok(_) => {
                        *current_txn = None;
                        Ok(format!("事务{}已回滚", txn_id))
                    },
                    Err(e) => Err(format!("回滚事务失败: {}", e)),
                }
            },
            None => Err("不在事务中，无法回滚".to_string()),
        }
    }
    
    /// 创建检查点
    pub fn checkpoint(&self) -> Result<String, String> {
        use std::collections::HashMap;
        let data = HashMap::new(); // 使用空的HashMap作为默认数据
        match self.txn_manager.create_checkpoint(data) {
            Ok(id) => Ok(format!("检查点{}已创建", id)),
            Err(e) => Err(format!("创建检查点失败: {}", e)),
        }
    }
    
    /// 压缩WAL日志
    pub fn compact(&self) -> Result<String, String> {
        match self.txn_manager.compact_wal() {
            Ok(_) => Ok("WAL日志已压缩".to_string()),
            Err(e) => Err(format!("压缩WAL日志失败: {}", e)),
        }
    }
    
    /// 列出活跃事务
    pub fn list_transactions(&self) -> Result<String, String> {
        let txns = self.txn_manager.list_active_transactions();
        
        if txns.is_empty() {
            return Ok("没有活跃事务".to_string());
        }
        
        let mut result = String::from("活跃事务列表:\n");
        for txn_id in txns {
            let state = self.txn_manager.get_transaction_state(txn_id)
                .map(|s| format!("{:?}", s))
                .unwrap_or_else(|| "未知".to_string());
                
            let current = {
                let current_txn = self.current_transaction_id.lock().unwrap();
                if let Some(id) = *current_txn {
                    id == txn_id
                } else {
                    false
                }
            };
            
            if current {
                result.push_str(&format!("* {} - {}\n", txn_id, state));
            } else {
                result.push_str(&format!("  {} - {}\n", txn_id, state));
            }
        }
        
        Ok(result)
    }
    
    /// 执行存储操作
    pub fn execute_operation(&self, operation: StoreOperation) -> Result<(), String> {
        let current_txn = self.current_transaction_id.lock().unwrap();
        
        match *current_txn {
            Some(txn_id) => {
                // 在事务中，将操作添加到事务
                self.txn_manager.execute_operation(txn_id, operation)
                    .map_err(|e| format!("执行操作失败: {}", e))
            },
            None => {
                // 不在事务中，开始一个隐式事务
                match self.txn_manager.begin_transaction() {
                    Ok(txn_id) => {
                        // 将操作添加到事务
                        if let Err(e) = self.txn_manager.execute_operation(txn_id, operation) {
                            return Err(format!("执行操作失败: {}", e));
                        }
                        
                        // 立即提交事务
                        self.txn_manager.commit_transaction(txn_id)
                            .map(|_| ()) // 将 bool 转换为 ()
                            .map_err(|e| format!("提交隐式事务失败: {}", e))
                    },
                    Err(e) => Err(format!("开始隐式事务失败: {}", e)),
                }
            },
        }
    }
    
    /// 获取事务管理器
    pub fn get_transaction_manager(&self) -> Arc<TransactionManager> {
        self.txn_manager.clone()
    }
    
    /// 检查是否在事务中
    pub fn in_transaction(&self) -> bool {
        let current_txn = self.current_transaction_id.lock().unwrap();
        current_txn.is_some()
    }
    
    /// 获取当前事务ID
    pub fn current_transaction_id(&self) -> Option<u64> {
        let current_txn = self.current_transaction_id.lock().unwrap();
        *current_txn
    }
    
    /// 恢复系统并返回数据
    pub fn recover_system(&self) -> Result<String, String> {
        match self.txn_manager.recover() {
            Ok(data) => {
                let count = data.len();
                Ok(format!("恢复了{}个键值对", count))
            },
            Err(e) => Err(format!("恢复系统失败: {}", e)),
        }
    }
}
