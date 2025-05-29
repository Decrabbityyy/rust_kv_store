use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::store::{WriteAheadLog, LogCommand, LogEntry, WalResult, WalError, Checkpoint};

/// 事务状态
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    /// 活跃事务，可以执行和提交
    Active,
    /// 已提交的事务
    Committed,
    /// 已回滚的事务
    RolledBack,
    /// 已准备好提交的事务（两阶段提交的第一阶段）
    Prepared,
}

/// 表示存储操作
#[derive(Debug, Clone)]
pub enum StoreOperation {
    /// 设置键值对
    Set(String, String),
    /// 删除键
    Delete(String),
    /// 列表左侧推入
    LPush(String, String),
    /// 列表右侧推入
    RPush(String, String),
    /// 列表左侧弹出
    LPop(String),
    /// 列表右侧弹出
    RPop(String),
    /// 删除列表
    LDel(String),
    /// 哈希表设置字段
    HSet(String, String, String),
    /// 哈希表删除字段
    HDel(String, String),
    /// 哈希表删除整表
    HDelKey(String),
    /// 集合添加元素
    SAdd(String, String),
    /// 集合移除元素
    SRem(String, String),
}

/// 事务结构
#[derive(Debug, Clone)]
pub struct Transaction {
    /// 事务ID
    pub id: u64,
    /// 事务状态
    pub state: TransactionState,
    /// 事务操作列表
    pub operations: Vec<StoreOperation>,
    /// 开始时间戳
    pub start_time: u64,
    /// 提交或回滚时间戳
    pub end_time: Option<u64>,
    /// 本地缓存修改数据
    pub local_data: HashMap<String, String>,
}

impl Transaction {
    /// 创建新事务
    pub fn new(id: u64) -> Self {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        Transaction {
            id,
            state: TransactionState::Active,
            operations: Vec::new(),
            start_time,
            end_time: None,
            local_data: HashMap::new(),
        }
    }
    
    /// 添加操作到事务
    pub fn add_operation(&mut self, operation: StoreOperation) -> Result<(), String> {
        if self.state != TransactionState::Active {
            return Err(format!("事务 {} 不再活跃，状态为: {:?}", self.id, self.state));
        }
        self.operations.push(operation);
        Ok(())
    }
    
    /// 标记为已提交
    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
        self.end_time = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );
    }
    
    /// 标记为已回滚
    pub fn rollback(&mut self) {
        self.state = TransactionState::RolledBack;
        self.end_time = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );
    }
    
    /// 标记为已准备
    pub fn prepare(&mut self) {
        self.state = TransactionState::Prepared;
    }
}

/// 事务管理器
pub struct TransactionManager {
    /// 当前活跃的事务
    active_transactions: Arc<RwLock<HashMap<u64, Arc<Mutex<Transaction>>>>>,
    /// WAL日志
    wal: Arc<Mutex<WriteAheadLog>>,
    /// 下一个事务ID
    next_txn_id: Arc<Mutex<u64>>,
    /// WAL日志路径
    wal_path: PathBuf,
    /// 是否启用自动检查点
    auto_checkpoint: bool,
    /// 事务操作计数器（用于自动检查点）
    operation_count: Arc<Mutex<u64>>,
    /// 自动检查点阈值
    checkpoint_threshold: u64,
    /// 存储引用，可选，用于获取操作前的数据
    store: Option<Arc<Mutex<super::Store>>>,
}

impl TransactionManager {
    /// 设置存储引用
    pub fn set_store(&mut self, store: Arc<Mutex<super::Store>>) {
        self.store = Some(store);
    }

    /// 获取存储引用
    pub fn get_store(&self) -> Option<Arc<Mutex<super::Store>>> {
        self.store.clone()
    }

    /// 创建新的事务管理器
    pub fn new(wal_path: &Path) -> WalResult<Self> {
        // 创建WAL实例
        let wal = WriteAheadLog::new(wal_path)?;
        
        // 恢复未完成的事务
        let mut active_txns = HashMap::new();
        let entries = wal.load_entries()?;
        
        // 初始ID使用当前时间戳，保证唯一性和较大的初始值，以避免ID冲突
        let mut last_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        for entry in entries {
            if entry.id > last_id {
                last_id = entry.id;
            }
            
            match entry.command {
                LogCommand::Begin => {
                    let txn = Transaction::new(entry.id);
                    active_txns.insert(entry.id, Arc::new(Mutex::new(txn)));
                }
                LogCommand::Commit | LogCommand::Rollback => {
                    active_txns.remove(&entry.id);
                }
                _ => {}
            }
        }
        
        Ok(TransactionManager {
            active_transactions: Arc::new(RwLock::new(active_txns)),
            wal: Arc::new(Mutex::new(wal)),
            next_txn_id: Arc::new(Mutex::new(last_id + 1)),
            wal_path: wal_path.to_path_buf(), // 保存WAL日志路径，用于故障恢复和重新初始化
            auto_checkpoint: true,
            operation_count: Arc::new(Mutex::new(0)),
            checkpoint_threshold: 1000,
            store: None, // 初始化时没有存储引用
        })
    }
    
    /// 设置是否启用自动检查点
    pub fn with_auto_checkpoint(mut self, enabled: bool, threshold: u64) -> Self {
        self.auto_checkpoint = enabled;
        self.checkpoint_threshold = threshold;
        self
    }
    
    /// 开始新事务
    pub fn begin_transaction(&self) -> WalResult<u64> {
        // 使用递增ID（性能最优）
        let txn_id = {
            let mut id = self.next_txn_id.lock().unwrap();
            *id += 1;
            *id
        };
        
        // 记录到WAL
        {
            let mut wal = self.wal.lock().unwrap();
            wal.begin(txn_id)?;
        }
        
        // 创建事务对象
        let txn = Transaction::new(txn_id);
        
        // 添加到活跃事务表
        {
            let mut active_txns = self.active_transactions.write().unwrap();
            active_txns.insert(txn_id, Arc::new(Mutex::new(txn)));
        }
        
        Ok(txn_id)
    }
    
    /// 获取事务
    pub fn get_transaction(&self, txn_id: u64) -> WalResult<Transaction> {
        let txns = self.active_transactions.read().unwrap();
        
        if let Some(txn_arc) = txns.get(&txn_id) {
            let txn = txn_arc.lock().unwrap().clone();
            Ok(txn)
        } else {
            Err(WalError::TransactionNotFound(txn_id))
        }
    }
    
    /// 提交事务
    pub fn commit_transaction(&self, txn_id: u64) -> WalResult<bool> {
        // 检查事务是否存在
        {
            let txns = self.active_transactions.read().unwrap();
            if !txns.contains_key(&txn_id) {
                return Err(WalError::TransactionNotFound(txn_id));
            }
        }
        
        // 记录提交到WAL
        {
            let mut wal = self.wal.lock().unwrap();
            wal.commit(txn_id)?;
        }
        
        // 更新事务状态
        {
            let txns = self.active_transactions.read().unwrap();
            if let Some(txn) = txns.get(&txn_id) {
                let mut txn = txn.lock().unwrap();
                txn.commit();
            }
        }
        
        // 考虑从活跃事务列表中移除
        {
            let mut txns = self.active_transactions.write().unwrap();
            txns.remove(&txn_id);
        }
        
        Ok(true)
    }
    
    /// 回滚事务
    pub fn rollback_transaction(&self, txn_id: u64) -> WalResult<()> {
        // 检查事务是否存在
        {
            let txns = self.active_transactions.read().unwrap();
            if !txns.contains_key(&txn_id) {
                return Err(WalError::TransactionNotFound(txn_id));
            }
        }
        
        // 记录回滚到WAL
        {
            let mut wal = self.wal.lock().unwrap();
            wal.rollback(txn_id)?;
        }
        
        // 更新事务状态
        {
            let txns = self.active_transactions.read().unwrap();
            if let Some(txn) = txns.get(&txn_id) {
                let mut txn = txn.lock().unwrap();
                txn.rollback();
            }
        }
        
        // 从活跃事务列表中移除
        {
            let mut txns = self.active_transactions.write().unwrap();
            txns.remove(&txn_id);
        }
        
        Ok(())
    }
    
    /// 执行事务操作
    pub fn execute_operation(&self, txn_id: u64, operation: StoreOperation) -> WalResult<()> {
        // 检查事务是否存在
        let txn_arc = {
            let active_txns = self.active_transactions.read().unwrap();
            match active_txns.get(&txn_id) {
                Some(txn) => txn.clone(),
                None => return Err(WalError::TransactionNotFound(txn_id)),
            }
        };
        
        // 添加操作到事务
        {
            let mut txn = txn_arc.lock().unwrap();
            if txn.state != TransactionState::Active {
                return Err(WalError::InvalidEntry(format!(
                    "事务 {} 状态为 {:?}，不是活跃状态", 
                    txn_id, txn.state
                )));
            }
            txn.add_operation(operation.clone()).map_err(WalError::InvalidEntry)?;
        }
        
        // 操作成功添加到事务
        Ok(())
    }
    
    /// 向事务添加操作并记录旧值用于回滚
    pub fn add_operation_to_transaction(&self, txn_id: u64, operation: StoreOperation) -> WalResult<bool> {
        self.execute_operation(txn_id, operation)?;
        Ok(true)
    }
    
    /// 执行事务操作，包含旧值和元数据
    pub fn execute_operation_with_old_value(
        &self, 
        txn_id: u64, 
        operation: StoreOperation,
        old_value: Option<String>,
        metadata: Option<String>
    ) -> WalResult<()> {
        // 检查事务是否存在
        let txn_arc = {
            let active_txns = self.active_transactions.read().unwrap();
            match active_txns.get(&txn_id) {
                Some(txn) => txn.clone(),
                None => return Err(WalError::TransactionNotFound(txn_id)),
            }
        };
        
        // 添加操作到事务
        {
            let mut txn = txn_arc.lock().unwrap();
            if txn.state != TransactionState::Active {
                return Err(WalError::InvalidEntry(format!(
                    "事务 {} 状态为 {:?}，不是活跃状态", 
                    txn_id, txn.state
                )));
            }
            txn.add_operation(operation.clone()).map_err(WalError::InvalidEntry)?;
        }
        
        // 记录操作到WAL
        {
            let mut wal = self.wal.lock().unwrap();
            
            // 优先使用传入的旧值和元数据，或尝试根据操作类型确定默认元数据
            let actual_old_value = old_value;
            let actual_metadata = metadata.or_else(|| {
                match &operation {
                    StoreOperation::Set(_, _) => Some("string".to_string()),
                    StoreOperation::Delete(_) => Some("string".to_string()),
                    StoreOperation::LPush(_, _) => Some("list:lpush".to_string()),
                    StoreOperation::RPush(_, _) => Some("list:rpush".to_string()),
                    StoreOperation::LPop(_) => Some("list:lpop".to_string()),
                    StoreOperation::RPop(_) => Some("list:rpop".to_string()),
                    StoreOperation::LDel(_) => Some("list:ldel".to_string()),
                    StoreOperation::HSet(_, _, _) => Some("hash:hset".to_string()),
                    StoreOperation::HDel(_, _) => Some("hash:hdel".to_string()),
                    StoreOperation::HDelKey(_) => Some("hash:hdelkey".to_string()),
                    StoreOperation::SAdd(_, _) => Some("set:sadd".to_string()),
                    StoreOperation::SRem(_, _) => Some("set:srem".to_string()),
                }
            });
            
            // 根据操作类型创建日志条目
            match &operation {
                StoreOperation::Set(key, value) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(key.clone()),
                        Some(value.clone()),
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::Delete(key) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Delete,
                        Some(key.clone()),
                        None,
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::LPush(key, value) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(format!("list:{}", key)),
                        Some(value.clone()),
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::RPush(key, value) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(format!("list:{}", key)),
                        Some(value.clone()),
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::LPop(key) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(format!("list:{}", key)),
                        None,
                        actual_old_value, // 使用传入的旧值
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::RPop(key) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(format!("list:{}", key)),
                        None,
                        actual_old_value, // 使用传入的旧值
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                // 处理其他操作类型
                StoreOperation::LDel(key) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(format!("list:{}", key)),
                        None,
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::HSet(key, field, value) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(format!("hash:{}:{}", key, field)),
                        Some(value.clone()),
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::HDel(key, field) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Delete,
                        Some(format!("hash:{}:{}", key, field)),
                        None,
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::HDelKey(key) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Delete,
                        Some(format!("hash:{}", key)),
                        None,
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::SAdd(key, value) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Put,
                        Some(format!("set:{}:{}", key, value)),
                        Some("1".to_string()),
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                },
                StoreOperation::SRem(key, value) => {
                    let entry = LogEntry::new_with_metadata(
                        LogCommand::Delete,
                        Some(format!("set:{}:{}", key, value)),
                        None,
                        actual_old_value,
                        actual_metadata,
                        txn_id
                    );
                    wal.append_entry(&entry)?;
                }
            }
        }
        
        // 增加操作计数
        {
            let mut count = self.operation_count.lock().unwrap();
            *count += 1;
        }
        
        // 检查是否需要创建检查点
        self.check_checkpoint_needed()?;
        
        Ok(())
    }
    
    /// 获取事务状态
    pub fn get_transaction_state(&self, txn_id: u64) -> Option<TransactionState> {
        let active_txns = self.active_transactions.read().unwrap();
        if let Some(txn) = active_txns.get(&txn_id) {
            let txn = txn.lock().unwrap();
            Some(txn.state.clone())
        } else {
            None
        }
    }
    
    /// 列出所有活跃事务
    pub fn list_active_transactions(&self) -> Vec<u64> {
        let active_txns = self.active_transactions.read().unwrap();
        active_txns.keys().cloned().collect()
    }
    
    /// 创建检查点
    pub fn create_checkpoint(&self, data: HashMap<String, String>) -> WalResult<u64> {
        let mut wal = self.wal.lock().unwrap();
        wal.create_checkpoint(Some(data))
    }
    
    /// 从WAL恢复数据
    pub fn recover(&self) -> WalResult<HashMap<String, String>> {
        let mut wal = self.wal.lock().unwrap();
        wal.recover()
    }
    
    /// 压缩WAL日志
    pub fn compact_wal(&self) -> WalResult<()> {
        let mut wal = self.wal.lock().unwrap();
        wal.compact()
    }
    
    /// 获取WAL管理器的可变引用
    pub fn get_wal_manager(&self) -> std::sync::MutexGuard<'_, WriteAheadLog> {
        self.wal.lock().unwrap()
    }
    
    /// 获取WAL日志路径
    pub fn get_wal_path(&self) -> PathBuf {
        self.wal_path.clone()
    }
    
    /// 检查是否需要创建检查点
    fn check_checkpoint_needed(&self) -> WalResult<()> {
        if !self.auto_checkpoint {
            return Ok(());
        }
        
        let should_checkpoint = {
            let count = self.operation_count.lock().unwrap();
            *count >= self.checkpoint_threshold
        };
        
        if should_checkpoint {
            // 自动检查点，使用空数据
            self.create_checkpoint(HashMap::new())?;
        }
        
        Ok(())
    }
    
    /// 检查事务超时并自动回滚
    pub fn check_transaction_timeouts(&self, timeout_seconds: u64) -> WalResult<Vec<u64>> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        let txns = self.active_transactions.read().unwrap();
        let mut timed_out = Vec::new();
        
        // 找出超时的事务
        for (id, txn_arc) in txns.iter() {
            let txn = txn_arc.lock().unwrap();
            if txn.state == TransactionState::Active {
                let duration = current_time - txn.start_time;
                if duration > timeout_seconds {
                    timed_out.push(*id);
                }
            }
        }
        
        // 回滚超时的事务
        for txn_id in &timed_out {
            match self.rollback_transaction(*txn_id) {
                Ok(_) => {},
                Err(e) => {
                    // 记录错误但继续处理其他超时事务
                    eprintln!("回滚超时事务 {} 失败: {}", txn_id, e);
                }
            }
        }
        
        Ok(timed_out)
    }
    
    /// 获取已完成的事务列表
    pub fn get_completed_transactions(&self) -> WalResult<Vec<Transaction>> {
        let entries = self.wal.lock().unwrap().load_entries()?;
        let mut transactions = HashMap::new();
        let mut active_ids = HashSet::new();
        
        // 先找出所有事务的开始记录
        for entry in &entries {
            if entry.command == LogCommand::Begin {
                let txn = Transaction::new(entry.id);
                transactions.insert(entry.id, txn);
                active_ids.insert(entry.id);
            }
        }
        
        // 处理每个事务的操作和状态
        for entry in &entries {
            if active_ids.contains(&entry.id) {
                match entry.command {
                    LogCommand::Put | LogCommand::Delete => {
                        if let Some(txn) = transactions.get_mut(&entry.id) {
                            let key = entry.key.clone().unwrap_or_default();
                            let value = entry.value.clone();
                            // 简化处理，仅考虑基本操作
                            let op = match entry.command {
                                LogCommand::Put => StoreOperation::Set(key, value.unwrap_or_default()),
                                LogCommand::Delete => StoreOperation::Delete(key),
                                _ => continue
                            };
                            let _ = txn.add_operation(op);
                        }
                    },
                    LogCommand::Commit => {
                        if let Some(txn) = transactions.get_mut(&entry.id) {
                            txn.commit();
                        }
                        active_ids.remove(&entry.id);
                    },
                    LogCommand::Rollback => {
                        if let Some(txn) = transactions.get_mut(&entry.id) {
                            txn.rollback();
                        }
                        active_ids.remove(&entry.id);
                    },
                    _ => {}
                }
            }
        }
        
        // 返回所有已完成的事务
        let completed = transactions.into_iter()
            .filter(|(_, txn)| {
                txn.state == TransactionState::Committed || txn.state == TransactionState::RolledBack
            })
            .map(|(_, txn)| txn)
            .collect();
            
        Ok(completed)
    }
    
    /// 获取最后一个检查点
    pub fn get_last_checkpoint(&self) -> WalResult<Option<Checkpoint>> {
        let wal = self.wal.lock().unwrap();
        // get_last_checkpoint 在 WAL 中返回 WalResult<Option<Checkpoint>>
        wal.get_last_checkpoint()
    }
}
