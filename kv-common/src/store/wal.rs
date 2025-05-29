// filepath: /Users/linyin/RustroverProjects/rust_kv_store/kv-common/src/store/wal.rs
use std::fs::{self, File};
use std::io::{BufWriter, BufRead, Write, BufReader};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::error::Error;
use std::fmt;

/// WAL操作可能的错误
#[derive(Debug)]
pub enum WalError {
    IoError(std::io::Error),
    InvalidEntry(String),
    TransactionNotFound(u64),
    CheckpointError(String),
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::IoError(e) => write!(f, "IO错误: {}", e),
            WalError::InvalidEntry(msg) => write!(f, "无效的日志条目: {}", msg),
            WalError::TransactionNotFound(txn_id) => write!(f, "事务未找到: {}", txn_id),
            WalError::CheckpointError(msg) => write!(f, "检查点错误: {}", msg),
        }
    }
}

impl Error for WalError {}

impl From<std::io::Error> for WalError {
    fn from(error: std::io::Error) -> Self {
        WalError::IoError(error)
    }
}

pub type WalResult<T> = std::result::Result<T, WalError>;

/// WAL日志支持的命令类型
#[derive(Debug, Clone, PartialEq)]
pub enum LogCommand {
    Put,      // 写入键值对
    Delete,   // 删除键
    Begin,    // 开始事务
    Commit,   // 提交事务
    Rollback, // 回滚事务
    Checkpoint, // 检查点
}

/// WAL日志条目
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub command: LogCommand,
    pub key: Option<String>,
    pub value: Option<String>,
    pub id: u64,
    pub timestamp: u64, // 添加时间戳，用于日志恢复和检查点
    pub old_value: Option<String>, // 操作前的值，用于回滚
    pub metadata: Option<String>, // 额外元数据，可存储操作的更多信息(如数据类型等)
}

impl LogEntry {
    /// 序列化日志条目为字符串
    pub fn serialize(&self) -> String {
        let cmd = match self.command {
            LogCommand::Put => "PUT",
            LogCommand::Delete => "DELETE",
            LogCommand::Begin => "BEGIN",
            LogCommand::Commit => "COMMIT",
            LogCommand::Rollback => "ROLLBACK",
            LogCommand::Checkpoint => "CHECKPOINT",
        };
        // 使用|分隔字段，增加了old_value和metadata字段
        format!("{}|{}|{}|{}|{}|{}|{}\n", 
            cmd, 
            self.key.clone().unwrap_or_default(), 
            self.value.clone().unwrap_or_default(), 
            self.id,
            self.timestamp,
            self.old_value.clone().unwrap_or_default(),
            self.metadata.clone().unwrap_or_default()
        )
    }
    
    /// 从字符串反序列化为日志条目
    pub fn deserialize(line: &str) -> Option<LogEntry> {
        let parts: Vec<&str> = line.trim().split('|').collect();
        
        // 支持旧版本日志格式 (没有old_value和metadata字段)
        if parts.len() < 4 { return None; }
        
        let command = match parts[0] {
            "PUT" => LogCommand::Put,
            "DELETE" => LogCommand::Delete,
            "BEGIN" => LogCommand::Begin,
            "COMMIT" => LogCommand::Commit,
            "ROLLBACK" => LogCommand::Rollback,
            "CHECKPOINT" => LogCommand::Checkpoint,
            _ => return None,
        };
        
        // 处理新增的时间戳字段
        let timestamp = if parts.len() >= 5 {
            parts[4].parse().unwrap_or_else(|_| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            })
        } else {
            0 // 默认时间戳
        };
        
        // 读取old_value字段(如果存在)
        let old_value = if parts.len() >= 6 && !parts[5].is_empty() {
            Some(parts[5].to_string())
        } else {
            None
        };
        
        // 读取metadata字段(如果存在)
        let metadata = if parts.len() >= 7 && !parts[6].is_empty() {
            Some(parts[6].to_string())
        } else {
            None
        };
        
        Some(LogEntry {
            command,
            key: if parts[1].is_empty() { None } else { Some(parts[1].to_string()) },
            value: if parts[2].is_empty() { None } else { Some(parts[2].to_string()) },
            id: parts[3].parse().ok()?,
            timestamp,
            old_value,
            metadata,
        })
    }
    
    /// 创建带时间戳的新日志条目
    pub fn new(command: LogCommand, key: Option<String>, value: Option<String>, id: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        LogEntry {
            command,
            key,
            value,
            id,
            timestamp,
            old_value: None,
            metadata: None,
        }
    }
    
    /// 创建带前镜像和元数据的完整日志条目
    pub fn new_with_metadata(
        command: LogCommand, 
        key: Option<String>, 
        value: Option<String>, 
        old_value: Option<String>,
        metadata: Option<String>,
        id: u64
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        LogEntry {
            command,
            key,
            value,
            id,
            timestamp,
            old_value,
            metadata,
        }
    }
}

/// 检查点数据结构
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub id: u64,
    pub timestamp: u64,
    pub data: HashMap<String, String>, // 保存检查点时的完整数据状态
}

impl Checkpoint {
    /// 将检查点序列化为文件
    pub fn serialize_to_file(&self, path: &Path) -> WalResult<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        
        // 写入检查点元数据
        writeln!(writer, "CHECKPOINT|{}|{}", self.id, self.timestamp)?;
        
        // 写入所有键值对数据
        for (key, value) in &self.data {
            writeln!(writer, "{}|{}", key, value)?;
        }
        
        writer.flush()?;
        // 确保检查点文件物理写入磁盘
        writer.get_mut().sync_all()?;
        Ok(())
    }
    
    /// 从文件反序列化检查点
    pub fn deserialize_from_file(path: &Path) -> WalResult<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        
        // 读取检查点元数据
        let meta_line = lines.next()
            .ok_or_else(|| WalError::CheckpointError("检查点文件为空".to_string()))??;
        
        let parts: Vec<&str> = meta_line.split('|').collect();
        if parts.len() < 3 || parts[0] != "CHECKPOINT" {
            return Err(WalError::CheckpointError("无效的检查点格式".to_string()));
        }
        
        let id = parts[1].parse::<u64>()
            .map_err(|_| WalError::CheckpointError("无法解析检查点ID".to_string()))?;
        let timestamp = parts[2].parse::<u64>()
            .map_err(|_| WalError::CheckpointError("无法解析检查点时间戳".to_string()))?;
        
        // 读取所有键值对
        let mut data = HashMap::new();
        for line in lines {
            let line = line?;
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 2 {
                data.insert(parts[0].to_string(), parts[1].to_string());
            }
        }
        
        Ok(Checkpoint {
            id,
            timestamp,
            data,
        })
    }
}

/// 预写式日志实现
#[derive(Debug)]
pub struct WriteAheadLog {
    log_file: PathBuf,
    writer: BufWriter<File>,
    pub last_sequence_number: u64,
    active_transactions: Vec<u64>,
    // 检查点相关字段
    checkpoint_interval: u64, // 多少条日志后创建一个检查点
    entries_since_checkpoint: u64,
    checkpoint_dir: PathBuf,
}

impl WriteAheadLog {
    /// 创建新的WAL实例
    pub fn new(log_file: &Path) -> WalResult<Self> {
        // 确保日志文件的目录存在
        if let Some(parent) = log_file.parent() {
            fs::create_dir_all(parent)?;
        }
        
        let file = std::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(log_file)?;
            
        // 创建检查点目录
        let checkpoint_dir = if let Some(parent) = log_file.parent() {
            let mut dir = parent.to_path_buf();
            dir.push("checkpoints");
            fs::create_dir_all(&dir)?;
            dir
        } else {
            let dir = PathBuf::from("checkpoints");
            fs::create_dir_all(&dir)?;
            dir
        };
        
        // 尝试初始化序列号和活动事务
        let mut last_sequence_number = 0;
        let mut active_transactions = Vec::new();
        
        let temp_reader = BufReader::new(File::open(log_file)?);
        
        for line in temp_reader.lines() {
            let line = line?;
            if let Some(entry) = LogEntry::deserialize(&line) {
                if entry.id > last_sequence_number {
                    last_sequence_number = entry.id;
                }
                
                // 跟踪活动事务
                match entry.command {
                    LogCommand::Begin => {
                        active_transactions.push(entry.id);
                    },
                    LogCommand::Commit | LogCommand::Rollback => {
                        if let Some(pos) = active_transactions.iter().position(|&id| id == entry.id) {
                            active_transactions.remove(pos);
                        }
                    },
                    _ => {}
                }
            }
        }
        
        Ok(WriteAheadLog {
            log_file: log_file.to_path_buf(),
            writer: BufWriter::new(file),
            last_sequence_number,
            active_transactions,
            checkpoint_interval: 1000, // 默认每1000条日志创建一个检查点
            entries_since_checkpoint: 0,
            checkpoint_dir,
        })
    }

    /// 设置检查点间隔
    pub fn with_checkpoint_interval(mut self, interval: u64) -> Self {
        self.checkpoint_interval = interval;
        self
    }
    
    /// 设置检查点目录
    pub fn with_checkpoint_dir(mut self, dir: PathBuf) -> WalResult<Self> {
        fs::create_dir_all(&dir)?;
        self.checkpoint_dir = dir;
        Ok(self)
    }

    /// 添加日志条目
    pub fn append_entry(&mut self, entry: &LogEntry) -> WalResult<()> {
        let line = entry.serialize();
        self.writer.write_all(line.as_bytes())?;
        self.writer.flush()?;
        
        // 执行fsync，确保数据物理写入磁盘
        self.writer.get_mut().sync_all()?; // 同步数据和元数据到磁盘
        
        self.last_sequence_number = entry.id;
        
        // 检查是否需要创建检查点
        self.entries_since_checkpoint += 1;
        if self.entries_since_checkpoint >= self.checkpoint_interval {
            self.create_checkpoint(None)?;
        }
        
        // 更新事务状态
        match entry.command {
            LogCommand::Begin => {
                self.active_transactions.push(entry.id);
            },
            LogCommand::Commit | LogCommand::Rollback => {
                if let Some(pos) = self.active_transactions.iter().position(|&id| id == entry.id) {
                    self.active_transactions.remove(pos);
                }
            },
            _ => {}
        }
        
        Ok(())
    }

    /// 加载所有日志条目
    pub fn load_entries(&self) -> WalResult<Vec<LogEntry>> {
        let file = File::open(&self.log_file)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        
        for line in reader.lines() {
            let line = line?;
            if let Some(entry) = LogEntry::deserialize(&line) {
                entries.push(entry);
            }
        }
        
        Ok(entries)
    }

    /// 开始事务
    pub fn begin(&mut self, txn_id: u64) -> WalResult<()> {
        let entry = LogEntry::new(LogCommand::Begin, None, None, txn_id);
        self.append_entry(&entry)
    }
    
    /// 提交事务
    pub fn commit(&mut self, txn_id: u64) -> WalResult<()> {
        // 检查事务是否存在
        if !self.active_transactions.contains(&txn_id) {
            return Err(WalError::TransactionNotFound(txn_id));
        }
        
        let entry = LogEntry::new(LogCommand::Commit, None, None, txn_id);
        self.append_entry(&entry)
    }
    
    /// 回滚事务
    pub fn rollback(&mut self, txn_id: u64) -> WalResult<()> {
        // 检查事务是否存在
        if !self.active_transactions.contains(&txn_id) {
            return Err(WalError::TransactionNotFound(txn_id));
        }
        
        let entry = LogEntry::new(LogCommand::Rollback, None, None, txn_id);
        self.append_entry(&entry)
    }
    
    /// 获取需要回滚的操作
    pub fn rollback_to(&self, txn_id: u64) -> WalResult<Vec<LogEntry>> {
        let entries = self.load_entries()?;
        let mut to_undo = Vec::new();
        let mut in_txn = false;
        
        // 从最新的日志向前查找
        for entry in entries.iter().rev() {
            if entry.id == txn_id && matches!(entry.command, LogCommand::Begin) {
                break;  // 找到了事务的起点，停止
            }
            
            if entry.id == txn_id {
                // 收集这个事务的所有操作
                if matches!(entry.command, LogCommand::Put | LogCommand::Delete) {
                    to_undo.push(entry.clone());
                }
                in_txn = true;
            }
        }
        
        if !in_txn && to_undo.is_empty() {
            return Err(WalError::TransactionNotFound(txn_id));
        }
        
        Ok(to_undo)
    }

    /// 创建检查点
    pub fn create_checkpoint(&mut self, data_snapshot: Option<HashMap<String, String>>) -> WalResult<u64> {
        let checkpoint_id = self.last_sequence_number + 1;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // 创建检查点记录
        let checkpoint = Checkpoint {
            id: checkpoint_id,
            timestamp,
            data: data_snapshot.unwrap_or_default(),
        };
        
        // 创建检查点文件
        let mut checkpoint_file_path = self.checkpoint_dir.clone();
        checkpoint_file_path.push(format!("checkpoint_{}.dat", checkpoint_id));
        checkpoint.serialize_to_file(&checkpoint_file_path)?;
        
        // 添加检查点条目到WAL
        let entry = LogEntry::new(
            LogCommand::Checkpoint, 
            Some(checkpoint_file_path.to_string_lossy().to_string()), 
            None, 
            checkpoint_id
        );
        self.append_entry(&entry)?;
        
        self.entries_since_checkpoint = 0;
        Ok(checkpoint_id)
    }
    
    /// 获取最后一个检查点
    pub fn get_latest_checkpoint(&self) -> WalResult<Option<Checkpoint>> {
        let entries = self.load_entries()?;
        
        // 从最新的日志向前查找检查点
        for entry in entries.iter().rev() {
            if matches!(entry.command, LogCommand::Checkpoint) {
                if let Some(checkpoint_path) = &entry.key {
                    let path = PathBuf::from(checkpoint_path);
                    if path.exists() {
                        return Ok(Some(Checkpoint::deserialize_from_file(&path)?));
                    }
                }
            }
        }
        
        Ok(None)
    }

    /// 获取最后一个检查点(别名，与TransactionManager方法签名匹配)
    pub fn get_last_checkpoint(&self) -> WalResult<Option<Checkpoint>> {
        self.get_latest_checkpoint()
    }

    /// 从WAL恢复数据
    pub fn recover(&mut self) -> WalResult<HashMap<String, String>> {
        // 首先尝试从最新的检查点恢复
        let mut data = if let Some(checkpoint) = self.get_latest_checkpoint()? {
            println!("从检查点 {} 恢复数据", checkpoint.id);
            checkpoint.data
        } else {
            println!("没有找到检查点，从头开始恢复");
            HashMap::new()
        };
        
        // 查找检查点之后的日志条目
        let entries = self.load_entries()?;
        let mut checkpoint_index = 0;
        
        // 找到最后一个检查点的位置
        for (i, entry) in entries.iter().enumerate() {
            if matches!(entry.command, LogCommand::Checkpoint) {
                checkpoint_index = i;
            }
        }
        
        // 重放检查点之后的所有已提交事务
        let mut txn_ops: HashMap<u64, Vec<LogEntry>> = HashMap::new();
        
        for entry in entries.iter().skip(checkpoint_index + 1) {
            match entry.command {
                LogCommand::Begin => {
                    // 开始一个新事务
                    txn_ops.entry(entry.id).or_default();
                },
                LogCommand::Put | LogCommand::Delete => {
                    // 将操作加入到对应的事务中
                    if self.active_transactions.contains(&entry.id) {
                        txn_ops.entry(entry.id).or_default().push(entry.clone());
                    }
                },
                LogCommand::Commit => {
                    // 提交事务: 应用所有操作
                    if let Some(ops) = txn_ops.remove(&entry.id) {
                        for op in ops {
                            match op.command {
                                LogCommand::Put => {
                                    if let (Some(key), Some(value)) = (&op.key, &op.value) {
                                        data.insert(key.clone(), value.clone());
                                    }
                                },
                                LogCommand::Delete => {
                                    if let Some(key) = &op.key {
                                        data.remove(key);
                                    }
                                },
                                _ => {}
                            }
                        }
                    }
                },
                LogCommand::Rollback => {
                    // 回滚事务: 丢弃所有操作
                    txn_ops.remove(&entry.id);
                },
                _ => {}
            }
        }
        
        // 丢弃未提交的事务
        for txn_id in &self.active_transactions {
            txn_ops.remove(txn_id);
        }
        
        Ok(data)
    }
    
    /// 压缩WAL日志
    pub fn compact(&mut self) -> WalResult<()> {
        // 首先创建一个检查点作为压缩基础
        let checkpoint_id = self.create_checkpoint(None)?;
        println!("创建检查点 {} 用于WAL压缩", checkpoint_id);
        
        // 获取当前WAL文件的路径
        let current_log_path = self.log_file.clone();
        
        // 创建一个临时文件路径用于新的WAL
        let mut temp_log_path = current_log_path.clone();
        temp_log_path.set_extension("temp");
        
        // 创建一个新的WAL文件
        let temp_file = File::create(&temp_log_path)?;
        let mut temp_writer = BufWriter::new(temp_file);
        
        // 读取当前WAL中的必要条目
        let entries = self.load_entries()?;
        let mut needed_entries = Vec::new();
        
        // 只保留检查点之后的条目和活跃事务的所有条目
        for entry in entries {
            if entry.id >= checkpoint_id || self.active_transactions.contains(&entry.id) {
                needed_entries.push(entry);
            }
        }
        
        // 将需要保留的条目写入新文件
        for entry in needed_entries {
            temp_writer.write_all(entry.serialize().as_bytes())?;
        }
        temp_writer.flush()?;
        // 确保临时文件数据物理写入磁盘
        temp_writer.get_mut().sync_all()?;
        
        // 关闭当前的WAL文件
        self.writer.flush()?;
        
        // 替换旧文件
        fs::rename(temp_log_path, &current_log_path)?;
        
        // 重新打开WAL文件
        let file = std::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&current_log_path)?;
            
        self.writer = BufWriter::new(file);
        
        println!("WAL压缩完成");
        Ok(())
    }
    
    /// 列出所有未提交的事务
    pub fn list_pending_transactions(&self) -> Vec<u64> {
        self.active_transactions.clone()
    }
    
    /// 检查事务是否活跃
    pub fn is_transaction_active(&self, txn_id: u64) -> bool {
        self.active_transactions.contains(&txn_id)
    }
    
    /// 获取WAL文件大小
    pub fn get_file_size(&self) -> WalResult<u64> {
        let metadata = fs::metadata(&self.log_file)?;
        Ok(metadata.len())
    }
    
    /// 设置日志文件轮换的大小阈值，超过该阈值时自动进行压缩
    pub fn compact_if_needed(&mut self, threshold_size: u64) -> WalResult<bool> {
        let current_size = self.get_file_size()?;
        if current_size > threshold_size {
            self.compact()?;
            return Ok(true);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    use tempfile::tempdir;

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry::new(
            LogCommand::Put, 
            Some("test_key".to_string()), 
            Some("test_value".to_string()), 
            1
        );
        
        let serialized = entry.serialize();
        let deserialized = LogEntry::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.command, LogCommand::Put);
        assert_eq!(deserialized.key, Some("test_key".to_string()));
        assert_eq!(deserialized.value, Some("test_value".to_string()));
        assert_eq!(deserialized.id, 1);
    }

    #[test]
    fn test_wal_append_and_load() -> WalResult<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        
        let mut wal = WriteAheadLog::new(&wal_path)?;
        
        // 添加条目
        let entry1 = LogEntry::new(
            LogCommand::Put, 
            Some("key1".to_string()), 
            Some("value1".to_string()), 
            1
        );
        wal.append_entry(&entry1)?;
        
        let entry2 = LogEntry::new(
            LogCommand::Put, 
            Some("key2".to_string()), 
            Some("value2".to_string()), 
            2
        );
        wal.append_entry(&entry2)?;
        
        // 加载并验证
        let loaded_entries = wal.load_entries()?;
        assert_eq!(loaded_entries.len(), 2);
        
        assert_eq!(loaded_entries[0].command, LogCommand::Put);
        assert_eq!(loaded_entries[0].key, Some("key1".to_string()));
        assert_eq!(loaded_entries[0].id, 1);
        
        assert_eq!(loaded_entries[1].command, LogCommand::Put);
        assert_eq!(loaded_entries[1].key, Some("key2".to_string()));
        assert_eq!(loaded_entries[1].id, 2);
        
        Ok(())
    }

    #[test]
    fn test_transaction_operations() -> WalResult<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("txn_test.wal");
        
        let mut wal = WriteAheadLog::new(&wal_path)?;
        
        // 开始事务
        let txn_id = 100;
        wal.begin(txn_id)?;
        assert!(wal.is_transaction_active(txn_id));
        
        // 添加操作
        let entry = LogEntry::new(
            LogCommand::Put, 
            Some("txn_key".to_string()), 
            Some("txn_value".to_string()), 
            txn_id
        );
        wal.append_entry(&entry)?;
        
        // 提交事务
        wal.commit(txn_id)?;
        assert!(!wal.is_transaction_active(txn_id));
        
        Ok(())
    }

    #[test]
    fn test_checkpoint_and_recovery() -> WalResult<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("checkpoint_test.wal");
        
        let mut wal = WriteAheadLog::new(&wal_path)?
            .with_checkpoint_interval(3); // 设置较小的检查点间隔
        
        // 添加几个操作以触发检查点
        for i in 1..5 {
            let txn_id = i;
            wal.begin(txn_id)?;
            
            let entry = LogEntry::new(
                LogCommand::Put, 
                Some(format!("key{}", i)), 
                Some(format!("value{}", i)), 
                txn_id
            );
            wal.append_entry(&entry)?;
            
            wal.commit(txn_id)?;
        }
        
        // 应该已经创建了一个检查点
        let checkpoint = wal.get_latest_checkpoint()?;
        assert!(checkpoint.is_some());
        
        // 恢复数据
        let recovered_data = wal.recover()?;
        assert!(recovered_data.contains_key("key1"));
        assert!(recovered_data.contains_key("key4"));
        assert_eq!(recovered_data.get("key3"), Some(&"value3".to_string()));
        
        Ok(())
    }

    #[test]
    fn test_compaction() -> WalResult<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("compact_test.wal");
        
        let mut wal = WriteAheadLog::new(&wal_path)?;
        
        // 添加大量操作
        for i in 1..100 {
            let entry = LogEntry::new(
                LogCommand::Put, 
                Some(format!("key{}", i)), 
                Some(format!("value{}", i)), 
                i
            );
            wal.append_entry(&entry)?;
        }
        
        // 获取压缩前的大小
        let _size_before = wal.get_file_size()?;
        
        // 压缩日志
        wal.compact()?;
        
        // 验证日志被压缩了
        let size_after = wal.get_file_size()?;
        assert!(size_after > 0); // 确保文件不为空
        
        // 恢复应该仍然可以工作
        let recovered_data = wal.recover()?;
        assert_eq!(recovered_data.len(), 99); // 应该有99个键值对
        
        Ok(())
    }
}
