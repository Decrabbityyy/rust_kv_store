use std::collections::{HashMap, VecDeque, HashSet};
use serde::{Deserialize, Serialize};

/// 存储系统中支持的数据类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    /// 字符串类型
    String(String),
    /// 列表类型（双向队列实现）
    List(VecDeque<String>),
    /// 哈希表类型
    Hash(HashMap<String, String>),
    /// 集合类型
    Set(HashSet<String>),
}

impl DataType {
    /// 获取数据类型名称
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::String(_) => "string",
            DataType::List(_) => "list",
            DataType::Hash(_) => "hash",
            DataType::Set(_) => "set",
        }
    }

    /// 检查是否为指定类型
    pub fn is_type(&self, type_name: &str) -> bool {
        self.type_name() == type_name
    }

    /// 获取数据的字节大小估算
    pub fn estimated_size(&self) -> usize {
        match self {
            DataType::String(s) => s.len(),
            DataType::List(list) => list.iter().map(|s| s.len()).sum::<usize>() + list.len() * 8,
            DataType::Hash(map) => {
                map.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + map.len() * 16
            }
            DataType::Set(set) => {
                set.iter().map(|s| s.len()).sum::<usize>() + set.len() * 8
            }
        }
    }
}

impl Default for DataType {
    fn default() -> Self {
        DataType::String(String::new())
    }
}
