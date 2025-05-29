use std::collections::HashMap;
use super::data_types::DataType;
use super::error::{StoreError, StoreResult};

pub struct HashHandler;

impl HashHandler {
    /// 设置哈希字段的内部实现
    pub fn hset_internal(
        data: &mut HashMap<String, DataType>,
        key: String,
        field: String,
        value: String,
    ) -> StoreResult<bool> {
        match data.get_mut(&key) {
            Some(DataType::Hash(hash)) => {
                let is_new = !hash.contains_key(&field);
                hash.insert(field, value);
                Ok(is_new)
            }
            Some(_) => {
                // 类型不匹配，替换为哈希类型
                let mut new_hash = HashMap::new();
                new_hash.insert(field, value);
                data.insert(key, DataType::Hash(new_hash));
                Ok(true)
            }
            None => {
                // 新键
                let mut new_hash = HashMap::new();
                new_hash.insert(field, value);
                data.insert(key, DataType::Hash(new_hash));
                Ok(true)
            }
        }
    }

    /// 获取哈希字段值的内部实现
    pub fn hget_internal(
        data: &HashMap<String, DataType>,
        key: &str,
        field: &str,
    ) -> StoreResult<Option<String>> {
        match data.get(key) {
            Some(DataType::Hash(hash)) => Ok(hash.get(field).cloned()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(None),
        }
    }

    /// 删除哈希字段的内部实现
    pub fn hdel_internal(
        data: &mut HashMap<String, DataType>,
        key: &str,
        field: &str,
    ) -> StoreResult<bool> {
        match data.get_mut(key) {
            Some(DataType::Hash(hash)) => Ok(hash.remove(field).is_some()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(false),
        }
    }

    /// 检查哈希字段是否存在的内部实现
    pub fn hexists_internal(
        data: &HashMap<String, DataType>,
        key: &str,
        field: &str,
    ) -> StoreResult<bool> {
        match data.get(key) {
            Some(DataType::Hash(hash)) => Ok(hash.contains_key(field)),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(false),
        }
    }

    /// 获取所有哈希字段的内部实现
    pub fn hkeys_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<Vec<String>> {
        match data.get(key) {
            Some(DataType::Hash(hash)) => Ok(hash.keys().cloned().collect()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(vec![]),
        }
    }

    /// 获取所有哈希值的内部实现
    pub fn hvals_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<Vec<String>> {
        match data.get(key) {
            Some(DataType::Hash(hash)) => Ok(hash.values().cloned().collect()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(vec![]),
        }
    }

    /// 获取哈希字段数量的内部实现
    pub fn hlen_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<usize> {
        match data.get(key) {
            Some(DataType::Hash(hash)) => Ok(hash.len()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(0),
        }
    }

    /// 获取所有哈希字段和值的内部实现
    pub fn hgetall_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<HashMap<String, String>> {
        match data.get(key) {
            Some(DataType::Hash(hash)) => Ok(hash.clone()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(HashMap::new()),
        }
    }

    /// 批量设置哈希字段的内部实现
    pub fn hmset_internal(
        data: &mut HashMap<String, DataType>,
        key: String,
        field_values: Vec<(String, String)>,
    ) -> StoreResult<()> {
        match data.get_mut(&key) {
            Some(DataType::Hash(hash)) => {
                for (field, value) in field_values {
                    hash.insert(field, value);
                }
                Ok(())
            }
            Some(_) => {
                // 类型不匹配，替换为哈希类型
                let mut new_hash = HashMap::new();
                for (field, value) in field_values {
                    new_hash.insert(field, value);
                }
                data.insert(key, DataType::Hash(new_hash));
                Ok(())
            }
            None => {
                // 新键
                let mut new_hash = HashMap::new();
                for (field, value) in field_values {
                    new_hash.insert(field, value);
                }
                data.insert(key, DataType::Hash(new_hash));
                Ok(())
            }
        }
    }

    /// 批量获取哈希字段值的内部实现
    pub fn hmget_internal(
        data: &HashMap<String, DataType>,
        key: &str,
        fields: &[String],
    ) -> StoreResult<Vec<Option<String>>> {
        match data.get(key) {
            Some(DataType::Hash(hash)) => {
                let values = fields
                    .iter()
                    .map(|field| hash.get(field).cloned())
                    .collect();
                Ok(values)
            }
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "hash".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(vec![None; fields.len()]),
        }
    }
}
