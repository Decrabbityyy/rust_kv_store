use std::collections::HashMap;
use super::data_types::DataType;
use super::error::{StoreError, StoreResult};

pub struct StringHandler;

impl StringHandler {
    /// 设置字符串值的内部实现
    fn set_string_internal(
        data: &mut HashMap<String, DataType>,
        key: String,
        value: String,
    ) -> StoreResult<String> {
        // 检查值中是否包含 EX 参数（用于设置过期时间）
        let parts: Vec<&str> = value.split(" EX ").collect();
        let actual_value = parts[0].to_string();
        
        // 根据是否存在键来决定操作类型
        let result = if let Some(data_type) = data.get_mut(&key) {
            match data_type {
                DataType::String(ref mut s) => {
                    *s = actual_value.clone();
                    "OK".to_string()
                }
                _ => {
                    // 如果类型不匹配，替换为字符串类型
                    data.insert(key, DataType::String(actual_value.clone()));
                    "OK".to_string()
                }
            }
        } else {
            // 新键
            data.insert(key, DataType::String(actual_value.clone()));
            "OK".to_string()
        };
        
        Ok(result)
    }

    /// 获取字符串值的内部实现
    fn get_string_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<Option<String>> {
        match data.get(key) {
            Some(DataType::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "string".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(None),
        }
    }

    /// 追加字符串的内部实现
    pub fn append_internal(
        data: &mut HashMap<String, DataType>,
        key: &str,
        value: &str,
    ) -> StoreResult<usize> {
        match data.get_mut(key) {
            Some(DataType::String(ref mut s)) => {
                s.push_str(value);
                Ok(s.len())
            }
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "string".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => {
                // 如果键不存在，创建新的字符串
                data.insert(key.to_string(), DataType::String(value.to_string()));
                Ok(value.len())
            }
        }
    }

    /// 获取字符串长度的内部实现
    pub fn strlen_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<usize> {
        match data.get(key) {
            Some(DataType::String(value)) => Ok(value.len()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "string".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(0), // Redis 行为：不存在的键长度为 0
        }
    }

    /// 检查字符串值是否包含过期时间设置
    fn parse_expiry_from_value(value: &str) -> (String, Option<u64>) {
        let parts: Vec<&str> = value.split(" EX ").collect();
        let actual_value = parts[0].to_string();
        
        if parts.len() > 1 {
            if let Ok(seconds) = parts[1].parse::<u64>() {
                return (actual_value, Some(seconds));
            }
        }
        
        (actual_value, None)
    }
}
