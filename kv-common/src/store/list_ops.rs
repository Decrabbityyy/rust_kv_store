use std::collections::{HashMap, VecDeque};
use super::data_types::DataType;
use super::error::{StoreError, StoreResult};

pub struct ListHandler;

impl ListHandler {
    /// 从左侧推入元素的内部实现
    pub fn lpush_internal(
        data: &mut HashMap<String, DataType>,
        key: String,
        value: String,
    ) -> StoreResult<usize> {
        match data.get_mut(&key) {
            Some(DataType::List(list)) => {
                list.push_front(value);
                Ok(list.len())
            }
            Some(_) => {
                // 类型不匹配，替换为列表类型
                let mut new_list = VecDeque::new();
                new_list.push_front(value);
                data.insert(key, DataType::List(new_list));
                Ok(1)
            }
            None => {
                // 新键
                let mut new_list = VecDeque::new();
                new_list.push_front(value);
                data.insert(key, DataType::List(new_list));
                Ok(1)
            }
        }
    }

    /// 从右侧推入元素的内部实现
    pub fn rpush_internal(
        data: &mut HashMap<String, DataType>,
        key: String,
        value: String,
    ) -> StoreResult<usize> {
        match data.get_mut(&key) {
            Some(DataType::List(list)) => {
                list.push_back(value);
                Ok(list.len())
            }
            Some(_) => {
                // 类型不匹配，替换为列表类型
                let mut new_list = VecDeque::new();
                new_list.push_back(value);
                data.insert(key, DataType::List(new_list));
                Ok(1)
            }
            None => {
                // 新键
                let mut new_list = VecDeque::new();
                new_list.push_back(value);
                data.insert(key, DataType::List(new_list));
                Ok(1)
            }
        }
    }

    /// 从左侧弹出元素的内部实现
    pub fn lpop_internal(
        data: &mut HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<Option<String>> {
        match data.get_mut(key) {
            Some(DataType::List(list)) => Ok(list.pop_front()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "list".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(None),
        }
    }

    /// 从右侧弹出元素的内部实现
    pub fn rpop_internal(
        data: &mut HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<Option<String>> {
        match data.get_mut(key) {
            Some(DataType::List(list)) => Ok(list.pop_back()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "list".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(None),
        }
    }

    /// 获取列表长度的内部实现
    pub fn llen_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<usize> {
        match data.get(key) {
            Some(DataType::List(list)) => Ok(list.len()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "list".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(0),
        }
    }

    /// 获取列表范围内元素的内部实现
    pub fn lrange_internal(
        data: &HashMap<String, DataType>,
        key: &str,
        start: isize,
        end: isize,
    ) -> StoreResult<Vec<String>> {
        match data.get(key) {
            Some(DataType::List(list)) => {
                let len = list.len() as isize;
                if len == 0 {
                    return Ok(vec![]);
                }

                // 处理负索引
                let start_idx = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    (start as usize).min(list.len())
                };

                let end_idx = if end < 0 {
                    (len + end + 1).max(0) as usize
                } else {
                    ((end + 1) as usize).min(list.len())
                };

                if start_idx >= end_idx {
                    return Ok(vec![]);
                }

                let result: Vec<String> = list
                    .iter()
                    .skip(start_idx)
                    .take(end_idx - start_idx)
                    .cloned()
                    .collect();

                Ok(result)
            }
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "list".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(vec![]),
        }
    }

    /// 根据索引获取元素的内部实现
    pub fn lindex_internal(
        data: &HashMap<String, DataType>,
        key: &str,
        index: isize,
    ) -> StoreResult<Option<String>> {
        match data.get(key) {
            Some(DataType::List(list)) => {
                let len = list.len() as isize;
                if len == 0 {
                    return Ok(None);
                }

                let idx = if index < 0 {
                    if -index > len {
                        return Ok(None);
                    }
                    (len + index) as usize
                } else {
                    if index >= len {
                        return Ok(None);
                    }
                    index as usize
                };

                Ok(list.get(idx).cloned())
            }
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "list".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(None),
        }
    }

    /// 根据索引设置元素的内部实现
    pub fn lset_internal(
        data: &mut HashMap<String, DataType>,
        key: &str,
        index: isize,
        value: String,
    ) -> StoreResult<bool> {
        match data.get_mut(key) {
            Some(DataType::List(list)) => {
                let len = list.len() as isize;
                if len == 0 {
                    return Ok(false);
                }

                let idx = if index < 0 {
                    if -index > len {
                        return Ok(false);
                    }
                    (len + index) as usize
                } else {
                    if index >= len {
                        return Ok(false);
                    }
                    index as usize
                };

                if let Some(element) = list.get_mut(idx) {
                    *element = value;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "list".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(false),
        }
    }
}
