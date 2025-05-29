use std::collections::{HashMap, HashSet};
use super::data_types::DataType;
use super::error::{StoreError, StoreResult};
use rand::seq::SliceRandom;
use rand::prelude::*;

pub struct SetHandler;

#[allow(dead_code)]
impl SetHandler {
    /// 添加集合成员的内部实现
    pub fn sadd_internal(
        data: &mut HashMap<String, DataType>,
        key: String,
        members: Vec<String>,
    ) -> StoreResult<usize> {
        match data.get_mut(&key) {
            Some(DataType::Set(set)) => {
                let initial_size = set.len();
                for member in members {
                    set.insert(member);
                }
                Ok(set.len() - initial_size)
            }
            Some(_) => {
                // 类型不匹配，替换为集合类型
                let mut new_set = HashSet::new();
                let added_count = members.len();
                for member in members {
                    new_set.insert(member);
                }
                data.insert(key, DataType::Set(new_set));
                Ok(added_count)
            }
            None => {
                // 新键
                let mut new_set = HashSet::new();
                let added_count = members.len();
                for member in members {
                    new_set.insert(member);
                }
                data.insert(key, DataType::Set(new_set));
                Ok(added_count)
            }
        }
    }

    /// 移除集合成员的内部实现
    pub fn srem_internal(
        data: &mut HashMap<String, DataType>,
        key: &str,
        member: &str,
    ) -> StoreResult<bool> {
        match data.get_mut(key) {
            Some(DataType::Set(set)) => Ok(set.remove(member)),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "set".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(false),
        }
    }

    /// 检查成员是否存在的内部实现
    pub fn sismember_internal(
        data: &HashMap<String, DataType>,
        key: &str,
        member: &str,
    ) -> StoreResult<bool> {
        match data.get(key) {
            Some(DataType::Set(set)) => Ok(set.contains(member)),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "set".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(false),
        }
    }

    /// 获取所有集合成员的内部实现
    pub fn smembers_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<Vec<String>> {
        match data.get(key) {
            Some(DataType::Set(set)) => Ok(set.iter().cloned().collect()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "set".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(vec![]),
        }
    }

    /// 获取集合大小的内部实现
    pub fn scard_internal(
        data: &HashMap<String, DataType>,
        key: &str,
    ) -> StoreResult<usize> {
        match data.get(key) {
            Some(DataType::Set(set)) => Ok(set.len()),
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "set".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(0),
        }
    }

    /// 随机获取集合成员的内部实现
    pub fn srandmember_internal(
        data: &HashMap<String, DataType>,
        key: &str,
        count: Option<isize>,
    ) -> StoreResult<Vec<String>> {
        match data.get(key) {
            Some(DataType::Set(set)) => {
                if set.is_empty() {
                    return Ok(vec![]);
                }

                let members: Vec<String> = set.iter().cloned().collect();
                let mut rng = rand::rng();

                match count {
                    None => {
                        // 返回一个随机成员
                        if let Some(member) = members.choose(&mut rng) {
                            Ok(vec![member.clone()])
                        } else {
                            Ok(vec![])
                        }
                    }
                    Some(0) => Ok(vec![]),
                    Some(n) => {
                        if n > 0 {
                            // 返回最多 n 个不重复的随机成员
                            let count = (n as usize).min(members.len());
                            let mut selected = members.clone();
                            selected.shuffle(&mut rng);
                            selected.truncate(count);
                            Ok(selected)
                        } else {
                            // 返回 |n| 个可能重复的随机成员
                            let count = (-n) as usize;
                            let mut result = Vec::new();
                            for _ in 0..count {
                                if let Some(member) = members.choose(&mut rng) {
                                    result.push(member.clone());
                                }
                            }
                            Ok(result)
                        }
                    }
                }
            }
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "set".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(vec![]),
        }
    }

    /// 随机弹出集合成员的内部实现
    pub fn spop_internal(
        data: &mut HashMap<String, DataType>,
        key: &str,
        count: Option<usize>,
    ) -> StoreResult<Vec<String>> {
        match data.get_mut(key) {
            Some(DataType::Set(set)) => {
                if set.is_empty() {
                    return Ok(vec![]);
                }

                let count = count.unwrap_or(1).min(set.len());
                let mut result = Vec::new();
                let mut rng = rand::rng();

                // 将集合转换为向量以便随机选择
                let members: Vec<String> = set.iter().cloned().collect();
                let mut selected_members = members.clone();
                selected_members.shuffle(&mut rng);

                for _ in 0..count {
                    if let Some(member) = selected_members.pop() {
                        if set.remove(&member) {
                            result.push(member);
                        }
                    }
                }

                Ok(result)
            }
            Some(_) => Err(StoreError::TypeMismatch {
                key: key.to_string(),
                expected: "set".to_string(),
                found: data.get(key).unwrap().type_name().to_string(),
            }),
            None => Ok(vec![]),
        }
    }

    /// 计算集合交集的内部实现
    pub fn sinter_internal(
        data: &HashMap<String, DataType>,
        keys: &[String],
    ) -> StoreResult<Vec<String>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut result_set: Option<HashSet<String>> = None;

        for key in keys {
            match data.get(key) {
                Some(DataType::Set(set)) => {
                    if let Some(ref mut result) = result_set {
                        // 计算交集
                        result.retain(|item| set.contains(item));
                    } else {
                        // 第一个集合
                        result_set = Some(set.clone());
                    }
                }
                Some(_) => {
                    return Err(StoreError::TypeMismatch {
                        key: key.to_string(),
                        expected: "set".to_string(),
                        found: data.get(key).unwrap().type_name().to_string(),
                    })
                }
                None => {
                    // 如果任何一个键不存在，交集为空
                    return Ok(vec![]);
                }
            }
        }

        match result_set {
            Some(set) => Ok(set.into_iter().collect()),
            None => Ok(vec![]),
        }
    }

    /// 计算集合并集的内部实现
    pub fn sunion_internal(
        data: &HashMap<String, DataType>,
        keys: &[String],
    ) -> StoreResult<Vec<String>> {
        let mut result_set = HashSet::new();

        for key in keys {
            match data.get(key) {
                Some(DataType::Set(set)) => {
                    result_set.extend(set.iter().cloned());
                }
                Some(_) => {
                    return Err(StoreError::TypeMismatch {
                        key: key.to_string(),
                        expected: "set".to_string(),
                        found: data.get(key).unwrap().type_name().to_string(),
                    })
                }
                None => {
                    // 不存在的键被忽略
                }
            }
        }

        Ok(result_set.into_iter().collect())
    }

    /// 计算集合差集的内部实现
    pub fn sdiff_internal(
        data: &HashMap<String, DataType>,
        keys: &[String],
    ) -> StoreResult<Vec<String>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // 从第一个集合开始
        let first_key = &keys[0];
        let mut result_set = match data.get(first_key) {
            Some(DataType::Set(set)) => set.clone(),
            Some(_) => {
                return Err(StoreError::TypeMismatch {
                    key: first_key.to_string(),
                    expected: "set".to_string(),
                    found: data.get(first_key).unwrap().type_name().to_string(),
                })
            }
            None => return Ok(vec![]),
        };

        // 从后续集合中移除元素
        for key in &keys[1..] {
            match data.get(key) {
                Some(DataType::Set(set)) => {
                    for item in set {
                        result_set.remove(item);
                    }
                }
                Some(_) => {
                    return Err(StoreError::TypeMismatch {
                        key: key.to_string(),
                        expected: "set".to_string(),
                        found: data.get(key).unwrap().type_name().to_string(),
                    })
                }
                None => {
                    // 不存在的键被忽略
                }
            }
        }

        Ok(result_set.into_iter().collect())
    }
}
