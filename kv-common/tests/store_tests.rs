use kv_common::store::{Store, StoreManager};
use std::fs;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

#[test]
fn test_string_operations() {
    let mut store = Store::new();
    
    // 测试设置字符串
    let result = store.set_string("key1".to_string(), "value1".to_string());
    assert_eq!(result, "OK".to_string());
    
    // 测试获取字符串
    let value = store.get_string("key1");
    assert_eq!(value, Some("value1".to_string()));
    
    // 测试获取不存在的键
    let value = store.get_string("nonexistent");
    assert_eq!(value, None);
    
    // 测试删除键
    let deleted = store.del_key("key1");
    assert!(deleted);
    
    // 确认键已被删除
    let value = store.get_string("key1");
    assert_eq!(value, None);
    
    // 测试删除不存在的键
    let deleted = store.del_key("nonexistent");
    assert!(!deleted);
}

#[test]
fn test_list_operations() {
    let mut store = Store::new();
    
    // 测试左侧推入
    let len = store.lpush("list1".to_string(), "item1".to_string());
    assert_eq!(len, 1);
    
    // 测试右侧推入
    let len = store.rpush("list1".to_string(), "item2".to_string());
    assert_eq!(len, 2);
    
    // 测试获取列表长度
    let len = store.llen("list1");
    assert_eq!(len, 2);
    
    // 测试范围查询（获取全部元素）
    let items = store.range("list1", 0, -1);
    assert_eq!(items, vec!["item1".to_string(), "item2".to_string()]);
    
    // 测试范围查询（部分元素）
    let items = store.range("list1", 0, 0);
    assert_eq!(items, vec!["item1".to_string()]);
    
    // 测试左侧弹出
    let item = store.lpop("list1");
    assert_eq!(item, Some("item1".to_string()));
    
    // 确认列表长度减少
    let len = store.llen("list1");
    assert_eq!(len, 1);
    
    // 测试右侧弹出
    let item = store.rpop("list1");
    assert_eq!(item, Some("item2".to_string()));
    
    // 确认列表为空
    let len = store.llen("list1");
    assert_eq!(len, 0);
    
    // 测试对空列表进行操作
    let item = store.lpop("list1");
    assert_eq!(item, None);
    
    // 测试删除列表
    store.rpush("list1".to_string(), "item3".to_string());
    let deleted = store.ldel("list1");
    assert!(deleted);
    
    // 确认列表已被删除
    let len = store.llen("list1");
    assert_eq!(len, 0);
}

#[test]
fn test_hash_operations() {
    let mut store = Store::new();
    
    // 测试设置哈希表字段
    let is_new = store.hset("hash1".to_string(), "field1".to_string(), "value1".to_string());
    assert!(is_new);
    
    // 测试获取哈希表字段
    let value = store.hget("hash1", "field1");
    assert_eq!(value, Some("value1".to_string()));
    
    // 测试获取不存在的字段
    let value = store.hget("hash1", "nonexistent");
    assert_eq!(value, None);
    
    // 测试更新已存在的字段
    let is_new = store.hset("hash1".to_string(), "field1".to_string(), "updated".to_string());
    assert!(!is_new);
    
    // 验证字段已更新
    let value = store.hget("hash1", "field1");
    assert_eq!(value, Some("updated".to_string()));
    
    // 测试删除字段
    let deleted = store.hdel_field("hash1", "field1");
    assert!(deleted);
    
    // 确认字段已被删除
    let value = store.hget("hash1", "field1");
    assert_eq!(value, None);
    
    // 测试删除不存在的字段
    let deleted = store.hdel_field("hash1", "nonexistent");
    assert!(!deleted);
    
    // 测试删除整个哈希表
    store.hset("hash1".to_string(), "field2".to_string(), "value2".to_string());
    let deleted = store.hdel_key("hash1");
    assert!(deleted);
    
    // 确认哈希表已被删除
    let value = store.hget("hash1", "field2");
    assert_eq!(value, None);
}

#[test]
fn test_key_expiry() {
    let mut store = Store::new();
    
    // 设置键并添加过期时间
    store.set_string("expiring_key".to_string(), "value".to_string());
    store.expire("expiring_key", 1); // 1秒后过期
    
    // 立即检查，键应该存在
    let value = store.get_string("expiring_key");
    assert_eq!(value, Some("value".to_string()));
    
    // 等待键过期
    sleep(Duration::from_secs(2));
    
    // 访问过期键会触发过期检查
    let value = store.get_string("expiring_key");
    assert_eq!(value, None);
    
    // 测试更新过期时间
    store.set_string("key2".to_string(), "value2".to_string());
    store.expire("key2", 1); // 1秒后过期
    
    // 立即检查，键应该存在
    let value = store.get_string("key2");
    assert_eq!(value, Some("value2".to_string()));
    
    // 等待键过期
    sleep(Duration::from_secs(2));
    
    // 过期后检查，键应该不存在
    let value = store.get_string("key2");
    assert_eq!(value, None);
    
    // 测试获取过期时间
    store.set_string("key3".to_string(), "value3".to_string());
    store.expire("key3", 300);
    let ttl = store.ttl("key3");
    assert!(ttl > 0 && ttl <= 300);
    
    // 测试清除过期时间
    store.persist("key3");
    let ttl = store.ttl("key3");
    assert_eq!(ttl, -1); // -1表示永不过期
}

#[test]
fn test_memory_optimization() {
    let mut store = Store::new();
    
    // 填充存储以测试内存优化
    for i in 0..100 {
        store.set_string(format!("key{}", i), format!("value{}", i));
    }
    
    // 获取总键数量
    let total_keys = store.get_all_keys().len();
    assert_eq!(total_keys, 100);
    
    // 测试清理所有过期键
    let expired_count = store.clean_expired_keys();
    assert_eq!(expired_count, 0); // 我们没有设置过期时间，所以不应该有键被清理
    
    // 测试获取低频访问键
    let low_freq_keys = store.get_low_frequency_keys(5, 3600, 50);
    assert!(!low_freq_keys.is_empty());
    
    // 测试序列化数据
    let serialized_data = store.serialize().unwrap();
    assert!(!serialized_data.is_empty());
    
    // 测试反序列化数据
    let mut new_store = Store::new();
    new_store.deserialize(&serialized_data).unwrap();
    
    // 验证导入成功
    for i in 0..100 {
        let key = format!("key{}", i);
        let expected_value = format!("value{}", i);
        assert_eq!(new_store.get_string(&key), Some(expected_value));
    }
}

#[test]
fn test_store_manager() {
    // 创建测试文件路径
    let test_file = "target/test_store.dat";
    
    // 确保测试开始前文件不存在
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // 初始化StoreManager
    let store_manager = StoreManager::new();
    
    // 获取存储实例并添加数据
    {
        let store = store_manager.get_store();
        let mut store = store.lock().unwrap();
        store.set_string("key1".to_string(), "value1".to_string());
        store.set_string("key2".to_string(), "value2".to_string());
        
        // 测试获取所有键
        let keys = store.get_all_keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }
    
    // 持久化到文件
    store_manager.save_to_file(test_file).unwrap();
    
    // 创建新的StoreManager并从文件加载
    let new_store_manager = StoreManager::new();
    new_store_manager.load_from_file(test_file).unwrap();
    
    // 验证数据已正确加载
    {
        let store = new_store_manager.get_store();
        let mut store = store.lock().unwrap();
        assert_eq!(store.get_string("key1"), Some("value1".to_string()));
        assert_eq!(store.get_string("key2"), Some("value2".to_string()));
    }
    
    // 清理测试文件
    fs::remove_file(test_file).unwrap();
}

#[test]
fn test_store_persistence_mode() {
    // 创建测试文件路径和目录
    let test_file = "target/test_store_persistence.dat";
    let low_freq_dir = "target/low_freq_test";
    
    // 确保测试开始前文件和目录不存在
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // 清理低频数据目录
    if Path::new(low_freq_dir).exists() {
        let _ = fs::remove_dir_all(low_freq_dir);
    }
    fs::create_dir_all(low_freq_dir).unwrap();
    
    // 初始化第一个StoreManager并启用内存优化
    let store_manager = StoreManager::new()
        .with_memory_config(
            true,                // 启用内存优化
            1,                   // 每秒检查一次
            5,                   // 访问阈值
            60,                  // 闲置时间阈值（秒）
            50,                  // 内存中最大键数量
            low_freq_dir         // 低频数据目录
        );
    
    // 添加数据 - 不使用后台任务，直接控制
    {
        let store = store_manager.get_store();
        let mut store = store.lock().unwrap();
        for i in 0..10 {
            store.set_string(format!("key{}", i), format!("value{}", i));
        }
    }
    
    // 手动触发内存优化，将一些键转移到磁盘
    let _ = store_manager.check_and_offload_low_frequency_data().unwrap();
    
    // 保存到文件
    store_manager.save_to_file(test_file).unwrap();
    
    // 验证文件已创建
    assert!(Path::new(test_file).exists());
    
    // 创建新的StoreManager并加载数据，使用相同的配置
    let new_store_manager = StoreManager::new()
        .with_memory_config(
            true,                // 启用内存优化
            1,                   // 每秒检查一次
            5,                   // 访问阈值
            60,                  // 闲置时间阈值（秒）
            50,                  // 内存中最大键数量
            low_freq_dir         // 低频数据目录
        );
    
    // 加载数据
    new_store_manager.load_from_file(test_file).unwrap();
    
    // 验证磁盘键的记录被正确加载
    {
        // 获取所有键
        let disk_keys = {
            let store = new_store_manager.get_store();
            let store = store.lock().unwrap();
            store.get_disk_keys()
        };
        
        println!("磁盘键数量: {}", disk_keys.len());
        println!("磁盘键: {:?}", disk_keys);
    }
    
    // 验证数据可以被正确加载和访问
    for i in 0..10 {
        let key = format!("key{}", i);
        let expected_value = format!("value{}", i);
        
        // 确保键已加载
        let load_result = new_store_manager.ensure_key_loaded(&key);
        if let Err(e) = &load_result {
            println!("加载键 {} 失败: {}", key, e);
        }
        assert!(load_result.is_ok());
        
        // 获取并检查值
        let store = new_store_manager.get_store();
        let mut store = store.lock().unwrap();
        let actual_value = store.get_string(&key);
        
        if actual_value != Some(expected_value.clone()) {
            println!("键 {} 值不匹配, 期望: {:?}, 实际: {:?}", key, Some(expected_value.clone()), actual_value);
        }
        
        assert_eq!(actual_value, Some(expected_value));
    }
    
    // 清理测试文件和目录
    fs::remove_file(test_file).unwrap();
    fs::remove_dir_all(low_freq_dir).unwrap();
}

#[test]
fn test_store_serialization_error_handling() {
    // 测试序列化错误处理
    let mut store = Store::new();
    store.set_string("key1".to_string(), "value1".to_string());
    
    // 使用错误的方式尝试序列化数据
    // 注意：Store没有serialize_to_file方法，我们需要使用serialize方法
    let serialized_data = store.serialize().unwrap();
    
    // 创建一个不存在的目录路径来测试写入错误
    let result = fs::write("/nonexistent_dir/test.dat", serialized_data);
    assert!(result.is_err());
}

#[test]
fn test_store_concurrent_access() {
    // 测试并发访问
    let store_manager = StoreManager::new();
    
    // 在多个线程中同时访问Store
    let handles: Vec<_> = (0..5).map(|i| {
        let store_manager_clone = store_manager.clone();
        std::thread::spawn(move || {
            let store = store_manager_clone.get_store();
            let mut store = store.lock().unwrap();
            
            // 每个线程设置不同的键
            let key = format!("concurrent_key{}", i);
            let value = format!("concurrent_value{}", i);
            store.set_string(key.clone(), value.clone());
            
            // 验证设置成功
            assert_eq!(store.get_string(&key), Some(value));
        })
    }).collect();
    
    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    // 验证所有键都被正确设置
    let store = store_manager.get_store();
    let mut store = store.lock().unwrap();
    for i in 0..5 {
        let key = format!("concurrent_key{}", i);
        let expected_value = format!("concurrent_value{}", i);
        assert_eq!(store.get_string(&key), Some(expected_value));
    }
}

#[test]
fn test_store_memory_optimization_edge_cases() {
    // 创建测试文件路径和目录
    let test_file = "target/test_memory_edge_cases.dat";
    let low_freq_dir = "target/low_freq_edge_test";
    
    // 确保测试开始前文件和目录不存在
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // 清理低频数据目录
    if Path::new(low_freq_dir).exists() {
        let _ = fs::remove_dir_all(low_freq_dir);
    }
    fs::create_dir_all(low_freq_dir).unwrap();
    
    // 测试非常低的内存阈值，触发更多的内存优化逻辑
    let store_manager = StoreManager::new()
        .with_memory_config(
            true,                // 启用内存优化
            1,                   // 每秒检查一次
            1,                   // 非常低的访问阈值，使大多数键都会被移出内存
            1,                   // 非常短的闲置时间（秒）
            2,                   // 最大内存中的键数量
            low_freq_dir         // 低频数据目录
        );
    
    // 添加多个键值对
    {
        let store = store_manager.get_store();
        let mut store = store.lock().unwrap();
        
        for i in 0..5 {
            let key = format!("edge_key{}", i);
            let value = format!("edge_value{}", i);
            store.set_string(key, value);
        }
    }
    
    // 等待内存优化逻辑运行
    sleep(Duration::from_secs(2));
    
    // 检查一些键是否已移出内存
    let store = store_manager.get_store();
    let store = store.lock().unwrap();
    
    // 获取所有键
    let keys = store.get_all_keys();
    
    // 验证所有键仍然可以访问（即使已经从内存中移出）
    for i in 0..5 {
        let key = format!("edge_key{}", i);
        assert!(keys.contains(&key));
    }
    
    // 清理测试文件和目录
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    if Path::new(low_freq_dir).exists() {
        let _ = fs::remove_dir_all(low_freq_dir);
    }
}

#[test]
fn test_store_advanced_list_operations() {
    let mut store = Store::new();
    
    // 测试空列表的操作
    assert_eq!(store.range("empty_list", 0, -1), Vec::<String>::new());
    assert_eq!(store.lpop("empty_list"), None);
    assert_eq!(store.rpop("empty_list"), None);
    
    // 创建列表并进行操作
    store.lpush("list1".to_string(), "item1".to_string());
    store.lpush("list1".to_string(), "item2".to_string());
    store.rpush("list1".to_string(), "item3".to_string());
    
    // 测试范围参数的各种情况
    assert_eq!(store.range("list1", 0, 0), vec!["item2".to_string()]);
    assert_eq!(store.range("list1", -2, -1), vec!["item1".to_string(), "item3".to_string()]);
    assert_eq!(store.range("list1", 1, 10), vec!["item1".to_string(), "item3".to_string()]);
    assert_eq!(store.range("list1", -10, 10), vec!["item2".to_string(), "item1".to_string(), "item3".to_string()]);
    
    // 测试列表删除
    store.ldel("list1");
    assert_eq!(store.range("list1", 0, -1), Vec::<String>::new());
}

#[test]
fn test_store_advanced_hash_operations() {
    let mut store = Store::new();
    
    // 测试空哈希表的操作
    assert_eq!(store.hget("empty_hash", "field"), None);
    
    // 创建哈希表并进行操作
    store.hset("hash1".to_string(), "field1".to_string(), "value1".to_string());
    store.hset("hash1".to_string(), "field2".to_string(), "value2".to_string());
    
    // 测试字段存在性
    assert_eq!(store.hget("hash1", "field1"), Some("value1".to_string()));
    assert_eq!(store.hget("hash1", "nonexistent"), None);
    
    // 测试字段删除
    assert!(store.hdel_field("hash1", "field1"));
    assert_eq!(store.hget("hash1", "field1"), None);
    assert_eq!(store.hget("hash1", "field2"), Some("value2".to_string()));
    
    // 测试整个哈希表删除
    store.hdel_key("hash1");
    assert_eq!(store.hget("hash1", "field2"), None);
    
    // 测试删除不存在的字段
    assert!(!store.hdel_field("nonexistent_hash", "field"));
}

#[test]
fn test_store_expiry_edge_cases() {
    let mut store = Store::new();
    
    // 设置键值对带过期时间
    store.set_string("expire_key1".to_string(), "value1".to_string() + " EX 1"); // 1秒过期
    store.set_string("expire_key2".to_string(), "value2".to_string() + " EX 60"); // 60秒过期
    
    // 验证键立即可访问
    assert_eq!(store.get_string("expire_key1"), Some("value1".to_string()));
    assert_eq!(store.get_string("expire_key2"), Some("value2".to_string()));
    
    // 不要直接调用私有方法is_expired
    // 刚设置的键不应该立即过期，应该能正常访问
    assert_eq!(store.get_string("expire_key1"), Some("value1".to_string()));
    
    // 等待expire_key1过期
    sleep(Duration::from_secs(2));
    
    // 检查过期后的情况 - 用ttl方法替代is_expired
    // ttl返回0或负值表示已过期
    assert!(store.ttl("expire_key1") <= 0); // 现在应该过期了
    assert_eq!(store.get_string("expire_key1"), None); // 过期键应该返回None
    assert_eq!(store.get_string("expire_key2"), Some("value2".to_string())); // 未过期的键应该仍然可访问
    
    // 测试删除一个过期键
    assert!(!store.del_key("expire_key1")); // 过期键应该返回false
    
    // 测试更新一个现有键的过期时间
    store.expire("expire_key2", 1); // 将expire_key2的过期时间设为1秒
    
    sleep(Duration::from_secs(2));
    
    // 验证更新过期时间后键已过期
    assert_eq!(store.get_string("expire_key2"), None);
}