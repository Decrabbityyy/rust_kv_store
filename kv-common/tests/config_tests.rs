use kv_common::config::{Settings, ServerConfig, PersistenceConfig, LoggingConfig, StorageConfig, MemoryConfig, PersistenceMode};
use std::fs;
use std::path::Path;

#[test]
fn test_config_default_values() {
    // 测试默认配置值
    let config = Settings::new().unwrap();
    
    // 验证服务器默认配置
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 6379);
    
    // 验证持久化默认配置
    assert_eq!(config.persistence.data_file, "data/storage.dat");
    assert!(matches!(config.persistence.mode, PersistenceMode::OnChange));
    assert_eq!(config.persistence.interval_seconds, 300);
    
    // 验证日志默认配置
    assert_eq!(config.logging.log_file, "logs/server.log");
    assert_eq!(config.logging.level, "info");
    
    // 验证存储默认配置
    assert_eq!(config.storage.enable_default_expiry, true);
    assert_eq!(config.storage.default_expiry_seconds, 3600);
    
    // 验证内存优化默认配置
    assert_eq!(config.memory.enable_memory_optimization, true);
    assert_eq!(config.memory.low_frequency_check_interval, 60);
    assert_eq!(config.memory.access_threshold, 100);
    assert_eq!(config.memory.idle_time_threshold, 600);
    assert_eq!(config.memory.max_memory_keys, 1000);
}

#[test]
fn test_config_create_directories() {
    // 定义测试用的目录路径
    let test_data_dir = "test_data_dir";
    let test_logs_dir = "test_logs_dir";
    let test_config = Settings {
        server: ServerConfig {
            host: "localhost".to_string(),
            port: 6379,
        },
        persistence: PersistenceConfig {
            data_file: format!("{}/test.dat", test_data_dir),
            mode: PersistenceMode::OnChange,
            interval_seconds: 300,
        },
        storage: StorageConfig {
            enable_default_expiry: true,
            default_expiry_seconds: 3600,
        },
        memory: MemoryConfig {
            enable_memory_optimization: true,
            low_frequency_check_interval: 60,
            access_threshold: 100,
            idle_time_threshold: 600,
            max_memory_keys: 1000,
        },
        logging: LoggingConfig {
            log_file: format!("{}/test.log", test_logs_dir),
            level: "info".to_string(),
        },
    };
    
    // 确保测试前目录不存在
    if Path::new(test_data_dir).exists() {
        fs::remove_dir_all(test_data_dir).unwrap();
    }
    if Path::new(test_logs_dir).exists() {
        fs::remove_dir_all(test_logs_dir).unwrap();
    }
    
    // 创建数据和日志目录
    let data_path = Path::new(&test_config.persistence.data_file);
    if let Some(parent) = data_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    
    let log_path = Path::new(&test_config.logging.log_file);
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    
    // 验证目录已创建
    assert!(Path::new(test_data_dir).exists());
    assert!(Path::new(test_logs_dir).exists());
    
    // 清理测试目录
    fs::remove_dir_all(test_data_dir).unwrap();
    fs::remove_dir_all(test_logs_dir).unwrap();
}

#[test]
fn test_config_defaults_creation() {
    // 备份原始配置文件（如果存在）
    let default_config_path = Path::new("config/default.toml");
    let backup_path = Path::new("config/default.toml.bak");
    
    let had_original = default_config_path.exists();
    if had_original {
        fs::copy(default_config_path, backup_path).unwrap();
        fs::remove_file(default_config_path).unwrap();
    }
    
    // 测试自动创建配置文件
    let config = Settings::new().unwrap();
    
    // 验证配置文件已创建
    assert!(default_config_path.exists());
    
    // 验证默认配置值
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 6379);
    
    // 恢复原始配置文件
    if had_original {
        fs::remove_file(default_config_path).unwrap();
        fs::rename(backup_path, default_config_path).unwrap();
    }
}