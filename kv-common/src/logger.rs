use log::{LevelFilter, SetLoggerError};
use simplelog::{CombinedLogger, Config, TermLogger, WriteLogger, TerminalMode, ColorChoice};
use std::fs::OpenOptions;
use std::path::Path;

pub fn init_logger(log_file: &str, level: &str) -> Result<(), SetLoggerError> {
    // 确保日志目录存在
    if let Some(parent) = Path::new(log_file).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                eprintln!("无法创建日志目录: {}", e);
            });
        }
    }

    // 打开或创建日志文件
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .unwrap_or_else(|e| {
            eprintln!("无法打开日志文件 {}: {}", log_file, e);
            panic!("无法初始化日志系统");
        });

    // 设置日志级别
    let level_filter = match level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };

    // 同时初始化终端日志和文件日志
    CombinedLogger::init(vec![
        // 输出到终端的日志
        TermLogger::new(
            level_filter,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        // 输出到文件的日志
        WriteLogger::new(level_filter, Config::default(), file),
    ])
}