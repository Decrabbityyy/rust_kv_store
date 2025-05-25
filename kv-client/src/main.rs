mod client;

use clap::{Command,Arg};
use kv_common::config::Settings;
use kv_common::logger;
use log::{error, info};
use client::Client;
use std::process;

fn main() {
    // 解析命令行参数
    let matches = Command::new("KV Store Client")
        .version("1.1")
        .about("A simple key-value store client")
        .arg(
            Arg::new("host")
                .short('h')
                .long("host")
                .value_name("HOST")
                .help("服务器主机地址")
                .num_args(1)
        )
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("服务器端口")
                .num_args(1)
        )
        .get_matches();

    // 加载配置
    let settings = match Settings::new() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("加载配置失败: {}", e);
            process::exit(1);
        }
    };

    // 初始化日志
    let log_file = settings.logging.log_file.replace("server", "client");
    if let Err(e) = logger::init_logger(&log_file, &settings.logging.level) {
        eprintln!("初始化日志失败: {}", e);
        process::exit(1);
    }

    info!("启动客户端模式");
    
    // 获取服务器地址和端口（优先使用命令行参数，否则使用配置文件）
    let host = matches.get_one::<String>("host")
        .unwrap_or(&settings.server.host);
    
    let port = matches.get_one::<u16>("port")
        .unwrap_or(&settings.server.port);
    
    // 启动客户端
    run_client(host, port);
}

// 启动客户端
fn run_client(host: &String, port: &u16) {
    let mut client = Client::new(host.clone(), *port);
    
    info!("客户端配置: 主机={}, 端口={}", host, port);
    
    match client.connect() {
        Ok(_) => info!("客户端正常关闭"),
        Err(e) => {
            error!("客户端连接失败: {}", e);
            process::exit(1);
        }
    }
}