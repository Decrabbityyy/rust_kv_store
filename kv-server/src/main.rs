mod server;

use clap::{Arg, Command};
use kv_common::config::Settings;
use kv_common::logger;
use log::{error, info};
use server::Server;
use std::process;

fn main() {
    // 解析命令行参数
    let matches = Command::new("KV Store Server")
        .version("1.1")
        .about("A simple key-value store server")
        .arg(
            Arg::new("host")
                .short('h')
                .long("host")
                .value_name("HOST")
                .help("服务器主机地址")
                .num_args(1),
        )
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("服务器端口")
                .num_args(1),
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
    if let Err(e) = logger::init_logger(&settings.logging.log_file, &settings.logging.level) {
        eprintln!("初始化日志失败: {}", e);
        process::exit(1);
    }

    info!("启动服务器模式");

    // 获取服务器地址和端口（优先使用命令行参数，否则使用配置文件）
    let host = matches
        .get_one::<String>("host")
        .unwrap_or(&settings.server.host);

    let port = matches
        .get_one::<u16>("port")
        .unwrap_or(&settings.server.port);

    // 启动服务器
    run_server(host, port, &settings.persistence.data_file);
}

// 启动服务器
fn run_server(host: &str, port: &u16, data_file: &str) {
    let mut server = Server::new(host.to_string(), *port, data_file.to_string());

    info!(
        "服务器配置: 主机={}, 端口={}, 数据文件={}",
        host, port, data_file
    );

    match server.start() {
        Ok(_) => info!("服务器正常关闭"),
        Err(e) => {
            error!("服务器启动失败: {}", e);
            process::exit(1);
        }
    }
}
