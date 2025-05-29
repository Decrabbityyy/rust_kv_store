use kv_common::command::CommandHandler;
use kv_common::store::StoreManager;
use log::{debug, error, info, warn};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use chrono::Local;

pub struct Server {
    host: String,
    port: u16,
    store_manager: StoreManager,
    data_file: String,
    wal_path: String,           // WAL日志存储路径
    running: Arc<AtomicBool>,
}

impl Server {
    pub fn new(host: String, port: u16, data_file: String) -> Self {
        // 根据数据文件生成WAL路径
        let wal_path = std::path::Path::new(&data_file)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join("wal")
            .to_string_lossy()
            .to_string();
            
        Server {
            host,
            port,
            store_manager: StoreManager::new(),
            data_file,
            wal_path,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    // 启动服务器
    pub fn start(&mut self) -> Result<(), String> {
        // 初始化WAL
        let wal_dir = std::path::Path::new(&self.wal_path);
        if !wal_dir.exists() {
            std::fs::create_dir_all(wal_dir)
                .map_err(|e| format!("创建WAL目录失败: {}", e))?;
        }
        
        // 使用WAL初始化StoreManager
        self.store_manager = self.store_manager.clone().with_wal(wal_dir);
        
        // 从WAL日志中恢复数据
        info!("从WAL恢复数据...");
        if let Err(e) = self.store_manager.recover_from_wal() {
            warn!("从WAL恢复数据失败: {}", e);
        }
        
        // 加载持久化数据
        info!("从数据文件加载数据...");
        self.store_manager.load_from_file(&self.data_file)
            .map_err(|e| format!("加载数据文件失败: {}", e))?;
        
        // 创建 TCP 监听器
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr)
            .map_err(|e| format!("无法绑定到地址 {}: {}", addr, e))?;
        
        info!("服务器在 {} 上启动", addr);
        
        // 设置为运行状态
        self.running.store(true, Ordering::SeqCst);
        let running = Arc::clone(&self.running);
        
        // 捕获 Ctrl+C 信号
        let running_sig = Arc::clone(&self.running);
        ctrlc::set_handler(move || {
            info!("接收到终止信号，正在关闭服务器...");
            running_sig.store(false, Ordering::SeqCst);
        }).map_err(|e| format!("无法设置信号处理程序: {}", e))?;
        
        // 监听连接
        listener.set_nonblocking(true)
            .map_err(|e| format!("设置非阻塞模式失败: {}", e))?;
        
        while running.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    info!("新连接: {}", addr);
                    
                    // 为每个客户端创建一个线程
                    let store_manager = self.store_manager.clone();
                    let data_file = self.data_file.clone();
                    
                    thread::spawn(move || {
                        if let Err(e) = Self::handle_client(stream, addr.to_string(), store_manager, data_file) {
                            error!("处理客户端 {} 时出错: {}", addr, e);
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // 没有新连接，稍等一会再检查
                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    error!("接受连接时出错: {}", e);
                }
            }
        }
        
        // 优雅关闭：创建检查点并保存数据
        info!("创建WAL检查点和保存数据...");
        match self.store_manager.save_to_file(&self.data_file) {
            Ok(_) => info!("数据成功保存到 {}", self.data_file),
            Err(e) => error!("保存数据失败: {}", e),
        }
        
        info!("服务器已关闭");
        Ok(())
    }
    
    // 处理单个客户端连接
    fn handle_client(
        mut stream: TcpStream,
        addr: String,
        store_manager: StoreManager,
        data_file: String,
    ) -> Result<(), String> {
        // 创建命令处理器
        let command_handler = CommandHandler::new(store_manager, data_file);
        
        // 设置读取超时
        stream.set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| format!("设置读取超时失败: {}", e))?;
        
        let mut buffer = [0; 1024];
        let client_disconnected = Arc::new(AtomicBool::new(false));
        
        while !client_disconnected.load(Ordering::SeqCst) {
            // 读取客户端命令
            match stream.read(&mut buffer) {
                Ok(0) => {
                    // 客户端断开连接
                    info!("客户端 {} 断开连接", addr);
                    break;
                }
                Ok(n) => {
                    let command_str = String::from_utf8_lossy(&buffer[..n]).trim().to_string();
                    debug!("从 {} 接收到命令: {}", addr, command_str);
                    
                    if command_str.is_empty() {
                        continue;
                    }
                    
                    // 解析并执行命令
                    let command = command_handler.parse_command(&command_str);
                    let response = command_handler.execute_command(command);
                    
                    // 发送响应
                    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                    let formatted_response = format!("[{}] {}\n", timestamp, response);
                    
                    if let Err(e) = stream.write_all(formatted_response.as_bytes()) {
                        error!("向客户端 {} 发送响应时出错: {}", addr, e);
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // 超时但客户端仍然连接
                    continue;
                }
                Err(e) => {
                    error!("从客户端 {} 读取时出错: {}", addr, e);
                    break;
                }
            }
        }
        
        Ok(())
    }
}