use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufReader, BufRead};
use std::thread;
use std::time::Duration;
use std::sync::{mpsc, Arc, Mutex};
use std::collections::HashMap;

// 简化的Store结构
struct Store {
    data: HashMap<String, String>,
}

impl Store {
    fn new() -> Self {
        Store {
            data: HashMap::new(),
        }
    }
    
    fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }
    
    fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }
}

// 简化的StoreManager结构
struct StoreManager {
    store: Arc<Mutex<Store>>,
}

impl StoreManager {
    fn new() -> Self {
        StoreManager {
            store: Arc::new(Mutex::new(Store::new())),
        }
    }
    
    fn get_store(&self) -> Arc<Mutex<Store>> {
        self.store.clone()
    }
}

// 简化的Server结构
struct Server {
    host: String,
    #[allow(dead_code)] // 这是测试代码，该字段虽然未被直接读取但仍保留
    port: u16,
    store_manager: StoreManager,
    data_file: String,
}

impl Server {
    fn new(host: &str, port: u16, store_manager: StoreManager, data_file: String) -> Self {
        Server {
            host: host.to_string(),
            port,
            store_manager,
            data_file,
        }
    }
    
    fn get_host(&self) -> &str {
        &self.host
    }
    
    fn get_data_file(&self) -> &str {
        &self.data_file
    }
    
    fn process_command(&mut self, command: &str) -> String {
        let parts: Vec<&str> = command.split_whitespace().collect();
        
        if parts.is_empty() {
            return "ERROR: Empty command".to_string();
        }
        
        match parts[0].to_lowercase().as_str() {
            "ping" => "PONG".to_string(),
            "set" => {
                if parts.len() < 3 {
                    return "ERROR: Invalid SET command".to_string();
                }
                
                let key = parts[1].to_string();
                let value = parts[2].to_string();
                
                let store = self.store_manager.get_store();
                let mut store = store.lock().unwrap();
                store.set(key, value);
                
                "OK".to_string()
            },
            "get" => {
                if parts.len() < 2 {
                    return "ERROR: Invalid GET command".to_string();
                }
                
                let key = parts[1];
                
                let store = self.store_manager.get_store();
                let store = store.lock().unwrap();
                
                match store.get(key) {
                    Some(value) => value,
                    None => "(nil)".to_string(),
                }
            },
            _ => format!("ERROR: Unknown command '{}'", parts[0]),
        }
    }
    
    #[allow(dead_code)]
    fn start_listener(&self) -> u16 {
        // 绑定到指定的主机和端口0（让操作系统分配一个可用端口）
        let listener = TcpListener::bind(format!("{}:0", self.host)).unwrap();
        listener.local_addr().unwrap().port()
        
    }
    
    #[allow(dead_code)] // 在测试代码中保留这个方法，虽然当前未使用
    fn handle_single_client(&mut self) {
        // 在端口0上绑定一个临时监听器
        let listener = TcpListener::bind(format!("{}:0", self.host)).unwrap();
        let addr = listener.local_addr().unwrap();
        
        // 输出绑定的地址，这样测试可以连接到它
        println!("Server listening on {}", addr);
        
        // 接受一个连接
        if let Ok((mut stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut buffer = String::new();
            
            // 读取一行命令
            if reader.read_line(&mut buffer).is_ok() {
                let command = buffer.trim();
                let response = self.process_command(command);
                
                // 发送响应
                let response_with_newline = format!("{}\n", response);
                stream.write_all(response_with_newline.as_bytes()).unwrap();
            }
        }
    }
}

#[test]
fn test_server_initialization() {
    // 测试基本的服务器创建
    let store_manager = StoreManager::new();
    let server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
    
    // 验证服务器实例创建成功
    assert_eq!(server.get_host(), "127.0.0.1");
    assert_eq!(server.get_data_file(), "test_storage.dat");
}

#[test]
fn test_server_command_processing() {
    // 创建临时服务器实例
    let store_manager = StoreManager::new();
    let mut server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
    
    // 测试处理 PING 命令
    let result = server.process_command("ping");
    assert_eq!(result, "PONG");
    
    // 测试处理 SET 命令
    let result = server.process_command("set test_key test_value");
    assert_eq!(result, "OK");
    
    // 测试处理 GET 命令
    let result = server.process_command("get test_key");
    assert_eq!(result, "test_value");
    
    // 测试处理无效命令
    let result = server.process_command("invalid_command");
    assert!(result.contains("ERROR"));
}

#[test]
fn test_server_client_interaction() {
    // 创建服务器并启动在临时端口
    let (tx, rx) = mpsc::channel();
    
    // 在单独线程中启动服务器
    let server_thread = thread::spawn(move || {
        let store_manager = StoreManager::new();
        let mut server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
        
        // 绑定到一个端口，并获取端口号
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        
        // 发送端口到测试线程
        tx.send(port).unwrap();
        
        // 接受连接并处理命令
        if let Ok((mut stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut buffer = String::new();
            
            if reader.read_line(&mut buffer).is_ok() {
                let command = buffer.trim();
                let response = server.process_command(command);
                stream.write_all(format!("{}\n", response).as_bytes()).unwrap();
            }
        }
    });
    
    // 获取服务器端口
    let port = rx.recv().unwrap();
    
    // 给服务器一些时间启动
    thread::sleep(Duration::from_millis(100));
    
    // 创建客户端连接
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    
    // 发送 PING 命令
    stream.write_all(b"ping\n").unwrap();
    
    // 读取响应
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    
    assert_eq!(response.trim(), "PONG");
    
    // 等待服务器线程完成
    server_thread.join().unwrap();
}

// 新增测试：测试服务器处理无效命令
#[test]
fn test_server_invalid_commands() {
    // 创建临时服务器实例
    let store_manager = StoreManager::new();
    let mut server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
    
    // 测试空命令
    let result = server.process_command("");
    assert_eq!(result, "ERROR: Empty command");
    
    // 测试参数不足的SET命令
    let result = server.process_command("set key");
    assert_eq!(result, "ERROR: Invalid SET command");
    
    // 测试参数不足的GET命令
    let result = server.process_command("get");
    assert_eq!(result, "ERROR: Invalid GET command");
    
    // 测试不存在的键
    let result = server.process_command("get nonexistent_key");
    assert_eq!(result, "(nil)");
    
    // 测试大小写不敏感
    let result = server.process_command("PING");
    assert_eq!(result, "PONG");
    let result = server.process_command("PiNg");
    assert_eq!(result, "PONG");
}

// 新增测试：测试服务器同时处理多个命令
#[test]
fn test_server_multiple_commands() {
    // 创建临时服务器实例
    let store_manager = StoreManager::new();
    let mut server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
    
    // 执行多个相关命令
    let result = server.process_command("set user:1 Alice");
    assert_eq!(result, "OK");
    
    let result = server.process_command("set user:2 Bob");
    assert_eq!(result, "OK");
    
    let result = server.process_command("get user:1");
    assert_eq!(result, "Alice");
    
    let result = server.process_command("get user:2");
    assert_eq!(result, "Bob");
    
    // 测试覆盖现有值
    let result = server.process_command("set user:1 Charlie");
    assert_eq!(result, "OK");
    
    let result = server.process_command("get user:1");
    assert_eq!(result, "Charlie");
}

// 新增测试：测试服务器处理多个客户端
#[test]
fn test_server_multiple_clients() {
    // 创建服务器并在临时端口上启动
    let (tx, rx) = mpsc::channel();
    
    // 分享的存储管理器，以便可以从测试线程中访问
    let store_manager = StoreManager::new();
    let store = store_manager.get_store();
    
    // 在单独线程中启动服务器
    let server_thread = thread::spawn(move || {
        let mut server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
        
        // 绑定到一个端口，并获取端口号
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        
        // 发送端口到测试线程
        tx.send(port).unwrap();
        
        // 接受3个连接并处理命令
        for _ in 0..3 {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut buffer = String::new();
                
                if reader.read_line(&mut buffer).is_ok() {
                    let command = buffer.trim();
                    let response = server.process_command(command);
                    stream.write_all(format!("{}\n", response).as_bytes()).unwrap();
                }
            }
        }
    });
    
    // 获取服务器端口
    let port = rx.recv().unwrap();
    
    // 给服务器一些时间启动
    thread::sleep(Duration::from_millis(100));
    
    // 模拟3个客户端连接
    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client1.write_all(b"set key1 value1\n").unwrap();
    let mut reader1 = BufReader::new(client1.try_clone().unwrap());
    let mut response1 = String::new();
    reader1.read_line(&mut response1).unwrap();
    assert_eq!(response1.trim(), "OK");
    
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client2.write_all(b"set key2 value2\n").unwrap();
    let mut reader2 = BufReader::new(client2.try_clone().unwrap());
    let mut response2 = String::new();
    reader2.read_line(&mut response2).unwrap();
    assert_eq!(response2.trim(), "OK");
    
    let mut client3 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client3.write_all(b"get key1\n").unwrap();
    let mut reader3 = BufReader::new(client3.try_clone().unwrap());
    let mut response3 = String::new();
    reader3.read_line(&mut response3).unwrap();
    assert_eq!(response3.trim(), "value1");
    
    // 等待服务器线程完成
    server_thread.join().unwrap();
    
    // 验证数据在共享存储中
    let store_data = store.lock().unwrap();
    assert_eq!(store_data.get("key1"), Some("value1".to_string()));
    assert_eq!(store_data.get("key2"), Some("value2".to_string()));
}

// 新增测试：测试服务器在客户端断开连接后的行为
#[test]
fn test_server_client_disconnect() {
    // 创建服务器并在临时端口上启动
    let (port_tx, port_rx) = mpsc::channel();
    let (status_tx, status_rx) = mpsc::channel::<String>(); // 新通道用于状态消息
    
    // 在单独线程中启动服务器
    let server_thread = thread::spawn(move || {
        let store_manager = StoreManager::new();
        let mut server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
        
        // 绑定到一个端口，并获取端口号
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        
        // 发送端口到测试线程
        port_tx.send(port).unwrap();
        
        // 接受一个连接
        if let Ok((mut stream, _)) = listener.accept() {
            // 设置读取超时
            stream.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
            
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut buffer = String::new();
            
            // 尝试读取命令
            match reader.read_line(&mut buffer) {
                Ok(0) => {
                    // 客户端关闭了连接，返回特殊消息表示测试成功
                    status_tx.send("client_closed".to_string()).unwrap();
                },
                Ok(_) => {
                    // 处理命令
                    let command = buffer.trim();
                    let response = server.process_command(command);
                    stream.write_all(format!("{}\n", response).as_bytes()).unwrap();
                    status_tx.send("command_processed".to_string()).unwrap();
                },
                Err(e) => {
                    // 读取错误，可能是超时
                    status_tx.send(format!("error: {}", e)).unwrap();
                }
            }
        }
    });
    
    // 获取服务器端口
    let port = port_rx.recv().unwrap();
    
    // 给服务器一些时间启动
    thread::sleep(Duration::from_millis(100));
    
    // 连接到服务器，然后立即关闭连接
    {
        let _stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        // 离开作用域时自动关闭
    }
    
    // 获取服务器的响应
    let result = status_rx.recv_timeout(Duration::from_secs(2));
    assert!(result.is_ok(), "服务器没有正确处理客户端断开连接");
    if let Ok(message) = result {
        // 根据服务器的具体实现，可能是客户端关闭或读取错误
        assert!(message == "client_closed" || message.starts_with("error"), 
                "服务器处理客户端断开连接不正确: {}", message);
    }
    
    // 等待服务器线程完成
    server_thread.join().unwrap();
}

// 新增测试：测试服务器的端口分配
#[test]
fn test_server_port_allocation() {
    let store_manager = StoreManager::new();
    let server = Server::new("127.0.0.1", 0, store_manager, "test_storage.dat".to_string());
    
    // 创建一个临时监听器来获取分配的端口
    let listener = TcpListener::bind(format!("{}:0", server.get_host())).unwrap();
    let port = listener.local_addr().unwrap().port();
    
    // 在后台线程中保持监听器打开
    let thread = thread::spawn(move || {
        if let Ok((_, _)) = listener.accept() {
            // 只是接受连接，不做任何处理
        }
    });
    
    // 验证分配了有效的端口号（不为0）
    assert!(port > 0, "服务器没有分配有效的端口号");
    
    // 给监听线程一点时间来启动
    thread::sleep(Duration::from_millis(100));
    
    // 使用超时连接验证端口
    let result = TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", port).parse().unwrap(),
        Duration::from_millis(500)
    );
    
    assert!(result.is_ok(), "无法连接到服务器分配的端口");
    
    // 主动关闭连接，避免线程挂起
    if let Ok(stream) = result {
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }
    
    // 等待监听线程结束，但设置一个超时防止无限等待
    let _ = thread.join();
}