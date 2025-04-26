use log::{error, info};
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(windows)]
use winapi::um::consoleapi::SetConsoleCtrlHandler;

pub struct Client {
    host: String,
    port: u16,
    connected: Arc<AtomicBool>,
    stream: Option<TcpStream>,
    response_rx: Option<Receiver<String>>,
}

impl Client {
    pub fn new(host: String, port: u16) -> Self {
        Client {
            host,
            port,
            connected: Arc::new(AtomicBool::new(false)),
            stream: None,
            response_rx: None,
        }
    }

    // 连接到服务器
    pub fn connect(&mut self) -> Result<(), String> {
        let addr = format!("{}:{}", self.host, self.port);
        info!("尝试连接到服务器: {}", addr);

        // 尝试建立连接
        let stream = TcpStream::connect(&addr)
            .map_err(|e| format!("无法连接到服务器 {}: {}", addr, e))?;

        info!("已连接到服务器: {}", addr);
        self.connected.store(true, Ordering::SeqCst);

        // 创建一个通道来接收响应
        let (tx, rx) = mpsc::channel();
        self.response_rx = Some(rx);

        // 启动接收线程
        let mut stream_clone = stream.try_clone()
            .map_err(|e| format!("克隆流失败: {}", e))?;
        let connected = Arc::clone(&self.connected);

        thread::spawn(move || {
            // 忽略接收线程中的错误，因为用户退出时可能会发生错误
            let _ = Self::receive_responses(&mut stream_clone, connected, tx);
        });

        // 保存流用于后续命令
        self.stream = Some(stream);
        
        // 设置Ctrl+C处理 - 使用单独的线程监听标准输入的中断
        let ctrl_c_connected = Arc::clone(&self.connected);
        thread::spawn(move || {
            // 这个线程将持续检查是否收到Ctrl+C信号
            // 当用户按下Ctrl+C时，Windows会发送一个中断，我们可以在这里捕获它
            if let Err(e) = Self::handle_ctrl_c(ctrl_c_connected) {
                error!("Ctrl+C 处理错误: {}", e);
            }
        });
        
        // 获取流的克隆，用于发送命令
        let stream_for_commands = self.stream.as_ref().unwrap().try_clone()
            .map_err(|e| format!("克隆流失败: {}", e))?;
        
        // 调用 send_commands 处理用户输入
        self.send_commands(stream_for_commands)?;

        Ok(())
    }

    // 专用于测试的连接方法，不会启动命令处理循环
    #[allow(dead_code)]
    pub fn connect_for_test(&mut self) -> Result<(), String> {
        let addr = format!("{}:{}", self.host, self.port);
        info!("尝试连接到服务器: {}", addr);

        // 尝试建立连接
        let stream = TcpStream::connect(&addr)
            .map_err(|e| format!("无法连接到服务器 {}: {}", addr, e))?;

        info!("已连接到服务器: {}", addr);
        self.connected.store(true, Ordering::SeqCst);

        // 创建一个通道来接收响应
        let (tx, rx) = mpsc::channel();
        self.response_rx = Some(rx);

        // 启动接收线程
        let mut stream_clone = stream.try_clone()
            .map_err(|e| format!("克隆流失败: {}", e))?;
        let connected = Arc::clone(&self.connected);

        thread::spawn(move || {
            // 忽略接收线程中的错误，因为用户退出时可能会发生错误
            let _ = Self::receive_responses(&mut stream_clone, connected, tx);
        });

        // 保存流用于后续命令
        self.stream = Some(stream);
        
        Ok(())
    }

    // 处理Ctrl+C信号
    fn handle_ctrl_c(connected: Arc<AtomicBool>) -> Result<(), String> {
        #[cfg(windows)]
        {
            // 创建一个全局状态用于共享 connected 变量
            static mut CONNECTED_PTR: *mut Arc<AtomicBool> = std::ptr::null_mut();
            unsafe {
                CONNECTED_PTR = Box::into_raw(Box::new(connected));
            }
            
            // 定义控制台事件处理函数
            extern "system" fn handler(_: u32) -> i32 {
                let connected = unsafe { &*CONNECTED_PTR };
                println!("\n接收到 Ctrl+C 信号，正在关闭连接...");
                connected.store(false, Ordering::SeqCst);
                
                // 在这里直接退出程序，但先给一点时间关闭连接
                thread::spawn(move || {
                    // 等待一小段时间，让其他线程有机会关闭连接
                    thread::sleep(Duration::from_millis(200));
                    std::process::exit(0);
                });
                
                1 // 返回 true 表示我们处理了这个事件
            }
            
            // 注册控制台事件处理函数
            if unsafe { SetConsoleCtrlHandler(Some(handler), 1) } == 0 {
                return Err("无法设置控制台事件处理器".to_string());
            }
            
            Ok(())
        }
        
        #[cfg(not(windows))]
        {
            // 非Windows平台上的简单实现
            // 使用单独的线程来监听标准输入，检测到 EOF 时表示 Ctrl+C
            let (tx, rx) = mpsc::channel();
            
            thread::spawn(move || {
                let stdin = io::stdin();
                let mut buffer = String::new();
                while stdin.read_line(&mut buffer).is_ok() {
                    buffer.clear();
                }
                // 如果到达这里，说明输入流被中断
                let _ = tx.send(());
            });
            
            // 等待信号
            thread::spawn(move || {
                if rx.recv().is_ok() {
                    println!("\n接收到中断信号，正在关闭连接...");
                    connected.store(false, Ordering::SeqCst);
                    
                    // 在这里直接退出程序，但先给一点时间关闭连接
                    thread::spawn(move || {
                        // 等待一小段时间，让其他线程有机会关闭连接
                        thread::sleep(Duration::from_millis(200));
                        std::process::exit(0);
                    });
                }
            });
            
            Ok(())
        }
    }

    // 用于测试的方法：发送单个命令并返回响应
    #[allow(dead_code)]
    pub fn send_command_with_response(&mut self, command: &str) -> Result<String, String> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err("未连接到服务器".to_string());
        }

        if let Some(stream) = &mut self.stream {
            // 在发送前清空响应通道中的任何剩余消息
            if let Some(rx) = &self.response_rx {
                // 非阻塞方式清空通道
                while rx.try_recv().is_ok() {
                    // 忽略旧消息
                }
            }
            
            // 发送带换行符的命令
            let command = format!("{}\n", command);
            stream.write_all(command.as_bytes())
                .map_err(|e| format!("发送命令失败: {}", e))?;
            stream.flush()
                .map_err(|e| format!("刷新流失败: {}", e))?;

            // 从响应通道接收响应，使用较短的超时
            if let Some(rx) = &self.response_rx {
                match rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(response) => Ok(response.trim().to_string()),
                    Err(_) => Err("接收响应超时".to_string())
                }
            } else {
                Err("响应通道未初始化".to_string())
            }
        } else {
            Err("流未初始化".to_string())
        }
    }

    // 发送命令到服务器
    fn send_commands(&self, mut stream: TcpStream) -> Result<(), String> {
        let stdin = io::stdin();
        let mut reader = stdin.lock();
        let mut buffer = String::new();

        println!("已连接到服务器。输入命令或输入 'exit' 退出。");
        println!("输入 'help' 获取可用命令列表。");

        while self.connected.load(Ordering::SeqCst) {
            print!("> ");
            io::stdout().flush().map_err(|e| format!("刷新标准输出失败: {}", e))?;

            buffer.clear();
            if reader.read_line(&mut buffer).map_err(|e| format!("读取输入失败: {}", e))? == 0 {
                break;
            }

            let command = buffer.trim();
            if command.is_empty() {
                continue;
            }

            if command.eq_ignore_ascii_case("exit") {
                println!("断开连接并退出...");
                self.connected.store(false, Ordering::SeqCst);
                // 在退出前关闭socket，防止产生错误
                let _ = stream.shutdown(std::net::Shutdown::Both);
                break;
            }

            // 每次命令循环开始时检查 connected 标志
            // 这样可以确保当 Ctrl+C 触发时，能够立即响应
            if !self.connected.load(Ordering::SeqCst) {
                println!("正在关闭连接...");
                // 在退出前关闭socket，防止产生错误
                let _ = stream.shutdown(std::net::Shutdown::Both);
                break;
            }

            // 处理 ping 命令，测量延迟
            if command.eq_ignore_ascii_case("ping") {
                let start_time = Instant::now();
                
                // 发送 ping 命令到服务器
                if let Err(e) = stream.write_all(format!("{}\n", command).as_bytes()) {
                    if self.connected.load(Ordering::SeqCst) {
                        error!("发送命令时出错: {}", e);
                        self.connected.store(false, Ordering::SeqCst);
                        return Err(format!("发送命令失败: {}", e));
                    }
                    break;
                }
                
                // 等待响应
                if let Some(rx) = &self.response_rx {
                    match rx.recv_timeout(Duration::from_secs(2)) {
                        Ok(_response) => {
                            // 先等待一小段时间，确保接收线程已经打印了响应
                            thread::sleep(Duration::from_millis(50));
                            let elapsed = start_time.elapsed();
                            println!("延迟: {} 毫秒", elapsed.as_millis());
                        },
                        Err(_) => println!("接收响应超时")
                    }
                }
                
                continue;
            }

            // 发送命令到服务器
            if let Err(e) = stream.write_all(format!("{}\n", command).as_bytes()) {
                // 只有在非正常退出时才显示错误
                if self.connected.load(Ordering::SeqCst) {
                    error!("发送命令时出错: {}", e);
                    self.connected.store(false, Ordering::SeqCst);
                    return Err(format!("发送命令失败: {}", e));
                }
                break;
            }

            // 等待一小段时间，让接收线程有时间处理响应
            thread::sleep(Duration::from_millis(100));
        }

        // 确保在退出时关闭连接
        if !self.connected.load(Ordering::SeqCst) && stream.take_error().is_ok() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }

        Ok(())
    }

    // 接收并显示服务器响应
    fn receive_responses(
        stream: &mut TcpStream, 
        connected: Arc<AtomicBool>,
        tx: Sender<String>
    ) -> Result<(), String> {
        let mut reader = BufReader::new(stream);
        let mut response = String::new();

        while connected.load(Ordering::SeqCst) {
            response.clear();
            match reader.read_line(&mut response) {
                Ok(0) => {
                    // 服务器断开连接
                    if connected.load(Ordering::SeqCst) {
                        println!("服务器断开连接");
                    }
                    connected.store(false, Ordering::SeqCst);
                    break;
                }
                Ok(_) => {
                    // 打印响应，不包括末尾的换行符
                    print!("{}", response);
                    // 忽略刷新错误，不影响程序退出
                    let _ = io::stdout().flush();
                    
                    // 发送响应到通道，用于测试
                    let _ = tx.send(response.clone());
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // 超时但仍然连接
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(e) => {
                    // 只有在非正常退出时才显示错误
                    if connected.load(Ordering::SeqCst) {
                        error!("接收响应时出错: {}", e);
                        connected.store(false, Ordering::SeqCst);
                        return Err(format!("接收响应失败: {}", e));
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}