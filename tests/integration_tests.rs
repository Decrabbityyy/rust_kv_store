use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufReader, BufRead};
use std::thread;
use std::time::Duration;
use std::sync::mpsc;

// 这个测试会模拟服务器和客户端之间的交互
#[test]
fn test_client_server_communication() {
    // 1. 启动一个测试服务器
    let (tx, rx) = mpsc::channel();
    
    let server_thread = thread::spawn(move || {
        // 在随机端口上启动临时服务器
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        
        // 发送服务器地址到主测试线程
        tx.send(addr).unwrap();
        
        // 接受一个连接
        if let Ok((mut stream, _)) = listener.accept() {
            let mut reader = BufReader::new(&stream);
            let mut buffer = String::new();
            
            // 读取客户端发送的命令
            reader.read_line(&mut buffer).unwrap();
            
            // 处理命令 (假设是 "ping")
            if buffer.trim() == "ping" {
                stream.write_all(b"PONG\n").unwrap();
            } else if buffer.trim().starts_with("set key1") {
                stream.write_all(b"OK\n").unwrap();
                
                // 清空缓冲区以接收下一个命令
                buffer.clear();
                reader.read_line(&mut buffer).unwrap();
                
                // 处理获取命令
                if buffer.trim() == "get key1" {
                    stream.write_all(b"value1\n").unwrap();
                }
            }
        }
    });
    
    // 获取服务器地址
    let server_addr = rx.recv().unwrap();
    
    // 等待服务器启动
    thread::sleep(Duration::from_millis(100));
    
    // 2. 创建客户端连接并测试
    let mut client = TcpStream::connect(server_addr).unwrap();
    
    // 测试 PING 命令
    client.write_all(b"ping\n").unwrap();
    
    let mut reader = BufReader::new(&client);
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    
    assert_eq!(response.trim(), "PONG");
    
    // 测试 SET 和 GET 命令
    client.write_all(b"set key1 value1\n").unwrap();
    
    response.clear();
    reader.read_line(&mut response).unwrap();
    assert_eq!(response.trim(), "OK");
    
    client.write_all(b"get key1\n").unwrap();
    
    response.clear();
    reader.read_line(&mut response).unwrap();
    assert_eq!(response.trim(), "value1");
    
    // 等待服务器线程完成
    server_thread.join().unwrap();
}

// 测试服务器的并发处理能力
#[test]
fn test_server_concurrency() {
    // 启动一个测试服务器
    let (tx, rx) = mpsc::channel();
    
    let server_thread = thread::spawn(move || {
        // 在随机端口上启动临时服务器
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        
        // 发送服务器地址到主测试线程
        tx.send(addr).unwrap();
        
        // 接受多个连接
        for _ in 0..3 {
            if let Ok((mut stream, _)) = listener.accept() {
                thread::spawn(move || {
                    let mut reader = BufReader::new(&stream);
                    let mut buffer = String::new();
                    
                    // 读取客户端发送的命令
                    reader.read_line(&mut buffer).unwrap();
                    
                    // 处理命令 (假设是 "ping")
                    if buffer.trim() == "ping" {
                        stream.write_all(b"PONG\n").unwrap();
                    }
                });
            }
        }
        
        // 保持服务器运行一小段时间
        thread::sleep(Duration::from_millis(500));
    });
    
    // 获取服务器地址
    let server_addr = rx.recv().unwrap();
    
    // 等待服务器启动
    thread::sleep(Duration::from_millis(100));
    
    // 创建多个并发客户端
    let mut handles = vec![];
    
    for _ in 0..3 {
        let handle = thread::spawn(move || {
            let mut client = TcpStream::connect(server_addr).unwrap();
            
            // 测试 PING 命令
            client.write_all(b"ping\n").unwrap();
            
            let mut reader = BufReader::new(&client);
            let mut response = String::new();
            reader.read_line(&mut response).unwrap();
            
            assert_eq!(response.trim(), "PONG");
        });
        
        handles.push(handle);
    }
    
    // 等待所有客户端线程完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    // 等待服务器线程完成
    server_thread.join().unwrap();
}