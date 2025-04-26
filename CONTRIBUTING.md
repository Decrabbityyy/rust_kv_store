# Rust KV Store 贡献指南

感谢您对 Rust KV Store 项目的关注！这份文档提供了参与项目开发的指南。

## 目录

- [详细文件结构](#详细文件结构)
- [核心组件说明](#核心组件说明)
- [设计决策与实现细节](#设计决策与实现细节)
- [测试](#测试)
- [贡献步骤](#贡献步骤)
- [编码规范](#编码规范)

## 详细文件结构

```
rust_kv_store/
├── Cargo.toml              # 工作区配置文件
├── Cargo.lock              # 依赖锁定文件
├── README.md               # 项目说明文档（面向用户）
├── CONTRIBUTING.md         # 贡献指南（本文档）
├── tarpaulin-report.html   # 测试覆盖率报告
│
├── config/                 # 全局配置目录
│   └── default.toml        # 默认配置文件
│
├── data/                   # 数据存储目录
│   └── storage.dat         # 持久化数据文件
│
├── logs/                   # 日志文件目录
│   └── server.log          # 服务器日志
│
├── kv-common/              # 核心共享库
│   ├── Cargo.toml          # 库配置文件
│   ├── config/             # 库专用配置
│   │   └── default.toml    # 默认配置模板
│   ├── src/                # 源代码
│   │   ├── lib.rs          # 库入口点
│   │   ├── command.rs      # 命令解析和执行
│   │   ├── config.rs       # 配置管理
│   │   ├── logger.rs       # 日志系统
│   │   └── store/          # 存储引擎
│   │       └── mod.rs      # 存储引擎实现
│   └── tests/              # 测试文件
│       ├── command_tests.rs # 命令测试
│       ├── config_tests.rs  # 配置测试
│       ├── logger_tests.rs  # 日志测试
│       └── store_tests.rs   # 存储引擎测试
│
├── kv-server/              # 服务端应用
│   ├── Cargo.toml          # 服务端配置文件
│   ├── src/                # 源代码
│   │   ├── main.rs         # 服务端入口点
│   │   └── server.rs       # 服务端逻辑
│   └── tests/              # 测试文件
│       └── server_tests.rs # 服务端测试
│
└── kv-client/              # 客户端应用
    ├── Cargo.toml          # 客户端配置文件
    ├── src/                # 源代码
    │   ├── main.rs         # 客户端入口点
    │   └── client.rs       # 客户端逻辑
    └── tests/              # 测试文件
        └── client_tests.rs # 客户端测试
```

## 核心组件说明

- **存储引擎 (store/mod.rs)**：
  - 负责数据的内存存储和持久化
  - 支持字符串、列表和哈希表三种数据类型
  - 实现了低频数据的内存优化（将不常用数据移至磁盘）
  - 提供过期时间机制

- **命令处理 (command.rs)**：
  - 解析和执行客户端发送的命令
  - 提供命令帮助系统
  - 处理各种数据类型的操作命令

- **配置管理 (config.rs)**：
  - 从配置文件加载设置
  - 提供默认配置值
  - 支持命令行参数覆盖

- **日志系统 (logger.rs)**：
  - 配置日志级别和输出
  - 记录系统运行状态和错误

- **服务端 (server.rs)**：
  - 处理客户端连接
  - 解析命令并响应
  - 管理数据持久化

- **客户端 (client.rs)**：
  - 连接服务器
  - 发送命令和接收响应
  - 提供交互式命令行界面

## 设计决策与实现细节

### 存储引擎的设计

- **低频数据管理**：系统使用基于访问频率和时间的算法，将不常用的数据移到磁盘，以优化内存使用。这些数据通过Base64编码的键名保存为独立的JSON文件。
- **可变方法的使用**：即使是读取操作（如`get_string`）也需要可变引用，因为它们会更新访问统计和处理过期键。
- **过期机制**：使用Unix时间戳实现键的过期功能，系统会在访问键时检查过期状态。

### 并发与线程安全

- **Mutex保护**：存储引擎使用`Arc<Mutex<Store>>`实现线程安全，允许多个客户端连接安全地访问数据。
- **背景任务**：系统支持后台定期检查低频数据和过期键。

### 未来扩展方向

- **可靠性增强**：添加事务支持和数据完整性检查
- **性能优化**：实现更复杂的缓存策略和并发控制
- **功能扩展**：增加更多数据结构（如集合、有序集合）

## 测试

项目包含全面的单元测试和集成测试，覆盖了核心功能。

### 运行测试

运行所有测试：
```bash
cargo test --workspace
```

运行特定组件的测试：
```bash
cargo test -p kv-common  # 运行共享库的测试
cargo test -p kv-server  # 运行服务器的测试
cargo test -p kv-client  # 运行客户端的测试
```

### 测试覆盖率

项目使用 `cargo-tarpaulin` 工具生成测试覆盖率报告。当前测试覆盖率约为52%。

安装 tarpaulin：
```bash
cargo install cargo-tarpaulin
```

生成覆盖率报告：
```bash
cargo tarpaulin --workspace --out Html
```

生成的报告 `tarpaulin-report.html` 包含详细的覆盖率信息，显示了哪些代码路径已被测试覆盖。

### 主要测试模块

- **store_tests**: 测试存储引擎的核心功能，包括字符串、列表和哈希表操作
- **command_tests**: 测试命令解析和执行功能
- **config_tests**: 测试配置加载和默认值处理
- **logger_tests**: 测试日志系统的初始化和级别设置
- **client_tests**: 测试客户端的连接和命令发送功能
- **server_tests**: 测试服务器的启动和请求处理功能

## 贡献步骤

1. Fork本仓库
2. 创建您的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建一个Pull Request

## 编码规范

- 遵循Rust标准代码风格
- 使用`cargo fmt`格式化代码
- 使用`cargo clippy`检查代码质量
- 为新功能编写测试，保持或提高测试覆盖率
- 更新文档以反映您的更改