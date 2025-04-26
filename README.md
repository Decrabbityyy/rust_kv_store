# Rust KV Store

一个基于Rust实现的高性能键值存储系统

[![Language](https://img.shields.io/badge/language-Rust-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![CI Status](https://img.shields.io/github/workflow/status/yourusername/rust_kv_store/Rust%20CI)](https://github.com/yourusername/rust_kv_store/actions)
[![Test Coverage](https://img.shields.io/badge/coverage-52%25-yellow)](tarpaulin-report.html)

## 目录

- [特性](#特性)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [支持的命令](#支持的命令)
- [配置](#配置)
- [测试](#测试)
- [许可证](#许可证)

## 特性

- **客户端/服务端架构** - 独立的客户端和服务端程序
- **多种数据类型支持** - 字符串、列表和哈希表
- **数据持久化** - 自动保存数据到磁盘
- **详细日志** - 可配置的日志级别和输出
- **可配置** - 通过配置文件或命令行参数进行配置

## 项目结构

项目采用工作区（Workspace）架构，分为三个子项目：

| 子项目 | 描述 |
|-------|------|
| **kv-common** | 共享库，包含所有核心功能（存储引擎、命令处理、配置和日志） |
| **kv-server** | 独立的服务端程序，负责处理客户端请求和数据持久化 |
| **kv-client** | 独立的客户端程序，提供命令行界面与服务端交互 |

## 快速开始

### 前置条件

- Rust 和 Cargo (1.65.0+)

### 安装

```bash
# 克隆仓库
git clone https://github.com/yourusername/rust_kv_store.git
cd rust_kv_store

# 构建项目
cargo build --release
```

### 运行服务端

```bash
# 使用cargo运行（默认配置）
cargo run --release -p kv-server

# 指定主机和端口
cargo run --release -p kv-server -- --host 127.0.0.1 --port 7878

# 直接运行编译后的可执行文件
./target/release/kv-server
# 或在Windows上
.\target\release\kv-server.exe

# 指定主机和端口
./target/release/kv-server --host 127.0.0.1 --port 7878
# 或在Windows上
.\target\release\kv-server.exe --host 127.0.0.1 --port 7878
```

### 运行客户端

```bash
# 使用cargo运行（默认配置）
cargo run --release -p kv-client

# 指定服务端地址和端口
cargo run --release -p kv-client -- --host 127.0.0.1 --port 7878

# 直接运行编译后的可执行文件
./target/release/kv-client
# 或在Windows上
.\target\release\kv-client.exe

# 指定服务端地址和端口
./target/release/kv-client --host 127.0.0.1 --port 7878
# 或在Windows上
.\target\release\kv-client.exe --host 127.0.0.1 --port 7878
```

### 开箱即用

程序会自动创建所需的配置文件、数据和日志目录。您无需手动创建任何文件或目录即可开始使用。

## 支持的命令

### 字符串操作

| 命令 | 描述 | 示例 |
|------|-----|------|
| `set <key> <value>` | 存储键值对 | `set name Alice` |
| `get <key>` | 获取键对应的值 | `get name` |
| `del <key>` | 删除键对应的值 | `del name` |

### 列表操作

| 命令 | 描述 | 示例 |
|------|-----|------|
| `lpush <key> <value>` | 在列表左端添加元素 | `lpush mylist Apple` |
| `rpush <key> <value>` | 在列表右端添加元素 | `rpush mylist Banana` |
| `range <key> <start> <end>` | 获取列表指定范围的元素 | `range mylist 0 -1` |
| `len <key>` | 获取列表长度 | `len mylist` |
| `lpop <key>` | 弹出并返回列表左端元素 | `lpop mylist` |
| `rpop <key>` | 弹出并返回列表右端元素 | `rpop mylist` |
| `ldel <key>` | 删除整个列表 | `ldel mylist` |

### 哈希表操作

| 命令 | 描述 | 示例 |
|------|-----|------|
| `hset <key> <field> <value>` | 设置哈希表字段的值 | `hset user:1 name Alice` |
| `hget <key> <field>` | 获取哈希表字段的值 | `hget user:1 name` |
| `hdel <key> <field>` | 删除哈希表字段 | `hdel user:1 name` |
| `hdel <key>` | 删除整个哈希表 | `hdel user:1` |

### 其他命令

| 命令 | 描述 |
|------|------|
| `ping` | 测试服务器连接 |
| `help` | 获取所有命令的帮助信息 |
| `help <command>` | 获取特定命令的帮助信息 |
| `exit` | 断开连接并退出客户端 |

## 配置

配置文件位于 `config/default.toml`，包含以下设置：

```toml
[server]
port = 6379
host = "127.0.0.1"

[persistence]
data_file = "data/storage.dat"
mode = "on_change"
interval_seconds = 300

[logging]
log_file = "logs/server.log"
level = "info"
```

## 测试

项目包含全面的测试套件，确保功能的稳定性和可靠性。

### 运行测试

```bash
# 运行所有测试
cargo test --workspace
```

## 开发与贡献

如果您对项目开发或贡献感兴趣，请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 文件，其中包含详细的：

- 项目文件结构
- 核心组件说明
- 设计决策与实现细节
- 测试覆盖率信息
- 贡献步骤与编码规范

## 许可证

本项目采用 MIT 许可证 - 详情见 [LICENSE](LICENSE) 文件。