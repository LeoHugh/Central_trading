# 股票中央交易系统 (Central Trading System)

中央交易系统是整个股票交易过程的核心部分。所有投资者发出的买卖指令在此系统中进行自动撮合，根据**价格优先**和**时间优先**原则成交，并将成交记录反馈到证券账户和资金账户。

## 技术栈

- **运行时**: Java 17
- **框架**: Spring Boot 3.2+
- **数据库**: MySQL 8.0+
- **消息队列**: Apache Kafka (Spring Kafka)

## 快速开始

### 1. 安装依赖

```bash
cd Central_trading
mvn clean install -DskipTests
```

### 2. 配置环境变量

修改 `src/main/resources/application.yml` 或通过环境变量配置（如 `DB_HOST`, `DB_PASSWORD`, `KAFKA_BROKERS` 等）。

### 3. 初始化数据库

项目启动时会自动执行 `src/main/resources/schema.sql`，无需手动初始化。

### 4. 启动

```bash
mvn spring-boot:run
```

系统启动后监听 `http://localhost:8082`。

## 核心功能

### 撮合引擎

- **价格优先**：买方价格高优先成交，卖方价格低优先成交
- **时间优先**：同价按进入系统时间排序
- **中间价格算法**：成交价 = (买价 + 卖价) / 2
- **涨跌停修正**：成交价超出限制时以限制价格为准
- **加权价格算法**：一条指令多次成交时计算加权平均价

### 涨跌停限制

| 股票类型 | 涨跌幅 | 计算方式 |
|----------|--------|----------|
| 普通股 (NORMAL) | 10% | 昨日收盘价 × (1 ± 0.10) |
| ST 股票 | 5% | 昨日收盘价 × (1 ± 0.05) |

### 指令过期

收盘后自动将当日未完全成交的委托标记为 EXPIRED，释放冻结的资金和持仓。

## REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/central-trading/orders` | 提交委托 |
| `POST` | `/api/central-trading/orders/{id}/cancel` | 撤销委托 |
| `GET` | `/api/central-trading/orders/{id}/result` | 查询成交结果 |
| `GET` | `/api/central-trading/stocks?keyword=` | 查询股票行情 |
| `GET` | `/api/central-trading/stocks/{code}` | 查询单只股票 |
| `POST` | `/api/central-trading/admin/stocks/{code}/suspend` | 暂停交易 |
| `POST` | `/api/central-trading/admin/stocks/{code}/resume` | 重启交易 |
| `GET` | `/api/central-trading/admin/stocks/{code}/orders` | 查看委托簿 |
| `POST` | `/api/central-trading/admin/stocks/{code}/price-limit` | 设置涨跌停 |
| `POST` | `/api/central-trading/admin/price-limits/refresh` | 刷新涨跌停缓存 |
| `GET` | `/api/central-trading/admin/kafka/status` | 获取 Kafka 状态 |

## Kafka Topics

| Topic | 方向 | 用途 |
|-------|------|------|
| `central.order.command` | 入站 | 接收买卖委托 |
| `central.cancel.command` | 入站 | 接收撤单请求 |
| `central.stock.query` | 入站 | 接收行情查询 |
| `client.stock.quote` | 出站 | 推送行情数据 |
| `client.trade.report` | 出站 | 推送成交反馈 |
| `client.order.report` | 出站 | 推送委托状态 |
| `webinfo.trade.report` | 出站 | 推送给 Web 端展示的成交报告 |

## 目录结构

```
Central_trading/
├── src/main/resources/
│   ├── schema.sql               # 数据库建表脚本
│   └── application.yml          # Spring Boot 配置文件
├── pom.xml                      # Maven 依赖配置
├── src/main/java/com/trading/central/
│   ├── CentralTradingApplication.java # Spring Boot 入口
│   ├── controller/              # HTTP API 层 (Order, Stock, Admin)
│   ├── engine/                  # 核心撮合逻辑 (MatchingEngine, OrderBook, PriceLimiter)
│   ├── kafka/                   # 消息队列消费者和生产者
│   ├── service/                 # 业务逻辑服务
│   ├── scheduler/               # 定时任务 (ExpiryJob)
│   ├── model/                   # 数据模型和 DTO
│   └── util/                    # 工具类与常量
```

## 联调配置

交易客户端联调时，在浏览器控制台执行：

```js
localStorage.setItem("centralTradingApiBase", "http://localhost:8082");
location.reload();
```

## 外部依赖

- **资金账户系统** (`ACCOUNT_API_BASE`): 冻结/释放资金和持仓
- **Kafka Broker** (`KAFKA_BROKERS`): 消息中间件
- **MySQL** (`DB_HOST`): 持久化存储

当外部服务不可用时，设置 `ACCOUNT_API_MOCK=true` (在 `application.yml` 中或环境变量配置) 使用模拟模式。另外可通过 `app.kafka.enabled=false` 禁用 Kafka。
