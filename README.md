# 股票中央交易系统 (Central Trading System)

中央交易系统是整个股票交易过程的核心部分。所有投资者发出的买卖指令在此系统中进行自动撮合，根据**价格优先**和**时间优先**原则成交，并将成交记录反馈到证券账户和资金账户。

## 技术栈

- **运行时**: Node.js
- **HTTP 框架**: Express
- **数据库**: MySQL 8.0+
- **消息队列**: Apache Kafka (KafkaJS)

## 快速开始

### 1. 安装依赖

```bash
cd Central_trading
npm install
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，配置数据库连接和 Kafka 地址
```

### 3. 初始化数据库

```bash
mysql -u root -p < database/schema.sql
```

### 4. 启动

```bash
npm run dev
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

收盘后（默认 15:00）自动将当日未完全成交的委托标记为 EXPIRED，释放冻结的资金和持仓。

## REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/central-trading/health` | 健康检查 |
| `POST` | `/api/central-trading/orders` | 提交委托 |
| `POST` | `/api/central-trading/orders/:id/cancel` | 撤销委托 |
| `GET` | `/api/central-trading/orders/:id/result` | 查询成交结果 |
| `GET` | `/api/central-trading/stocks?keyword=` | 查询股票行情 |
| `GET` | `/api/central-trading/stocks/:code` | 查询单只股票 |
| `POST` | `/api/central-trading/admin/stocks/:code/suspend` | 暂停交易 |
| `POST` | `/api/central-trading/admin/stocks/:code/resume` | 重启交易 |
| `GET` | `/api/central-trading/admin/stocks/:code/orders` | 查看委托簿 |
| `POST` | `/api/central-trading/admin/stocks/:code/price-limit` | 设置涨跌停 |

## Kafka Topics

| Topic | 方向 | 用途 |
|-------|------|------|
| `central.order.command` | 入站 | 接收买卖委托 |
| `central.cancel.command` | 入站 | 接收撤单请求 |
| `central.stock.query` | 入站 | 接收行情查询 |
| `client.stock.quote` | 出站 | 推送行情数据 |
| `client.trade.report` | 出站 | 推送成交反馈 |
| `client.order.report` | 出站 | 推送委托状态 |

## 目录结构

```
Central_trading/
├── database/schema.sql          # 数据库建表脚本
├── server/
│   ├── app.js                   # Express 入口
│   ├── db.js                    # MySQL 连接池
│   ├── engine/
│   │   ├── order-book.js        # 委托簿（买卖队列）
│   │   ├── matching-engine.js   # 撮合引擎
│   │   └── price-limiter.js     # 涨跌停计算
│   ├── kafka/
│   │   ├── index.js             # Kafka 初始化
│   │   ├── producer.js          # 消息发送
│   │   └── consumer.js          # 消息消费
│   ├── services/
│   │   ├── order-service.js     # 委托生命周期
│   │   ├── trade-service.js     # 成交处理
│   │   ├── stock-service.js     # 行情服务
│   │   └── account-service.js   # 外部账户系统调用
│   ├── routes/
│   │   ├── orders.js            # 委托 API
│   │   ├── stocks.js            # 行情 API
│   │   └── admin.js             # 管理 API
│   ├── scheduler/
│   │   └── expiry-job.js        # 指令过期任务
│   └── utils/
│       ├── constants.js         # 常量定义
│       └── logger.js            # 日志工具
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

当外部服务不可用时，设置 `ACCOUNT_API_MOCK=true` 使用模拟模式。
