/**
 * 中央交易系统 — Express 应用入口
 *
 * 启动流程：
 *  1. 加载环境变量
 *  2. 启动 Express HTTP 服务
 *  3. 初始化 Kafka（消费者 + 生产者）
 *  4. 刷新涨跌停缓存
 *  5. 启动指令过期定时任务
 */

require("dotenv").config();

const express = require("express");
const cors = require("cors");

const ordersRouter = require("./routes/orders");
const stocksRouter = require("./routes/stocks");
const adminRouter = require("./routes/admin");

const { initKafka } = require("./kafka/index");
const { refreshAllLimits } = require("./engine/price-limiter");
const { startExpiryJob, manualExpire } = require("./scheduler/expiry-job");
const orderService = require("./services/order-service");
const logger = require("./utils/logger");

const app = express();
const port = Number(process.env.PORT || 8082);

// ======================== 中间件 ========================
app.use(cors());
app.use(express.json());

// ======================== 路由 ========================

// 健康检查
app.get("/api/central-trading/health", (req, res) => {
  res.json({ ok: true, service: "central-trading-system" });
});

// 委托相关
app.use("/api/central-trading/orders", ordersRouter);

// 股票行情
app.use("/api/central-trading/stocks", stocksRouter);

// 管理员接口
app.use("/api/central-trading/admin", adminRouter);

// 手动触发过期（测试用）
app.post("/api/central-trading/admin/expire", async (req, res, next) => {
  try {
    await manualExpire();
    res.json({ success: true, message: "手动过期清理已执行" });
  } catch (err) {
    next(err);
  }
});

// ======================== 错误处理 ========================
app.use((err, req, res, next) => {
  logger.error("Request error:", err.message, err.stack);
  const statusCode = err.statusCode || 500;
  res.status(statusCode).json({
    success: false,
    message: statusCode === 500 ? "中央交易系统内部错误" : err.message,
  });
});

// ======================== 启动 ========================
app.listen(port, async () => {
  logger.info(`中央交易系统 HTTP 服务已启动: http://localhost:${port}`);

  // 1. 刷新涨跌停缓存
  try {
    await refreshAllLimits();
  } catch (err) {
    logger.warn("涨跌停缓存刷新失败（数据库可能未就绪）:", err.message);
  }

  // 2. 初始化 Kafka
  const kafkaResult = await initKafka({
    onOrderCommand: orderService.receiveOrder,
    onCancelCommand: orderService.cancelOrder,
    onStockQuery: orderService.handleStockQuery,
  });

  if (kafkaResult.ok) {
    logger.info("Kafka 消息管道已就绪");
  } else {
    logger.warn(`Kafka 未启动: ${kafkaResult.message}`);
    logger.info("系统将以 HTTP-only 模式运行，委托通过 REST API 提交");
  }

  // 3. 启动指令过期定时任务
  startExpiryJob();

  logger.info("=== 中央交易系统启动完成 ===");
  logger.info(`REST API:  http://localhost:${port}/api/central-trading/`);
  logger.info(`健康检查:  http://localhost:${port}/api/central-trading/health`);
  logger.info(`管理接口:  http://localhost:${port}/api/central-trading/admin/`);
});
