/**
 * 中央交易系统 — REST 管理员路由
 *
 * 提供管理员监控和配置功能：
 *   POST /admin/stocks/:stockCode/price-limit   设置涨跌停幅度
 *   POST /admin/stocks/:stockCode/suspend        暂停交易
 *   POST /admin/stocks/:stockCode/resume          重启交易
 *   GET  /admin/stocks/:stockCode/orders          查看委托簿
 *   GET  /admin/kafka/status                      查看 Kafka 状态
 */

const express = require("express");
const { getBookSnapshot } = require("../engine/matching-engine");
const { updateLimitRate, refreshAllLimits } = require("../engine/price-limiter");
const stockService = require("../services/stock-service");
const { getKafkaStatus } = require("../kafka/index");
const { TRADE_STATUS } = require("../utils/constants");
const logger = require("../utils/logger");

const router = express.Router();

/**
 * POST /admin/stocks/:stockCode/price-limit
 * 设置涨跌停幅度（次日生效）
 */
router.post("/stocks/:stockCode/price-limit", async (req, res, next) => {
  try {
    const { stockCode } = req.params;
    const { stockType, limitRate } = req.body;

    if (!stockType || limitRate === undefined) {
      return res.status(400).json({
        success: false,
        message: "需要 stockType (NORMAL/ST) 和 limitRate",
      });
    }

    await updateLimitRate(stockType, Number(limitRate));

    res.json({
      success: true,
      message: `${stockType} 类型涨跌停幅度已设置为 ${(Number(limitRate) * 100).toFixed(1)}%（次日生效）`,
    });
  } catch (err) {
    next(err);
  }
});

/**
 * POST /admin/stocks/:stockCode/suspend
 * 暂停股票交易
 */
router.post("/stocks/:stockCode/suspend", async (req, res, next) => {
  try {
    const { stockCode } = req.params;
    await stockService.setTradeStatus(stockCode, TRADE_STATUS.SUSPENDED);

    res.json({
      success: true,
      message: `${stockCode} 交易已暂停`,
    });
  } catch (err) {
    next(err);
  }
});

/**
 * POST /admin/stocks/:stockCode/resume
 * 重启股票交易
 */
router.post("/stocks/:stockCode/resume", async (req, res, next) => {
  try {
    const { stockCode } = req.params;
    await stockService.setTradeStatus(stockCode, TRADE_STATUS.TRADING);

    // 重启后立即推送行情通知
    await stockService.queryAndSendQuote(stockCode);

    res.json({
      success: true,
      message: `${stockCode} 交易已重启`,
    });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /admin/stocks/:stockCode/orders
 * 查看委托簿（管理员监控）
 */
router.get("/stocks/:stockCode/orders", async (req, res, next) => {
  try {
    const snapshot = getBookSnapshot(req.params.stockCode);
    res.json({ success: true, data: snapshot });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /admin/kafka/status
 * 查看 Kafka 连接状态
 */
router.get("/kafka/status", (req, res) => {
  res.json({ success: true, data: getKafkaStatus() });
});

/**
 * POST /admin/price-limits/refresh
 * 手动刷新涨跌停缓存
 */
router.post("/price-limits/refresh", async (req, res, next) => {
  try {
    await refreshAllLimits();
    res.json({ success: true, message: "涨跌停缓存已刷新" });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
