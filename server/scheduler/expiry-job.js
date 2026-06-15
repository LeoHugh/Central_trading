/**
 * 中央交易系统 — 指令过期定时任务
 *
 * 业务规则（sharesystem.md 第294-296行）：
 *   当一个交易指令发出以后，如果因为某些条件没有满足，在该交易日内没有成交，
 *   那么在第二天的时候该指令已经过期了，需要从交易系统内移去。
 *
 * 实现策略：
 *   - 每分钟检查一次当前时间是否已过收盘时间
 *   - 收盘后执行一次过期清理
 *   - 清理内容：将未完全成交的委托标记为 EXPIRED，释放冻结资金/持仓
 */

const pool = require("../db");
const { clearBookOrders, getAllStockCodes } = require("../engine/matching-engine");
const accountService = require("../services/account-service");
const { sendOrderReport } = require("../kafka/producer");
const { ORDER_STATUS, SIDE } = require("../utils/constants");
const { resetTradeSeq } = require("../services/trade-service");
const logger = require("../utils/logger");

let intervalId = null;
let lastExpiredDate = ""; // 防止同一天重复执行

/**
 * 启动过期检查定时任务
 */
function startExpiryJob() {
  const checkInterval = 60 * 1000; // 每 60 秒检查一次

  intervalId = setInterval(() => {
    checkAndExpire().catch((err) => {
      logger.error("[ExpiryJob] 过期检查异常:", err.message);
    });
  }, checkInterval);

  logger.info("[ExpiryJob] 过期检查定时任务已启动（每60秒检查）");
}

/**
 * 停止定时任务
 */
function stopExpiryJob() {
  if (intervalId) {
    clearInterval(intervalId);
    intervalId = null;
  }
}

/**
 * 检查是否需要执行过期清理
 */
async function checkAndExpire() {
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  // 防止同一天重复执行
  if (lastExpiredDate === today) return;

  const endHour = Number(process.env.TRADING_END_HOUR || 15);
  const endMinute = Number(process.env.TRADING_END_MINUTE || 0);

  if (now.getHours() > endHour || (now.getHours() === endHour && now.getMinutes() >= endMinute)) {
    logger.info(`[ExpiryJob] 收盘时间已过，开始执行 ${today} 过期清理...`);
    await expireOrders(today);
    lastExpiredDate = today;
  }
}

/**
 * 执行过期清理
 * @param {string} tradeDate YYYY-MM-DD
 */
async function expireOrders(tradeDate) {
  // 1. 查询所有当天未完全成交的委托
  const [orders] = await pool.execute(
    `SELECT order_id, account_id, stock_code, side, price,
            remaining_quantity, status
     FROM order_book
     WHERE trade_date = ? AND status IN (?, ?)`,
    [tradeDate, ORDER_STATUS.ACCEPTED, ORDER_STATUS.PART_TRADED]
  );

  if (orders.length === 0) {
    logger.info(`[ExpiryJob] ${tradeDate} 无需过期的委托`);
    return;
  }

  logger.info(`[ExpiryJob] ${tradeDate} 共 ${orders.length} 条委托需要过期`);

  let expiredCount = 0;

  for (const order of orders) {
    try {
      // 更新数据库状态
      await pool.execute(
        `UPDATE order_book SET status = ?, update_time = ? WHERE order_id = ?`,
        [ORDER_STATUS.EXPIRED, new Date().toISOString(), order.order_id]
      );

      // 释放冻结的资金/持仓
      const remainQty = Number(order.remaining_quantity);
      if (remainQty > 0) {
        if (order.side === SIDE.BUY) {
          const releaseAmount = Number(order.price) * remainQty;
          await accountService.releaseFunds(order.account_id, releaseAmount);
        } else {
          await accountService.releaseHolding(order.account_id, order.stock_code, remainQty);
        }
      }

      // 发送过期通知
      await sendOrderReport(order.order_id, ORDER_STATUS.EXPIRED, "当日委托已过期");

      expiredCount++;
    } catch (err) {
      logger.error(`[ExpiryJob] 过期处理失败: ${order.order_id}`, err.message);
    }
  }

  // 2. 清空所有内存委托簿
  const stockCodes = getAllStockCodes();
  for (const code of stockCodes) {
    clearBookOrders(code);
  }

  // 3. 重置成交序号
  resetTradeSeq();

  logger.info(`[ExpiryJob] 过期清理完成: ${expiredCount}/${orders.length} 条委托已过期`);
}

/**
 * 手动触发过期清理（用于测试）
 */
async function manualExpire() {
  const today = new Date().toISOString().slice(0, 10);
  await expireOrders(today);
  lastExpiredDate = today;
}

module.exports = {
  startExpiryJob,
  stopExpiryJob,
  manualExpire,
};
