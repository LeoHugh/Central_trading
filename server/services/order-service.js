/**
 * 中央交易系统 — 委托管理服务
 *
 * 核心入口，处理从 Kafka / REST 接收到的委托和撤单请求：
 *  - receiveOrder()  : 接收买卖委托 → 验证 → 冻结资金/持仓 → 存库 → 撮合
 *  - cancelOrder()   : 接收撤单请求 → 移除 → 释放 → 通知
 *  - handleStockQuery() : 处理行情查询请求
 */

const pool = require("../db");
const { matchOrder, cancelOrderInBook } = require("../engine/matching-engine");
const { validateOrderPrice } = require("../engine/price-limiter");
const { executeTrade } = require("./trade-service");
const stockService = require("./stock-service");
const accountService = require("./account-service");
const { sendOrderReport } = require("../kafka/producer");
const { ORDER_STATUS, SIDE, TRADE_STATUS } = require("../utils/constants");
const logger = require("../utils/logger");

/**
 * 接收买卖委托
 *
 * Kafka 消息格式 (central.order.command):
 *   { accountId, orderId, stockCode, side, price, quantity, timestamp }
 *
 * @param {Object} msg
 */
async function receiveOrder(msg) {
  const { accountId, orderId, stockCode, side, price, quantity, timestamp } = msg;

  // --- 基本验证 ---
  if (!accountId || !orderId || !stockCode || !side || !price || !quantity) {
    logger.warn(`[OrderService] 委托参数不完整: ${orderId}`);
    await sendOrderReport(orderId || "UNKNOWN", ORDER_STATUS.REJECTED, "委托参数不完整");
    return;
  }

  if (side !== SIDE.BUY && side !== SIDE.SELL) {
    await sendOrderReport(orderId, ORDER_STATUS.REJECTED, `无效的买卖方向: ${side}`);
    return;
  }

  // --- 验证股票存在且未停牌 ---
  const stockInfo = await stockService.getStockInfo(stockCode);
  if (!stockInfo) {
    await sendOrderReport(orderId, ORDER_STATUS.REJECTED, `股票 ${stockCode} 不存在`);
    return;
  }
  if (stockInfo.trade_status === TRADE_STATUS.SUSPENDED) {
    await sendOrderReport(orderId, ORDER_STATUS.REJECTED, `股票 ${stockCode} 已停牌，暂停交易`);
    return;
  }

  // --- 验证涨跌停 ---
  const priceCheck = await validateOrderPrice(stockCode, Number(price));
  if (!priceCheck.valid) {
    await sendOrderReport(orderId, ORDER_STATUS.REJECTED, priceCheck.reason);
    return;
  }

  // --- 冻结资金/持仓 ---
  try {
    if (side === SIDE.BUY) {
      const freezeAmount = Number(price) * Number(quantity);
      await accountService.freezeFunds(accountId, freezeAmount);
    } else {
      await accountService.freezeHolding(accountId, stockCode, Number(quantity));
    }
  } catch (err) {
    await sendOrderReport(orderId, ORDER_STATUS.REJECTED, `冻结失败: ${err.message}`);
    return;
  }

  // --- 写入数据库 ---
  const entryTime = timestamp || new Date().toISOString();
  const tradeDate = new Date().toISOString().slice(0, 10);

  try {
    await pool.execute(
      `INSERT INTO order_book
         (order_id, account_id, stock_code, side, price, quantity,
          filled_quantity, remaining_quantity, status, entry_time, update_time, trade_date)
       VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)`,
      [orderId, accountId, stockCode, side, Number(price), Number(quantity),
       Number(quantity), ORDER_STATUS.ACCEPTED, entryTime, entryTime, tradeDate]
    );
  } catch (err) {
    // 可能是重复委托
    if (err.code === "ER_DUP_ENTRY") {
      await sendOrderReport(orderId, ORDER_STATUS.REJECTED, "重复的委托编号");
    } else {
      await sendOrderReport(orderId, ORDER_STATUS.REJECTED, `系统错误: ${err.message}`);
    }
    // 回滚冻结
    try {
      if (side === SIDE.BUY) {
        await accountService.releaseFunds(accountId, Number(price) * Number(quantity));
      } else {
        await accountService.releaseHolding(accountId, stockCode, Number(quantity));
      }
    } catch (rollbackErr) {
      logger.error(`[OrderService] 冻结回滚失败: ${orderId}`, rollbackErr.message);
    }
    return;
  }

  // --- 发送 ACCEPTED 状态 ---
  await sendOrderReport(orderId, ORDER_STATUS.ACCEPTED, "委托已受理");

  // --- 构建内存委托对象并触发撮合 ---
  const orderEntry = {
    orderId,
    accountId,
    stockCode,
    side,
    price: Number(price),
    quantity: Number(quantity),
    filledQuantity: 0,
    remainingQuantity: Number(quantity),
    status: ORDER_STATUS.ACCEPTED,
    entryTime,
  };

  const result = await matchOrder(orderEntry, executeTrade);

  // --- 如果没有任何成交且已挂单，更新状态 ---
  if (result.trades.length === 0) {
    logger.info(`[OrderService] ${orderId} 无匹配对手方，已挂单等待`);
  }
}

/**
 * 接收撤单请求
 *
 * Kafka 消息格式 (central.cancel.command):
 *   { orderId, accountId, timestamp }
 *
 * @param {Object} msg
 */
async function cancelOrder(msg) {
  const { orderId, accountId } = msg;

  if (!orderId) {
    logger.warn("[OrderService] 撤单缺少 orderId");
    return;
  }

  // 从数据库查询委托
  const [rows] = await pool.execute(
    `SELECT order_id, account_id, stock_code, side, price,
            quantity, filled_quantity, remaining_quantity, status
     FROM order_book WHERE order_id = ?`,
    [orderId]
  );

  if (rows.length === 0) {
    await sendOrderReport(orderId, ORDER_STATUS.REJECTED, "委托不存在");
    return;
  }

  const order = rows[0];

  // 已完全成交或已取消的委托不能撤单
  if (order.status === ORDER_STATUS.TRADED ||
      order.status === ORDER_STATUS.CANCELED ||
      order.status === ORDER_STATUS.EXPIRED) {
    await sendOrderReport(orderId, ORDER_STATUS.REJECTED,
      `委托状态为 ${order.status}，无法撤单`);
    return;
  }

  // 从内存委托簿中移除
  cancelOrderInBook(orderId, order.stock_code);

  // 更新数据库状态
  await pool.execute(
    `UPDATE order_book SET status = ?, update_time = ? WHERE order_id = ?`,
    [ORDER_STATUS.CANCELED, new Date().toISOString(), orderId]
  );

  // 释放冻结的资金/持仓（只释放剩余未成交部分）
  const remainQty = Number(order.remaining_quantity);
  try {
    if (order.side === SIDE.BUY) {
      const releaseAmount = Number(order.price) * remainQty;
      await accountService.releaseFunds(order.account_id, releaseAmount);
    } else {
      await accountService.releaseHolding(order.account_id, order.stock_code, remainQty);
    }
  } catch (err) {
    logger.error(`[OrderService] 撤单释放资源失败: ${orderId}`, err.message);
  }

  // 发送撤单成功通知
  await sendOrderReport(orderId, ORDER_STATUS.CANCELED, "用户撤单成功");

  logger.info(`[OrderService] 撤单成功: ${orderId}`);
}

/**
 * 处理行情查询请求
 *
 * Kafka 消息格式 (central.stock.query):
 *   { stockCode, queryId, timestamp }
 *
 * @param {Object} msg
 */
async function handleStockQuery(msg) {
  const { stockCode } = msg;
  if (!stockCode) {
    logger.warn("[OrderService] 行情查询缺少 stockCode");
    return;
  }

  await stockService.queryAndSendQuote(stockCode);
  logger.debug(`[OrderService] 行情查询已处理: ${stockCode}`);
}

module.exports = {
  receiveOrder,
  cancelOrder,
  handleStockQuery,
};
