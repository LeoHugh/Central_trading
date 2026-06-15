/**
 * 中央交易系统 — 成交处理服务
 *
 * 当撮合引擎产生一笔成交时，本服务负责：
 *  1. 生成成交编号 (tradeNo)
 *  2. 写入成交记录到数据库
 *  3. 更新最新成交价和价格历史
 *  4. 调用账户系统更新资金和持仓
 *  5. 通过 Kafka 发送成交反馈和行情更新
 */

const pool = require("../db");
const accountService = require("./account-service");
const stockService = require("./stock-service");
const { sendTradeReport, sendOrderReport, sendStockQuote } = require("../kafka/producer");
const { getTopPrices } = require("../engine/matching-engine");
const { ORDER_STATUS, SIDE } = require("../utils/constants");
const logger = require("../utils/logger");

// 当日成交序号计数器
let tradeSeq = 0;

/**
 * 生成成交编号，格式：T + YYYYMMDD + 4位序号
 */
function generateTradeNo() {
  tradeSeq += 1;
  const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  return `T${dateStr}${String(tradeSeq).padStart(4, "0")}`;
}

/**
 * 处理一笔成交
 *
 * @param {Object} buyOrder   买方委托
 * @param {Object} sellOrder  卖方委托
 * @param {number} tradePrice 成交价格
 * @param {number} tradeQty   成交数量
 */
async function executeTrade(buyOrder, sellOrder, tradePrice, tradeQty) {
  const tradeNo = generateTradeNo();
  const tradeTime = new Date().toISOString();
  const tradeAmount = Math.round(tradePrice * tradeQty * 100) / 100;

  // 1. 写入成交记录
  await pool.execute(
    `INSERT INTO trade_record
       (trade_no, buyer_order_id, seller_order_id, stock_code, trade_price, trade_quantity, trade_amount, trade_time)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [tradeNo, buyOrder.orderId, sellOrder.orderId, buyOrder.stockCode,
     tradePrice, tradeQty, tradeAmount, tradeTime]
  );

  // 2. 更新最新成交价和价格历史
  await stockService.updateLatestPrice(buyOrder.stockCode, tradePrice);
  await stockService.recordPriceHistory(buyOrder.stockCode, tradePrice, tradeTime);

  // 3. 更新双方委托在数据库中的状态
  await updateOrderInDb(buyOrder);
  await updateOrderInDb(sellOrder);

  // 4. 调用账户系统：买方扣划冻结资金 + 增加持仓
  try {
    await accountService.settleBuyFunds(buyOrder.accountId, tradeAmount);
    await accountService.settleBuyerHolding(buyOrder.accountId, buyOrder.stockCode, tradeQty);
  } catch (err) {
    logger.error(`[TradeService] 买方账户更新失败: ${buyOrder.orderId}`, err.message);
  }

  // 5. 调用账户系统：卖方扣减冻结持仓 + 回款
  try {
    await accountService.settleSellerHolding(sellOrder.accountId, sellOrder.stockCode, tradeQty);
    await accountService.settleSellFunds(sellOrder.accountId, tradeAmount);
  } catch (err) {
    logger.error(`[TradeService] 卖方账户更新失败: ${sellOrder.orderId}`, err.message);
  }

  // 6. 发送成交反馈到交易客户端 (client.trade.report)
  await sendTradeReport({
    tradeNo,
    buyerOrderId: buyOrder.orderId,
    sellerOrderId: sellOrder.orderId,
    stockCode: buyOrder.stockCode,
    tradePrice,
    tradeQuantity: tradeQty,
    tradeTime,
  });

  // 7. 发送订单状态更新 (client.order.report) — 买方
  await sendOrderReport(
    buyOrder.orderId,
    buyOrder.status,
    buyOrder.status === ORDER_STATUS.TRADED ? "全部成交" : `部分成交 ${buyOrder.filledQuantity}/${buyOrder.quantity}`
  );

  // 8. 发送订单状态更新 — 卖方
  await sendOrderReport(
    sellOrder.orderId,
    sellOrder.status,
    sellOrder.status === ORDER_STATUS.TRADED ? "全部成交" : `部分成交 ${sellOrder.filledQuantity}/${sellOrder.quantity}`
  );

  // 9. 推送最新行情 (client.stock.quote)
  await pushLatestQuote(buyOrder.stockCode, tradePrice);

  logger.info(
    `[TradeService] 成交完成: ${tradeNo} ${buyOrder.stockCode} ` +
    `${tradePrice}×${tradeQty} 买方=${buyOrder.orderId} 卖方=${sellOrder.orderId}`
  );
}

/**
 * 更新委托在数据库中的状态
 */
async function updateOrderInDb(order) {
  await pool.execute(
    `UPDATE order_book
     SET filled_quantity = ?, remaining_quantity = ?, status = ?, update_time = ?
     WHERE order_id = ?`,
    [order.filledQuantity, order.remainingQuantity, order.status,
     new Date().toISOString(), order.orderId]
  );
}

/**
 * 推送最新行情
 */
async function pushLatestQuote(stockCode, latestPrice) {
  try {
    const quote = await stockService.buildQuote(stockCode);
    if (quote) {
      quote.latestPrice = latestPrice;
      await sendStockQuote(quote);
    }
  } catch (err) {
    logger.error(`[TradeService] 推送行情失败: ${stockCode}`, err.message);
  }
}

/**
 * 重置成交序号（每日开盘时调用）
 */
function resetTradeSeq() {
  tradeSeq = 0;
}

module.exports = {
  executeTrade,
  resetTradeSeq,
};
