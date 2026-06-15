/**
 * 中央交易系统 — Kafka 消息生产者
 *
 * 负责向交易客户端和网上信息发布系统推送消息：
 *  - client.order.report  : 委托状态反馈
 *  - client.trade.report  : 成交反馈
 *  - client.stock.quote   : 行情反馈
 *  - webinfo.trade.report : 网上信息发布（成交信息）
 */

const { TOPICS } = require("../utils/constants");
const logger = require("../utils/logger");

let producer = null;

/**
 * 设置 Kafka producer 实例（由 kafka/index.js 调用）
 */
function setProducer(p) {
  producer = p;
}

/**
 * 发送消息到指定 topic
 */
async function sendMessage(topic, key, value) {
  if (!producer) {
    logger.warn(`Kafka producer 未就绪，消息未发送: topic=${topic} key=${key}`);
    return;
  }
  try {
    await producer.send({
      topic,
      messages: [{ key: String(key), value: JSON.stringify(value) }],
    });
    logger.debug(`[Kafka 发送] topic=${topic} key=${key}`);
  } catch (err) {
    logger.error(`[Kafka 发送失败] topic=${topic}`, err.message);
  }
}

/**
 * 发送委托状态反馈
 * 格式遵循 KAFKA_CONTRACT：
 * { orderId, status, reason, timestamp }
 */
async function sendOrderReport(orderId, status, reason) {
  const msg = {
    orderId,
    status,
    reason: reason || "",
    timestamp: new Date().toISOString(),
  };
  await sendMessage(TOPICS.orderReport, orderId, msg);
}

/**
 * 发送成交反馈
 * 格式遵循 KAFKA_CONTRACT：
 * { tradeNo, buyerOrderId, sellerOrderId, stockCode, tradePrice, tradeQuantity, tradeTime }
 */
async function sendTradeReport(trade) {
  const msg = {
    tradeNo: trade.tradeNo,
    buyerOrderId: trade.buyerOrderId,
    sellerOrderId: trade.sellerOrderId,
    stockCode: trade.stockCode,
    tradePrice: trade.tradePrice,
    tradeQuantity: trade.tradeQuantity,
    tradeTime: trade.tradeTime || new Date().toISOString(),
  };
  // 发送到交易客户端
  await sendMessage(TOPICS.tradeReport, trade.buyerOrderId, msg);

  // 同时发送到网上信息发布系统
  await sendMessage(TOPICS.webTradeReport, trade.stockCode, msg);
}

/**
 * 发送股票行情反馈
 * 格式遵循 KAFKA_CONTRACT：
 * { stockCode, stockName, latestPrice, previousClose, highestPrice, lowestPrice,
 *   bidPrice, askPrice, tradeStatus, notice, quoteTime }
 */
async function sendStockQuote(quote) {
  const msg = {
    stockCode: quote.stockCode,
    stockName: quote.stockName || "",
    latestPrice: quote.latestPrice,
    previousClose: quote.previousClose,
    highestPrice: quote.highestPrice,
    lowestPrice: quote.lowestPrice,
    bidPrice: quote.bidPrice,
    askPrice: quote.askPrice,
    tradeStatus: quote.tradeStatus || "可交易",
    notice: quote.notice || "",
    quoteTime: quote.quoteTime || new Date().toISOString(),
  };
  await sendMessage(TOPICS.stockQuote, quote.stockCode, msg);
}

module.exports = {
  setProducer,
  sendOrderReport,
  sendTradeReport,
  sendStockQuote,
};
