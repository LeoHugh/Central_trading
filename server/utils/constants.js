/**
 * 中央交易系统 — 常量定义
 */

// Kafka Topics（与 KAFKA_CONTRACT 一致）
const TOPICS = {
  // 入站（交易客户端 → 中央交易系统）
  orderCommand:  process.env.KAFKA_TOPIC_ORDER_COMMAND  || "central.order.command",
  cancelCommand: process.env.KAFKA_TOPIC_CANCEL_COMMAND || "central.cancel.command",
  stockQuery:    process.env.KAFKA_TOPIC_STOCK_QUERY    || "central.stock.query",

  // 出站（中央交易系统 → 交易客户端）
  stockQuote:  process.env.KAFKA_TOPIC_STOCK_QUOTE  || "client.stock.quote",
  tradeReport: process.env.KAFKA_TOPIC_TRADE_REPORT || "client.trade.report",
  orderReport: process.env.KAFKA_TOPIC_ORDER_REPORT || "client.order.report",

  // 出站（中央交易系统 → 网上信息发布系统）
  webTradeReport: process.env.KAFKA_TOPIC_WEB_TRADE_REPORT || "webinfo.trade.report",
};

// 委托状态
const ORDER_STATUS = {
  SUBMITTED:   "SUBMITTED",
  ACCEPTED:    "ACCEPTED",
  PART_TRADED: "PART_TRADED",
  TRADED:      "TRADED",
  CANCELED:    "CANCELED",
  EXPIRED:     "EXPIRED",
  REJECTED:    "REJECTED",
};

// 买卖方向
const SIDE = {
  BUY:  "BUY",
  SELL: "SELL",
};

// 股票类型
const STOCK_TYPE = {
  NORMAL: "NORMAL",
  ST:     "ST",
};

// 交易状态
const TRADE_STATUS = {
  TRADING:   "TRADING",
  SUSPENDED: "SUSPENDED",
};

// 默认涨跌幅
const DEFAULT_LIMIT_RATES = {
  NORMAL: 0.10,
  ST:     0.05,
};

module.exports = {
  TOPICS,
  ORDER_STATUS,
  SIDE,
  STOCK_TYPE,
  TRADE_STATUS,
  DEFAULT_LIMIT_RATES,
};
