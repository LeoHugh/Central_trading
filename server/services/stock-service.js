/**
 * 中央交易系统 — 股票行情服务
 *
 * 负责：
 *  1. 查询股票基础信息和最新价格
 *  2. 查询当日/周/月最高最低价
 *  3. 组装完整行情数据
 *  4. 更新最新成交价
 */

const pool = require("../db");
const { getTopPrices } = require("../engine/matching-engine");
const { sendStockQuote } = require("../kafka/producer");
const logger = require("../utils/logger");

/**
 * 查询股票基础信息
 * @param {string} stockCode
 */
async function getStockInfo(stockCode) {
  const [rows] = await pool.execute(
    `SELECT stock_code, stock_name, stock_type, previous_close,
            latest_price, open_price, trade_status, notice
     FROM stock_info WHERE stock_code = ?`,
    [stockCode]
  );
  return rows.length > 0 ? rows[0] : null;
}

/**
 * 通过关键字搜索股票（代码或名称模糊匹配）
 * @param {string} keyword
 */
async function searchStocks(keyword) {
  const q = String(keyword || "").trim();
  if (!q) {
    const [rows] = await pool.execute(
      `SELECT stock_code, stock_name, stock_type, previous_close,
              latest_price, open_price, trade_status, notice
       FROM stock_info ORDER BY stock_code LIMIT 50`
    );
    return rows;
  }
  const [rows] = await pool.execute(
    `SELECT stock_code, stock_name, stock_type, previous_close,
            latest_price, open_price, trade_status, notice
     FROM stock_info
     WHERE stock_code = ? OR stock_name LIKE ?
     LIMIT 50`,
    [q, `%${q}%`]
  );
  return rows;
}

/**
 * 获取某只股票当日最高/最低成交价
 * @param {string} stockCode
 */
async function getDayHighLow(stockCode) {
  const today = new Date().toISOString().slice(0, 10);
  const [rows] = await pool.execute(
    `SELECT MAX(trade_price) AS highestPrice, MIN(trade_price) AS lowestPrice
     FROM trade_price_history
     WHERE stock_code = ? AND DATE(trade_time) = ?`,
    [stockCode, today]
  );
  if (rows.length > 0 && rows[0].highestPrice !== null) {
    return {
      highestPrice: Number(rows[0].highestPrice),
      lowestPrice: Number(rows[0].lowestPrice),
    };
  }
  return null;
}

/**
 * 组装完整行情数据并通过 Kafka 推送
 * @param {string} stockCode
 */
async function queryAndSendQuote(stockCode) {
  const info = await getStockInfo(stockCode);
  if (!info) {
    logger.warn(`[StockService] 股票 ${stockCode} 不存在`);
    return null;
  }

  const dayHL = await getDayHighLow(stockCode);
  const { bidPrice, askPrice } = getTopPrices(stockCode);

  const quote = {
    stockCode: info.stock_code,
    stockName: info.stock_name,
    latestPrice: Number(info.latest_price),
    previousClose: Number(info.previous_close),
    highestPrice: dayHL ? dayHL.highestPrice : Number(info.latest_price),
    lowestPrice: dayHL ? dayHL.lowestPrice : Number(info.latest_price),
    bidPrice: bidPrice || Number(info.latest_price),
    askPrice: askPrice || Number(info.latest_price),
    tradeStatus: info.trade_status === "TRADING" ? "可交易" : "停牌",
    notice: info.notice || "",
    quoteTime: new Date().toISOString(),
  };

  // 通过 Kafka 推送行情
  await sendStockQuote(quote);

  return quote;
}

/**
 * 构建行情数据（不发送 Kafka，仅返回数据供 REST 使用）
 * @param {string} stockCode
 */
async function buildQuote(stockCode) {
  const info = await getStockInfo(stockCode);
  if (!info) return null;

  const dayHL = await getDayHighLow(stockCode);
  const { bidPrice, askPrice } = getTopPrices(stockCode);

  return {
    stockCode: info.stock_code,
    stockName: info.stock_name,
    latestPrice: Number(info.latest_price),
    previousClose: Number(info.previous_close),
    highestPrice: dayHL ? dayHL.highestPrice : Number(info.latest_price),
    lowestPrice: dayHL ? dayHL.lowestPrice : Number(info.latest_price),
    bidPrice: bidPrice || Number(info.latest_price),
    askPrice: askPrice || Number(info.latest_price),
    tradeStatus: info.trade_status === "TRADING" ? "可交易" : "停牌",
    notice: info.notice || "",
    quoteTime: new Date().toISOString(),
  };
}

/**
 * 更新最新成交价
 * @param {string} stockCode
 * @param {number} price
 */
async function updateLatestPrice(stockCode, price) {
  await pool.execute(
    `UPDATE stock_info SET latest_price = ?, update_time = NOW() WHERE stock_code = ?`,
    [price, stockCode]
  );
}

/**
 * 记录成交价格历史
 * @param {string} stockCode
 * @param {number} price
 * @param {string} tradeTime ISO 时间
 */
async function recordPriceHistory(stockCode, price, tradeTime) {
  await pool.execute(
    `INSERT INTO trade_price_history (stock_code, trade_price, trade_time) VALUES (?, ?, ?)`,
    [stockCode, price, tradeTime]
  );
}

/**
 * 暂停/恢复股票交易
 * @param {string} stockCode
 * @param {string} status TRADING / SUSPENDED
 */
async function setTradeStatus(stockCode, status) {
  await pool.execute(
    `UPDATE stock_info SET trade_status = ?, update_time = NOW() WHERE stock_code = ?`,
    [status, stockCode]
  );
  logger.info(`[StockService] ${stockCode} 交易状态更新为 ${status}`);
}

module.exports = {
  getStockInfo,
  searchStocks,
  getDayHighLow,
  queryAndSendQuote,
  buildQuote,
  updateLatestPrice,
  recordPriceHistory,
  setTradeStatus,
};
