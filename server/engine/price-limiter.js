/**
 * 中央交易系统 — 涨跌停价格限制器
 *
 * 规则：
 *   今日涨停价 = 昨日收盘价 + (昨日收盘价 × 涨跌幅)
 *   今日跌停价 = 昨日收盘价 - (昨日收盘价 × 涨跌幅)
 *   普通股涨跌幅 = 10%，ST 股涨跌幅 = 5%
 */

const pool = require("../db");
const logger = require("../utils/logger");
const { DEFAULT_LIMIT_RATES } = require("../utils/constants");

// 内存缓存：stockCode → { upperLimit, lowerLimit, limitRate, previousClose }
const limitsCache = new Map();

/**
 * 从数据库加载某只股票的涨跌停信息并缓存
 * @param {string} stockCode
 * @returns {Promise<{upperLimit: number, lowerLimit: number, limitRate: number, previousClose: number}>}
 */
async function loadPriceLimits(stockCode) {
  const [rows] = await pool.execute(
    `SELECT s.stock_code, s.stock_type, s.previous_close,
            COALESCE(p.limit_rate, ?) AS limit_rate
     FROM stock_info s
     LEFT JOIN price_limit_config p ON p.stock_type = s.stock_type
     WHERE s.stock_code = ?`,
    [DEFAULT_LIMIT_RATES.NORMAL, stockCode]
  );

  if (rows.length === 0) {
    return null;
  }

  const row = rows[0];
  const previousClose = Number(row.previous_close);
  const limitRate = Number(row.limit_rate);

  const limits = {
    previousClose,
    limitRate,
    upperLimit: roundPrice(previousClose * (1 + limitRate)),
    lowerLimit: roundPrice(previousClose * (1 - limitRate)),
  };

  limitsCache.set(stockCode, limits);
  return limits;
}

/**
 * 获取涨跌停限制（优先使用缓存）
 * @param {string} stockCode
 * @returns {Promise<{upperLimit: number, lowerLimit: number, limitRate: number, previousClose: number}|null>}
 */
async function getPriceLimits(stockCode) {
  if (limitsCache.has(stockCode)) {
    return limitsCache.get(stockCode);
  }
  return loadPriceLimits(stockCode);
}

/**
 * 验证委托价格是否在涨跌停范围内
 * @param {string} stockCode
 * @param {number} price
 * @returns {Promise<{valid: boolean, reason?: string, upperLimit?: number, lowerLimit?: number}>}
 */
async function validateOrderPrice(stockCode, price) {
  const limits = await getPriceLimits(stockCode);
  if (!limits) {
    return { valid: false, reason: `股票 ${stockCode} 不存在` };
  }

  if (price > limits.upperLimit) {
    return {
      valid: false,
      reason: `委托价格 ${price} 超过涨停价 ${limits.upperLimit}`,
      upperLimit: limits.upperLimit,
      lowerLimit: limits.lowerLimit,
    };
  }

  if (price < limits.lowerLimit) {
    return {
      valid: false,
      reason: `委托价格 ${price} 低于跌停价 ${limits.lowerLimit}`,
      upperLimit: limits.upperLimit,
      lowerLimit: limits.lowerLimit,
    };
  }

  return {
    valid: true,
    upperLimit: limits.upperLimit,
    lowerLimit: limits.lowerLimit,
  };
}

/**
 * 将原始计算的撮合价格钳制在涨跌停范围内
 *
 * 规则：如果计算所得价格超出涨跌停限制，以限制价格为准。
 * @param {string} stockCode
 * @param {number} rawPrice
 * @returns {Promise<number>}
 */
async function clampTradePrice(stockCode, rawPrice) {
  const limits = await getPriceLimits(stockCode);
  if (!limits) return rawPrice;

  if (rawPrice > limits.upperLimit) {
    logger.info(`成交价 ${rawPrice} 被钳制到涨停价 ${limits.upperLimit} (${stockCode})`);
    return limits.upperLimit;
  }
  if (rawPrice < limits.lowerLimit) {
    logger.info(`成交价 ${rawPrice} 被钳制到跌停价 ${limits.lowerLimit} (${stockCode})`);
    return limits.lowerLimit;
  }
  return roundPrice(rawPrice);
}

/**
 * 刷新所有缓存（通常在每日开盘前调用）
 */
async function refreshAllLimits() {
  const [rows] = await pool.execute(
    `SELECT s.stock_code, s.stock_type, s.previous_close,
            COALESCE(p.limit_rate, ?) AS limit_rate
     FROM stock_info s
     LEFT JOIN price_limit_config p ON p.stock_type = s.stock_type`,
    [DEFAULT_LIMIT_RATES.NORMAL]
  );

  limitsCache.clear();
  for (const row of rows) {
    const previousClose = Number(row.previous_close);
    const limitRate = Number(row.limit_rate);
    limitsCache.set(row.stock_code, {
      previousClose,
      limitRate,
      upperLimit: roundPrice(previousClose * (1 + limitRate)),
      lowerLimit: roundPrice(previousClose * (1 - limitRate)),
    });
  }
  logger.info(`涨跌停缓存已刷新，共 ${limitsCache.size} 只股票`);
}

/**
 * 更新某只股票的涨跌停幅度（管理员设置，次日生效）
 * @param {string} stockType NORMAL / ST
 * @param {number} newRate 新的涨跌幅比例
 */
async function updateLimitRate(stockType, newRate) {
  await pool.execute(
    `INSERT INTO price_limit_config (stock_type, limit_rate)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE limit_rate = ?`,
    [stockType, newRate, newRate]
  );
  logger.info(`涨跌停幅度已更新: ${stockType} = ${(newRate * 100).toFixed(1)}%`);
}

/**
 * 保留两位小数
 */
function roundPrice(value) {
  return Math.round(value * 100) / 100;
}

module.exports = {
  getPriceLimits,
  validateOrderPrice,
  clampTradePrice,
  refreshAllLimits,
  updateLimitRate,
};
