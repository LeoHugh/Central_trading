/**
 * 中央交易系统 — 撮合引擎
 *
 * 核心撮合逻辑：
 *   1. 价格优先：买方价格高优先，卖方价格低优先
 *   2. 时间优先：同价先进入系统的优先
 *   3. 中间价格算法：成交价 = (买价 + 卖价) / 2
 *   4. 涨跌停修正：成交价超出限制以限制价格为准
 *   5. 加权价格算法：一条指令多次成交时，最终价格 = Σ(Pi × Si) / Σ(Si)
 */

const OrderBook = require("./order-book");
const { clampTradePrice } = require("./price-limiter");
const { SIDE, ORDER_STATUS } = require("../utils/constants");
const logger = require("../utils/logger");

// 全局委托簿：stockCode → OrderBook
const orderBooks = new Map();

/**
 * 获取或创建某只股票的 OrderBook
 * @param {string} stockCode
 * @returns {OrderBook}
 */
function getOrderBook(stockCode) {
  if (!orderBooks.has(stockCode)) {
    orderBooks.set(stockCode, new OrderBook(stockCode));
  }
  return orderBooks.get(stockCode);
}

/**
 * 新委托进入撮合引擎
 *
 * @param {import('./order-book').OrderEntry} newOrder
 * @param {function} onTrade  回调：(buyOrder, sellOrder, tradePrice, tradeQty) => Promise<void>
 * @returns {Promise<{trades: Array, finalStatus: string}>}
 */
async function matchOrder(newOrder, onTrade) {
  const book = getOrderBook(newOrder.stockCode);
  const trades = [];

  while (newOrder.remainingQuantity > 0) {
    // 取对手方最优委托
    const counter =
      newOrder.side === SIDE.BUY ? book.getTopSell() : book.getTopBuy();

    if (!counter) {
      logger.debug(`[撮合] ${newOrder.stockCode} 无对手方委托，挂单等待`);
      break;
    }

    // 价格匹配判断
    if (newOrder.side === SIDE.BUY && newOrder.price < counter.price) {
      logger.debug(
        `[撮合] 买价 ${newOrder.price} < 最低卖价 ${counter.price}，不匹配`
      );
      break;
    }
    if (newOrder.side === SIDE.SELL && newOrder.price > counter.price) {
      logger.debug(
        `[撮合] 卖价 ${newOrder.price} > 最高买价 ${counter.price}，不匹配`
      );
      break;
    }

    // 计算成交价格 — 中间价格算法
    const rawPrice = (newOrder.price + counter.price) / 2;
    // 涨跌停修正
    const tradePrice = await clampTradePrice(newOrder.stockCode, rawPrice);

    // 计算成交数量
    const tradeQty = Math.min(
      newOrder.remainingQuantity,
      counter.remainingQuantity
    );

    // 更新双方委托的成交量
    newOrder.filledQuantity += tradeQty;
    newOrder.remainingQuantity -= tradeQty;
    counter.filledQuantity += tradeQty;
    counter.remainingQuantity -= tradeQty;

    // 确定双方角色
    const buyOrder = newOrder.side === SIDE.BUY ? newOrder : counter;
    const sellOrder = newOrder.side === SIDE.SELL ? newOrder : counter;

    logger.info(
      `[撮合成交] ${newOrder.stockCode} 价格=${tradePrice} 数量=${tradeQty} ` +
        `买方=${buyOrder.orderId} 卖方=${sellOrder.orderId}`
    );

    // 执行成交回调（写库、通知、资金/持仓变动）
    if (onTrade) {
      await onTrade(buyOrder, sellOrder, tradePrice, tradeQty);
    }

    trades.push({ buyOrder, sellOrder, tradePrice, tradeQty });

    // 如果对手方完全成交，从委托簿移除
    if (counter.remainingQuantity <= 0) {
      counter.status = ORDER_STATUS.TRADED;
      if (newOrder.side === SIDE.BUY) {
        book.popTopSell();
      } else {
        book.popTopBuy();
      }
    } else {
      counter.status = ORDER_STATUS.PART_TRADED;
    }
  }

  // 确定新委托的最终状态
  if (newOrder.remainingQuantity <= 0) {
    newOrder.status = ORDER_STATUS.TRADED;
  } else if (newOrder.filledQuantity > 0) {
    newOrder.status = ORDER_STATUS.PART_TRADED;
    book.addOrder(newOrder); // 部分成交，剩余挂单
  } else {
    newOrder.status = ORDER_STATUS.ACCEPTED;
    book.addOrder(newOrder); // 完全未成交，挂单等待
  }

  return {
    trades,
    finalStatus: newOrder.status,
  };
}

/**
 * 从委托簿中取消一条委托
 * @param {string} orderId
 * @param {string} stockCode 如果提供，可缩小搜索范围
 * @returns {import('./order-book').OrderEntry|null}
 */
function cancelOrderInBook(orderId, stockCode) {
  if (stockCode) {
    const book = orderBooks.get(stockCode);
    if (book) return book.removeOrder(orderId);
    return null;
  }
  // 遍历所有 order book
  for (const book of orderBooks.values()) {
    const removed = book.removeOrder(orderId);
    if (removed) return removed;
  }
  return null;
}

/**
 * 获取某只股票的委托簿快照（用于管理员查看）
 * @param {string} stockCode
 */
function getBookSnapshot(stockCode) {
  const book = orderBooks.get(stockCode);
  if (!book) {
    return { stockCode, buyOrders: [], sellOrders: [], stats: null };
  }
  return {
    stockCode,
    buyOrders: book.getAllBuyOrders().map(formatOrderForDisplay),
    sellOrders: book.getAllSellOrders().map(formatOrderForDisplay),
    stats: book.getStats(),
  };
}

/**
 * 获取某只股票的买一卖一价格
 * @param {string} stockCode
 * @returns {{bidPrice: number|null, askPrice: number|null}}
 */
function getTopPrices(stockCode) {
  const book = orderBooks.get(stockCode);
  if (!book) return { bidPrice: null, askPrice: null };
  return {
    bidPrice: book.getBidPrice(),
    askPrice: book.getAskPrice(),
  };
}

/**
 * 清空某只股票的全部委托（收盘清理用）
 * @param {string} stockCode
 * @returns {Array}
 */
function clearBookOrders(stockCode) {
  const book = orderBooks.get(stockCode);
  if (!book) return [];
  return book.clearAll();
}

/**
 * 获取所有委托簿的股票代码列表
 */
function getAllStockCodes() {
  return Array.from(orderBooks.keys());
}

function formatOrderForDisplay(order) {
  return {
    orderId: order.orderId,
    accountId: order.accountId,
    side: order.side,
    price: order.price,
    quantity: order.quantity,
    filledQuantity: order.filledQuantity,
    remainingQuantity: order.remainingQuantity,
    status: order.status,
    entryTime: order.entryTime,
  };
}

module.exports = {
  getOrderBook,
  matchOrder,
  cancelOrderInBook,
  getBookSnapshot,
  getTopPrices,
  clearBookOrders,
  getAllStockCodes,
};
