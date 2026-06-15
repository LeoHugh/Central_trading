/**
 * 中央交易系统 — 单只股票的买卖委托簿 (Order Book)
 *
 * 买方队列按价格降序排列（价格高的优先成交），同价按进入时间升序
 * 卖方队列按价格升序排列（价格低的优先成交），同价按进入时间升序
 *
 * 使用排序数组实现，插入为 O(n)，取头部为 O(1)，适合教学/演示规模。
 */

const { SIDE } = require("../utils/constants");

class OrderBook {
  /**
   * @param {string} stockCode 股票代码
   */
  constructor(stockCode) {
    this.stockCode = stockCode;
    /** @type {Array<OrderEntry>} 买方委托（价格降序，同价时间升序） */
    this.buyOrders = [];
    /** @type {Array<OrderEntry>} 卖方委托（价格升序，同价时间升序） */
    this.sellOrders = [];
  }

  /**
   * 向委托簿中插入一条委托
   * @param {OrderEntry} order
   */
  addOrder(order) {
    const list = order.side === SIDE.BUY ? this.buyOrders : this.sellOrders;
    const idx = this._findInsertIndex(list, order);
    list.splice(idx, 0, order);
  }

  /**
   * 按 orderId 移除一条委托
   * @param {string} orderId
   * @returns {OrderEntry|null} 被移除的委托，或 null
   */
  removeOrder(orderId) {
    for (const list of [this.buyOrders, this.sellOrders]) {
      const idx = list.findIndex((o) => o.orderId === orderId);
      if (idx !== -1) {
        return list.splice(idx, 1)[0];
      }
    }
    return null;
  }

  /**
   * 获取当前最优买方委托（不移除）
   * @returns {OrderEntry|null}
   */
  getTopBuy() {
    return this.buyOrders.length > 0 ? this.buyOrders[0] : null;
  }

  /**
   * 获取当前最优卖方委托（不移除）
   * @returns {OrderEntry|null}
   */
  getTopSell() {
    return this.sellOrders.length > 0 ? this.sellOrders[0] : null;
  }

  /**
   * 移除并返回最优买方委托
   * @returns {OrderEntry|null}
   */
  popTopBuy() {
    return this.buyOrders.length > 0 ? this.buyOrders.shift() : null;
  }

  /**
   * 移除并返回最优卖方委托
   * @returns {OrderEntry|null}
   */
  popTopSell() {
    return this.sellOrders.length > 0 ? this.sellOrders.shift() : null;
  }

  /**
   * 获取所有买方委托（只读副本）
   */
  getAllBuyOrders() {
    return [...this.buyOrders];
  }

  /**
   * 获取所有卖方委托（只读副本）
   */
  getAllSellOrders() {
    return [...this.sellOrders];
  }

  /**
   * 获取当前最高买价（买一价/bidPrice）
   * @returns {number|null}
   */
  getBidPrice() {
    const top = this.getTopBuy();
    return top ? top.price : null;
  }

  /**
   * 获取当前最低卖价（卖一价/askPrice）
   * @returns {number|null}
   */
  getAskPrice() {
    const top = this.getTopSell();
    return top ? top.price : null;
  }

  /**
   * 清空所有委托（用于收盘清理）
   * @returns {Array<OrderEntry>} 被清除的全部委托
   */
  clearAll() {
    const all = [...this.buyOrders, ...this.sellOrders];
    this.buyOrders = [];
    this.sellOrders = [];
    return all;
  }

  /**
   * 获取委托簿统计
   */
  getStats() {
    return {
      stockCode: this.stockCode,
      buyCount: this.buyOrders.length,
      sellCount: this.sellOrders.length,
      bidPrice: this.getBidPrice(),
      askPrice: this.getAskPrice(),
      totalBuyQuantity: this.buyOrders.reduce((sum, o) => sum + o.remainingQuantity, 0),
      totalSellQuantity: this.sellOrders.reduce((sum, o) => sum + o.remainingQuantity, 0),
    };
  }

  // ---- 私有方法 ----

  /**
   * 二分查找合适的插入位置
   */
  _findInsertIndex(list, order) {
    let lo = 0;
    let hi = list.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._shouldInsertBefore(order, list[mid])) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }
    return lo;
  }

  /**
   * 判断 orderA 是否应排在 orderB 前面
   */
  _shouldInsertBefore(a, b) {
    if (a.side === SIDE.BUY) {
      // 买方：价格高优先；同价，时间早优先
      if (a.price !== b.price) return a.price > b.price;
      return a.entryTime < b.entryTime;
    } else {
      // 卖方：价格低优先；同价，时间早优先
      if (a.price !== b.price) return a.price < b.price;
      return a.entryTime < b.entryTime;
    }
  }
}

/**
 * @typedef {Object} OrderEntry
 * @property {string}  orderId            委托编号
 * @property {string}  accountId          资金账户ID
 * @property {string}  stockCode          股票代码
 * @property {string}  side               BUY / SELL
 * @property {number}  price              委托价格
 * @property {number}  quantity           委托数量（原始）
 * @property {number}  filledQuantity     已成交数量
 * @property {number}  remainingQuantity  剩余数量
 * @property {string}  status             委托状态
 * @property {string}  entryTime          进入系统时间 (ISO 8601)
 */

module.exports = OrderBook;
