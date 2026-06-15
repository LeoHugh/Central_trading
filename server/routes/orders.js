/**
 * 中央交易系统 — REST 委托路由
 *
 * 兼容交易客户端 INTERFACES.md 定义的 HTTP 接口：
 *   POST /api/central-trading/orders           提交委托
 *   POST /api/central-trading/orders/:id/cancel 撤销委托
 *   GET  /api/central-trading/orders/:id/result 查询成交结果
 */

const express = require("express");
const pool = require("../db");
const orderService = require("../services/order-service");
const logger = require("../utils/logger");

const router = express.Router();

/**
 * POST /api/central-trading/orders
 * 提交买卖委托
 */
router.post("/", async (req, res, next) => {
  try {
    const body = req.body;
    const orderId = body.orderNo || body.orderId || `O${Date.now()}`;

    // 转换为 Kafka 消息格式后调用 order-service
    await orderService.receiveOrder({
      accountId: body.fundAccountNo || body.accountId,
      orderId,
      stockCode: body.stockCode,
      side: body.direction || body.side,
      price: body.price,
      quantity: body.quantity,
      timestamp: body.timestamp || new Date().toISOString(),
    });

    res.status(202).json({
      success: true,
      data: {
        accepted: true,
        orderNo: orderId,
        status: "SUBMITTED",
      },
    });
  } catch (err) {
    next(err);
  }
});

/**
 * POST /api/central-trading/orders/:orderId/cancel
 * 撤销委托
 */
router.post("/:orderId/cancel", async (req, res, next) => {
  try {
    await orderService.cancelOrder({
      orderId: req.params.orderId,
      accountId: req.body.fundAccountNo || req.body.accountId,
      timestamp: req.body.timestamp || new Date().toISOString(),
    });

    res.status(202).json({
      success: true,
      data: {
        canceled: true,
      },
    });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /api/central-trading/orders/:orderId/result
 * 查询委托的成交结果
 */
router.get("/:orderId/result", async (req, res, next) => {
  try {
    const orderId = req.params.orderId;

    // 查询委托状态
    const [orders] = await pool.execute(
      `SELECT order_id, account_id, stock_code, side, price,
              quantity, filled_quantity, remaining_quantity, status,
              entry_time, update_time
       FROM order_book WHERE order_id = ?`,
      [orderId]
    );

    if (orders.length === 0) {
      return res.status(404).json({
        success: false,
        message: "委托不存在",
      });
    }

    const order = orders[0];

    // 查询关联的成交记录
    const [trades] = await pool.execute(
      `SELECT trade_no, trade_price, trade_quantity, trade_amount, trade_time
       FROM trade_record
       WHERE buyer_order_id = ? OR seller_order_id = ?
       ORDER BY trade_time`,
      [orderId, orderId]
    );

    // 计算加权平均价格
    let weightedPrice = 0;
    let totalTraded = 0;
    for (const t of trades) {
      weightedPrice += Number(t.trade_price) * Number(t.trade_quantity);
      totalTraded += Number(t.trade_quantity);
    }
    if (totalTraded > 0) {
      weightedPrice = Math.round((weightedPrice / totalTraded) * 100) / 100;
    }

    res.json({
      success: true,
      data: {
        orderId: order.order_id,
        status: order.status,
        stockCode: order.stock_code,
        side: order.side,
        orderPrice: Number(order.price),
        orderQuantity: Number(order.quantity),
        filledQuantity: Number(order.filled_quantity),
        remainingQuantity: Number(order.remaining_quantity),
        tradePrice: weightedPrice || null,
        tradedQuantity: totalTraded,
        tradeTime: trades.length > 0 ? trades[trades.length - 1].trade_time : null,
        trades: trades.map((t) => ({
          tradeNo: t.trade_no,
          tradePrice: Number(t.trade_price),
          tradeQuantity: Number(t.trade_quantity),
          tradeAmount: Number(t.trade_amount),
          tradeTime: t.trade_time,
        })),
      },
    });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
