/**
 * 中央交易系统 — REST 股票行情路由
 *
 * 兼容交易客户端 INTERFACES.md 定义：
 *   GET /api/central-trading/stocks?keyword=600519
 */

const express = require("express");
const stockService = require("../services/stock-service");

const router = express.Router();

/**
 * GET /api/central-trading/stocks?keyword=
 * 查询股票行情（支持代码或名称搜索）
 */
router.get("/", async (req, res, next) => {
  try {
    const keyword = String(req.query.keyword || "").trim();
    const stocks = await stockService.searchStocks(keyword);

    // 为每只股票构建完整行情
    const quotes = [];
    for (const stock of stocks) {
      const quote = await stockService.buildQuote(stock.stock_code);
      if (quote) quotes.push(quote);
    }

    res.json(quotes);
  } catch (err) {
    next(err);
  }
});

/**
 * GET /api/central-trading/stocks/:stockCode
 * 查询单只股票详细行情
 */
router.get("/:stockCode", async (req, res, next) => {
  try {
    const quote = await stockService.buildQuote(req.params.stockCode);
    if (!quote) {
      return res.status(404).json({ success: false, message: "股票不存在" });
    }
    res.json({ success: true, data: quote });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
