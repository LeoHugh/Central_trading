/**
 * 中央交易系统 — 外部账户系统调用服务
 *
 * 对接资金账户和证券账户系统的接口（来自 API(1).md）：
 *   - updateFundBalance：冻结/扣划/释放资金
 *   - updateSecurityHolding：冻结/扣减/增加持仓
 *
 * 当 ACCOUNT_API_MOCK=true 时，所有调用返回模拟成功，便于独立开发测试。
 */

const logger = require("../utils/logger");

const ACCOUNT_API_BASE = process.env.ACCOUNT_API_BASE || "http://localhost:8080";
const IS_MOCK = process.env.ACCOUNT_API_MOCK === "true";

/**
 * 通用 HTTP 请求封装
 */
async function callAccountApi(path, body) {
  if (IS_MOCK) {
    logger.debug(`[AccountService Mock] ${path}`, JSON.stringify(body));
    return { success: true, mock: true };
  }

  const url = `${ACCOUNT_API_BASE}${path}`;
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok) {
      logger.error(`[AccountService] ${path} 失败: ${resp.status}`, data);
      throw new Error(data.message || `Account API error: ${resp.status}`);
    }
    return data;
  } catch (err) {
    logger.error(`[AccountService] ${path} 调用异常:`, err.message);
    throw err;
  }
}

// ======================== 资金账户操作 ========================

/**
 * 买入下单：冻结资金
 * delta_fund_a = -amount (可用减少), delta_fund_f = +amount (冻结增加)
 */
async function freezeFunds(accountId, amount) {
  return callAccountApi("/api/fund-accounts/updateBalance", {
    fund_acc_no: accountId,
    delta_fund_a: -amount,
    delta_fund_f: amount,
  });
}

/**
 * 买入成交：扣划冻结资金
 * delta_fund_f = -amount (冻结减少，资金真正扣除)
 */
async function settleBuyFunds(accountId, amount) {
  return callAccountApi("/api/fund-accounts/updateBalance", {
    fund_acc_no: accountId,
    delta_fund_a: 0,
    delta_fund_f: -amount,
  });
}

/**
 * 卖出成交：增加可用资金（回款）
 * delta_fund_a = +amount
 */
async function settleSellFunds(accountId, amount) {
  return callAccountApi("/api/fund-accounts/updateBalance", {
    fund_acc_no: accountId,
    delta_fund_a: amount,
    delta_fund_f: 0,
  });
}

/**
 * 撤单/过期：释放冻结资金
 * delta_fund_a = +amount (可用恢复), delta_fund_f = -amount (冻结减少)
 */
async function releaseFunds(accountId, amount) {
  return callAccountApi("/api/fund-accounts/updateBalance", {
    fund_acc_no: accountId,
    delta_fund_a: amount,
    delta_fund_f: -amount,
  });
}

// ======================== 证券账户操作 ========================

/**
 * 卖出下单：冻结持仓
 * delta_security_a = -qty (可卖减少), delta_security_f = +qty (冻结增加)
 */
async function freezeHolding(accountId, stockCode, quantity) {
  return callAccountApi("/api/security-accounts/updateHolding", {
    sec_acc_no: accountId,
    stock_code: stockCode,
    delta_security_a: -quantity,
    delta_security_f: quantity,
  });
}

/**
 * 卖出成交：扣减冻结持仓
 * delta_security_f = -qty
 */
async function settleSellerHolding(accountId, stockCode, quantity) {
  return callAccountApi("/api/security-accounts/updateHolding", {
    sec_acc_no: accountId,
    stock_code: stockCode,
    delta_security_a: 0,
    delta_security_f: -quantity,
  });
}

/**
 * 买入成交：增加可用持仓
 * delta_security_a = +qty
 */
async function settleBuyerHolding(accountId, stockCode, quantity) {
  return callAccountApi("/api/security-accounts/updateHolding", {
    sec_acc_no: accountId,
    stock_code: stockCode,
    delta_security_a: quantity,
    delta_security_f: 0,
  });
}

/**
 * 撤单/过期：释放冻结持仓
 * delta_security_a = +qty (可卖恢复), delta_security_f = -qty (冻结减少)
 */
async function releaseHolding(accountId, stockCode, quantity) {
  return callAccountApi("/api/security-accounts/updateHolding", {
    sec_acc_no: accountId,
    stock_code: stockCode,
    delta_security_a: quantity,
    delta_security_f: -quantity,
  });
}

module.exports = {
  freezeFunds,
  settleBuyFunds,
  settleSellFunds,
  releaseFunds,
  freezeHolding,
  settleSellerHolding,
  settleBuyerHolding,
  releaseHolding,
};
