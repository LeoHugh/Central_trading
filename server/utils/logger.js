/**
 * 中央交易系统 — 简易日志工具
 */

const PREFIX = "[CentralTrading]";

function ts() {
  return new Date().toISOString();
}

const logger = {
  info(...args) {
    console.log(ts(), PREFIX, "[INFO]", ...args);
  },
  warn(...args) {
    console.warn(ts(), PREFIX, "[WARN]", ...args);
  },
  error(...args) {
    console.error(ts(), PREFIX, "[ERROR]", ...args);
  },
  debug(...args) {
    if (process.env.DEBUG === "true") {
      console.log(ts(), PREFIX, "[DEBUG]", ...args);
    }
  },
};

module.exports = logger;
