/**
 * 中央交易系统 — Kafka 消息消费者
 *
 * 消费来自交易客户端的 3 个 Topic：
 *  - central.order.command  : 买卖委托
 *  - central.cancel.command : 取消委托
 *  - central.stock.query    : 查询股票行情
 */

const { TOPICS } = require("../utils/constants");
const logger = require("../utils/logger");

let consumer = null;
let messageHandlers = {};

/**
 * 设置 Kafka consumer 实例
 */
function setConsumer(c) {
  consumer = c;
}

/**
 * 注册消息处理回调
 * @param {Object} handlers { onOrderCommand, onCancelCommand, onStockQuery }
 */
function registerHandlers(handlers) {
  messageHandlers = handlers;
}

/**
 * 订阅并启动消息消费
 */
async function startConsuming() {
  if (!consumer) {
    logger.warn("Kafka consumer 未初始化，跳过消费启动");
    return;
  }

  await consumer.subscribe({ topic: TOPICS.orderCommand, fromBeginning: false });
  await consumer.subscribe({ topic: TOPICS.cancelCommand, fromBeginning: false });
  await consumer.subscribe({ topic: TOPICS.stockQuery, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let payload;
      try {
        const raw = message.value ? message.value.toString() : "{}";
        payload = JSON.parse(raw);
      } catch (err) {
        logger.error(`[Kafka] 消息解析失败: topic=${topic}`, err.message);
        return;
      }

      try {
        if (topic === TOPICS.orderCommand && messageHandlers.onOrderCommand) {
          await messageHandlers.onOrderCommand(payload);
        } else if (topic === TOPICS.cancelCommand && messageHandlers.onCancelCommand) {
          await messageHandlers.onCancelCommand(payload);
        } else if (topic === TOPICS.stockQuery && messageHandlers.onStockQuery) {
          await messageHandlers.onStockQuery(payload);
        } else {
          logger.warn(`[Kafka] 未知 topic 或无处理器: ${topic}`);
        }
      } catch (err) {
        logger.error(`[Kafka] 消息处理异常: topic=${topic}`, err.message, err.stack);
      }
    },
  });

  logger.info(
    `Kafka 消费者已启动，订阅: ${TOPICS.orderCommand}, ${TOPICS.cancelCommand}, ${TOPICS.stockQuery}`
  );
}

module.exports = {
  setConsumer,
  registerHandlers,
  startConsuming,
};
