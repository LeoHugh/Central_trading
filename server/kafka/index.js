/**
 * 中央交易系统 — Kafka 初始化与管理
 */

const logger = require("../utils/logger");
const { setProducer } = require("./producer");
const { setConsumer, registerHandlers, startConsuming } = require("./consumer");

let Kafka;
try {
  ({ Kafka } = require("kafkajs"));
} catch (err) {
  Kafka = null;
}

let kafkaReady = false;
let kafkaError = "";

function kafkaEnabled() {
  return process.env.KAFKA_ENABLED === "true";
}

/**
 * 初始化 Kafka 连接
 * @param {Object} handlers { onOrderCommand, onCancelCommand, onStockQuery }
 */
async function initKafka(handlers) {
  if (!kafkaEnabled()) {
    logger.info("Kafka 已禁用 (KAFKA_ENABLED != true)");
    return { ok: false, message: "Kafka disabled" };
  }

  if (!Kafka) {
    kafkaError = "kafkajs 未安装，请先运行 npm install";
    logger.error(kafkaError);
    return { ok: false, message: kafkaError };
  }

  const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
    .split(",")
    .map((b) => b.trim())
    .filter(Boolean);

  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || "central-trading",
    brokers,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "central-trading-group",
  });

  try {
    await producer.connect();
    logger.info("Kafka producer 已连接");

    setProducer(producer);

    await consumer.connect();
    logger.info("Kafka consumer 已连接");

    setConsumer(consumer);
    registerHandlers(handlers);
    await startConsuming();

    kafkaReady = true;
    return { ok: true };
  } catch (err) {
    kafkaError = err.message;
    logger.error("Kafka 启动失败:", err.message);
    return { ok: false, message: err.message };
  }
}

function getKafkaStatus() {
  return {
    enabled: kafkaEnabled(),
    ready: kafkaReady,
    error: kafkaError,
  };
}

module.exports = {
  initKafka,
  getKafkaStatus,
};
