package com.trading.central.scheduler;

import com.trading.central.engine.MatchingEngine;
import com.trading.central.kafka.KafkaProducerService;
import com.trading.central.service.AccountService;
import com.trading.central.service.TradeService;
import com.trading.central.util.Constants.OrderStatus;
import com.trading.central.util.Constants.Side;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class ExpiryJob {

    private final JdbcTemplate jdbcTemplate;
    private final MatchingEngine matchingEngine;
    private final AccountService accountService;
    private final TradeService tradeService;
    private final KafkaProducerService kafkaProducerService;

    @Value("${app.trading.end-hour:15}")
    private int endHour;

    @Value("${app.trading.end-minute:0}")
    private int endMinute;

    private String lastExpiredDate = "";

    public ExpiryJob(JdbcTemplate jdbcTemplate, MatchingEngine matchingEngine, AccountService accountService, TradeService tradeService, KafkaProducerService kafkaProducerService) {
        this.jdbcTemplate = jdbcTemplate;
        this.matchingEngine = matchingEngine;
        this.accountService = accountService;
        this.tradeService = tradeService;
        this.kafkaProducerService = kafkaProducerService;
    }

    @Scheduled(fixedRate = 60000)
    public void checkAndExpire() {
        LocalDateTime now = LocalDateTime.now();
        LocalDate today = now.toLocalDate();
        String todayText = today.toString();

        if (todayText.equals(lastExpiredDate)) {
            return;
        }

        if (now.getHour() > endHour || (now.getHour() == endHour && now.getMinute() >= endMinute)) {
            log.info("[ExpiryJob] 收盘时间已过，开始执行 {} 过期清理...", todayText);
            expireOrders(today);
            lastExpiredDate = todayText;
        }
    }

    public Map<String, Object> expireOrders(LocalDate tradeDate) {
        String sql = "SELECT order_id, account_id, stock_code, side, price, remaining_quantity, status " +
                     "FROM order_book WHERE trade_date = ? AND status IN (?, ?)";
        List<Map<String, Object>> orders = jdbcTemplate.queryForList(sql, tradeDate, OrderStatus.ACCEPTED.name(), OrderStatus.PART_TRADED.name());

        Map<String, Object> summary = new HashMap<>();
        summary.put("tradeDate", tradeDate.toString());
        summary.put("total", orders.size());

        if (orders.isEmpty()) {
            log.info("[ExpiryJob] {} 无需过期的委托", tradeDate);
            summary.put("expiredCount", 0);
            summary.put("failedCount", 0);
            return summary;
        }

        log.info("[ExpiryJob] {} 共 {} 条委托需要过期", tradeDate, orders.size());
        int expiredCount = 0;
        int failedCount = 0;

        for (Map<String, Object> order : orders) {
            String orderId = order.get("order_id").toString();
            try {
                jdbcTemplate.update("UPDATE order_book SET status = ?, update_time = NOW() WHERE order_id = ?", OrderStatus.EXPIRED.name(), orderId);
                matchingEngine.cancelOrderInBook(orderId, order.get("stock_code").toString());

                int remainQty = Integer.parseInt(order.get("remaining_quantity").toString());
                if (remainQty > 0) {
                    BigDecimal price = new BigDecimal(order.get("price").toString());
                    String accountId = order.get("account_id").toString();
                    String stockCode = order.get("stock_code").toString();
                    if (Side.BUY.name().equals(order.get("side").toString())) {
                        accountService.releaseFunds(accountId, price.multiply(new BigDecimal(remainQty)));
                    } else {
                        accountService.releaseHolding(accountId, stockCode, remainQty);
                    }
                }

                kafkaProducerService.sendOrderReport(orderId, OrderStatus.EXPIRED.name(), "当日委托已过期");
                expiredCount++;
            } catch (Exception err) {
                failedCount++;
                log.error("[ExpiryJob] 过期处理失败: {}", orderId, err);
            }
        }

        tradeService.resetTradeSeq();
        log.info("[ExpiryJob] 过期清理完成: {}/{} 条委托已过期", expiredCount, orders.size());
        summary.put("expiredCount", expiredCount);
        summary.put("failedCount", failedCount);
        return summary;
    }
}
