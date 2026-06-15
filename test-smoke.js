// Quick smoke test for the Central Trading System REST API
const http = require("http");

function request(method, path, body) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const options = {
      hostname: "localhost",
      port: 8082,
      path,
      method,
      headers: { "Content-Type": "application/json" },
    };
    if (data) options.headers["Content-Length"] = Buffer.byteLength(data);

    const req = http.request(options, (res) => {
      let chunks = "";
      res.on("data", (c) => (chunks += c));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(chunks) });
        } catch {
          resolve({ status: res.statusCode, body: chunks });
        }
      });
    });
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

async function main() {
  console.log("=== Central Trading System Smoke Test ===\n");

  // 1. Health check
  const health = await request("GET", "/api/central-trading/health");
  console.log("1. Health check:", health.status, JSON.stringify(health.body));

  // 2. Submit BUY order
  const buy = await request("POST", "/api/central-trading/orders", {
    fundAccountNo: "6222026000000001",
    stockCode: "600519",
    direction: "BUY",
    price: 1688.35,
    quantity: 100,
  });
  console.log("2. Submit BUY:", buy.status, JSON.stringify(buy.body));

  // 3. Submit SELL order (should trigger matching)
  const sell = await request("POST", "/api/central-trading/orders", {
    fundAccountNo: "6222026000000002",
    stockCode: "600519",
    direction: "SELL",
    price: 1680.00,
    quantity: 50,
  });
  console.log("3. Submit SELL:", sell.status, JSON.stringify(sell.body));

  // 4. Query stock
  const stocks = await request("GET", "/api/central-trading/stocks?keyword=600519");
  console.log("4. Query stock:", stocks.status, JSON.stringify(stocks.body));

  // 5. Admin: view order book
  const book = await request("GET", "/api/central-trading/admin/stocks/600519/orders");
  console.log("5. Order book:", book.status, JSON.stringify(book.body));

  // 6. Cancel order
  const orderId = buy.body?.data?.orderNo;
  if (orderId) {
    const cancel = await request("POST", `/api/central-trading/orders/${orderId}/cancel`, {
      fundAccountNo: "6222026000000001",
    });
    console.log("6. Cancel order:", cancel.status, JSON.stringify(cancel.body));
  }

  console.log("\n=== Done ===");
}

main().catch(console.error);
