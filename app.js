require("dotenv").config();
const { create_log_tables } = require("./create_log_tables");
const {
  insertNewSales,
  updateTicketUsage,
  collectDailyStats,
} = require("./jobs");
const cron = require("node-cron");
const express = require("express");
const { pgClientOLTP, pgClient } = require("./dbClient");
const app = express();

const PORT = process.env.PORT || 3030;

const insertSalesAndUpdateUsagesJob = cron.schedule("* * * * *", async () => {
  await insertNewSales();
  await updateTicketUsage();
});
// Every day “At 01:00.”
const collectStatsJob = cron.schedule("0 1 * * *", async () => {
  await collectDailyStats();
});

const server = app.listen(PORT, async () => {
  try {
    await pgClientOLTP.connect();
    await pgClient.connect();
    await create_log_tables();
    insertSalesAndUpdateUsagesJob.start();
    collectStatsJob.start();
    console.log(`Server is running on port ${PORT}`);
  } catch (e) {
    console.log(e);
    server.close();
  }
});

process.on("SIGTERM", shutDown);
process.on("SIGINT", shutDown);
process.on("exit", shutDown);

function shutDown() {
  console.log("Received kill signal, shutting down gracefully");

  insertSalesAndUpdateUsagesJob.stop();
  collectStatsJob.stop();

  server.close(() => {
    console.log("Closed out remaining connections");
    process.exit(0);
  });

  setTimeout(() => {
    console.error(
      "Could not close connections in time, forcefully shutting down"
    );
    process.exit(1);
  }, 10000);
}
