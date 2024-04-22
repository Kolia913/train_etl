const { insertNewSales } = require("./insertNewSales");
const { updateTicketUsage } = require("./updateTicketsUsage");
const { collectDailyStats } = require("./collectDailyStats");

module.exports = {
  insertNewSales,
  updateTicketUsage,
  collectDailyStats,
};
