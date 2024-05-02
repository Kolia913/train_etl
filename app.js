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
const bodyParser = require("body-parser");
const { createObjectCsvWriter } = require("csv-writer");
const fs = require("node:fs");
const { lodatServicesAndTicketSalesFacts } = require("./initial_load");
const cors = require("cors");

const PORT = process.env.PORT || 3030;

const insertSalesAndUpdateUsagesJob = cron.schedule("* * * * *", async () => {
  await insertNewSales();
  await updateTicketUsage();
});
// Every day “At 01:00.”
const collectStatsJob = cron.schedule("0 1 * * *", async () => {
  await collectDailyStats();
});

app.use(bodyParser.json());
app.use(cors());

function makeAliasFromName(name) {
  if (name === "service") {
    return "srv";
  }
  const nameParts = name.split("_");
  if (nameParts.length === 2) {
    return `${nameParts[0].charAt(0)}${nameParts[1].charAt(0)}`;
  } else {
    return nameParts[0].charAt(0);
  }
}

function isObject(value) {
  return Object.keys(value).length;
}

function objectToQuery(object, name) {
  let query = "";
  for (let key of Object.keys(object)) {
    if (object[key]) {
      if (isObject(object[key])) {
        query += objectToQuery(object[key], `${key}`);
      } else {
        if (name) {
          query += `${makeAliasFromName(name)}.${key} as ${makeAliasFromName(
            name
          )}_${key}, `;
        } else {
          query += `f.${key} as f_${key}, `;
        }
      }
    }
  }
  return query.slice(0, -1);
}

function objectToJsonQuery(object, name) {
  let query;
  if (name) {
    query = `'${name}', json_build_object(`;
  } else {
    query = "json_build_object(";
  }
  for (let key of Object.keys(object)) {
    if (object[key]) {
      if (isObject(object[key])) {
        query += objectToJsonQuery(object[key], `${key}`);
      } else {
        if (name) {
          query += `'${key}', ${makeAliasFromName(name)}.${key},`;
        } else {
          query += `'${key}', f.${key},`;
        }
      }
    }
  }
  query = query.slice(0, -1);
  if (!name) {
    query += `) as fact`;
  } else {
    query += "),";
  }

  return query;
}

app.post("/ticket-sales/export/json", async (req, res) => {
  const selectedCoumns = req.body;

  const sqlQuery = `SELECT  ${objectToJsonQuery(selectedCoumns)}
                    FROM fact_sales_and_usage f
                        JOIN wagon w ON f.wagon = w.id
                        JOIN age a ON f.age = a.id
                        JOIN date ds ON f.date_sale = ds.id
                        JOIN date du ON f.date_usage = du.id
                        JOIN time ts ON f.time_sale = ts.id
                        JOIN station ss ON f.start_station = ss.id
                        JOIN station fs ON f.final_station = fs.id
                        JOIN seat s ON f.seat = s.id;`;

  const result = await pgClient.query(sqlQuery);
  fs.writeFileSync(
    "./exports/data.json",
    JSON.stringify(result.rows.map((row) => row.fact))
  );

  const file = `${__dirname}/exports/data.json`;
  res.download(file);
});

app.post("/ticket-sales/export/csv", async (req, res) => {
  const selectedCoumns = req.body;

  const sqlQuery = `SELECT  ${objectToQuery(selectedCoumns)}
                    FROM fact_sales_and_usage f
                        JOIN wagon w ON f.wagon = w.id
                        JOIN age a ON f.age = a.id
                        JOIN date ds ON f.date_sale = ds.id
                        JOIN date du ON f.date_usage = du.id
                        JOIN time ts ON f.time_sale = ts.id
                        JOIN station ss ON f.start_station = ss.id
                        JOIN station fs ON f.final_station = fs.id
                        JOIN seat s ON f.seat = s.id;`;
  const result = await pgClient.query(sqlQuery);
  const csvWriter = createObjectCsvWriter({
    path: "./exports/data.csv",
    header: Object.keys(result.rows[0]).map((key) => ({
      id: key,
      title: key,
    })),
  });

  await csvWriter.writeRecords(result.rows);
  const file = `${__dirname}/exports/data.csv`;
  res.download(file);
  try {
    const data = "";
  } catch (e) {
    console.error("Error exporting to CSV:", e);
    res.status(500).send("Internal Server Error");
  }
});

app.post("/services-sales/export/json", async (req, res) => {
  const selectedCoumns = req.body;

  const sqlQuery = `SELECT  ${objectToJsonQuery(selectedCoumns)}
                    FROM fact_sales_services f
                      JOIN wagon w ON f.wagon = w.id
                      JOIN service srv ON f.service = srv.id
                      JOIN date du ON f.date_usage = du.id
                      JOIN time ts ON f.time_sale = ts.id
                      JOIN station ss ON f.start_station = ss.id
                      JOIN station fs ON f.final_station = fs.id
                      JOIN seat s ON f.seat = s.id;`;

  const result = await pgClient.query(sqlQuery);
  fs.writeFileSync(
    "./exports/data.json",
    JSON.stringify(result.rows.map((row) => row.fact))
  );

  const file = `${__dirname}/exports/data.json`;
  res.download(file);
});

app.post("/services-sales/export/csv", async (req, res) => {
  const selectedCoumns = req.body;

  const sqlQuery = `SELECT  ${objectToQuery(selectedCoumns)}
                    FROM fact_sales_services f
                      JOIN wagon w ON f.wagon = w.id
                      JOIN service srv ON f.service = srv.id
                      JOIN date du ON f.date_usage = du.id
                      JOIN time ts ON f.time_sale = ts.id
                      JOIN station ss ON f.start_station = ss.id
                      JOIN station fs ON f.final_station = fs.id
                      JOIN seat s ON f.seat = s.id;`;
  const result = await pgClient.query(sqlQuery);
  const csvWriter = createObjectCsvWriter({
    path: "./exports/data.csv",
    header: Object.keys(result.rows[0]).map((key) => ({
      id: key,
      title: key,
    })),
  });

  await csvWriter.writeRecords(result.rows);
  const file = `${__dirname}/exports/data.csv`;
  res.download(file);
  try {
    const data = "";
  } catch (e) {
    console.error("Error exporting to CSV:", e);
    res.status(500).send("Internal Server Error");
  }
});

app.post("/wagon-efficiency/export/json", async (req, res) => {
  const selectedCoumns = req.body;

  const sqlQuery = `SELECT  ${objectToJsonQuery(selectedCoumns)}
                    FROM fact_wagon_efficiency f
                        JOIN wagon w ON f.wagon = w.id
                        JOIN date d ON f.date = d.id
                        JOIN station ss ON f.start_station = ss.id
                        JOIN station fs ON f.final_station = fs.id;`;

  const result = await pgClient.query(sqlQuery);
  fs.writeFileSync(
    "./exports/data.json",
    JSON.stringify(result.rows.map((row) => row.fact))
  );

  const file = `${__dirname}/exports/data.json`;
  res.download(file);
});

app.post("/wagon-efficiency/export/csv", async (req, res) => {
  const selectedCoumns = req.body;

  const sqlQuery = `SELECT  ${objectToQuery(selectedCoumns)}
                    FROM fact_wagon_efficiency f
                        JOIN wagon w ON f.wagon = w.id
                        JOIN date d ON f.date = d.id
                        JOIN station ss ON f.start_station = ss.id
                        JOIN station fs ON f.final_station = fs.id;`;
  const result = await pgClient.query(sqlQuery);
  const csvWriter = createObjectCsvWriter({
    path: "./exports/data.csv",
    header: Object.keys(result.rows[0]).map((key) => ({
      id: key,
      title: key,
    })),
  });

  await csvWriter.writeRecords(result.rows);
  const file = `${__dirname}/exports/data.csv`;
  res.download(file);
  try {
    const data = "";
  } catch (e) {
    console.error("Error exporting to CSV:", e);
    res.status(500).send("Internal Server Error");
  }
});

app.post("/initial-load", async (req, res) => {
  try {
    const { rowCount: isEff } = await pgClient.query(
      "SELECT * FROM fact_wagon_efficiency"
    );
    const { rowCount: isSrv } = await pgClient.query(
      "SELECT * FROM fact_sales_services"
    );
    const { rowCount: isTckt } = await pgClient.query(
      "SELECT * FROM fact_sales_and_usage"
    );
    if (isEff || isSrv || isTckt) {
      res
        .send(403)
        .send(
          "Your storage is not empty, please clean it before initial load data"
        );
    } else {
      await lodatServicesAndTicketSalesFacts();
      res.status(201).send("Initial load successfully completed!");
    }
  } catch (e) {
    res.status(500).send("Internal server error!");
  }
});

app.post("/collect-daily-stats", async (req, res) => {
  try {
    await collectDailyStats();
    res.status(201).send("Daily stats collected!");
  } catch (e) {
    console.log(e);
    res.status(500).send("Internal server error");
  }
});

app.get("/stats", async (req, res) => {
  const filters = req.query;
  const { rows: sales } = await pgClient.query(
    `SELECT COUNT(*) as sls_count FROM fact_sales_and_usage f 
      JOIN wagon w ON f.wagon = w.id
      JOIN date d ON f.date_sale = d.id
      WHERE w.wagon_type = '${filters.wagonType}' AND d.month_with_year = '${filters.monthWithYear}';`
  );

  const { rows: services } =
    await pgClient.query(`SELECT COUNT(*) as srv_count FROM fact_sales_services f 
      JOIN wagon w ON f.wagon = w.id
      JOIN date d ON f.date_usage = d.id
      WHERE w.wagon_type = '${filters.wagonType}' AND d.month_with_year = '${filters.monthWithYear}';`);

  const { rows: efficiency } = await pgClient.query(
    `SELECT 
      SUM(f.tickets_income) as tickets_income, 
      SUM(f.services_income) as services_income, 
      SUM(f.wagon_prime_cost) as prime_cost, 
      SUM(f.marginal_income) as marginal_income
    FROM fact_wagon_efficiency f JOIN wagon w ON f.wagon = w.id
      JOIN date d ON f.date = d.id WHERE w.wagon_type = '${filters.wagonType}' AND d.month_with_year = '${filters.monthWithYear}';`
  );

  const stats = {
    sold_tickets_count: sales[0].sls_count,
    sold_services_count: services[0].srv_count,
    tickets_income: efficiency[0].tickets_income?.toFixed(2),
    services_income: efficiency[0].services_income?.toFixed(2),
    prime_cost: efficiency[0].prime_cost?.toFixed(2),
    marginal_income: efficiency[0].marginal_income?.toFixed(2),
  };

  res.status(200).json(stats);
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
