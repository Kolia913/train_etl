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
