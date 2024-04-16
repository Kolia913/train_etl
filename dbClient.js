require("dotenv").config();
const { Client } = require("pg");

const pgClient = new Client({
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_DATABASE,
});

const pgClientOLTP = new Client({
  user: process.env.DB_USER_OLTP,
  password: process.env.DB_PASS_OLTP,
  host: process.env.DB_HOST_OLTP,
  port: process.env.DB_PORT_OLTP,
  database: process.env.DB_DATABASE_OLTP,
});

module.exports = {
  pgClient,
  pgClientOLTP,
};
