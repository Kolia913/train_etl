const { pgClientOLTP } = require("./dbClient");

async function create_log_tables() {
  try {
    await pgClientOLTP.query(`BEGIN TRANSACTION`);
    await pgClientOLTP.query(`
    CREATE TABLE IF NOT EXISTS updated_log (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(255),
        record_id INT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`);
    await pgClientOLTP.query(`CREATE TABLE IF NOT EXISTS created_log (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(255),
        record_id INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`);

    await pgClientOLTP.query(`CREATE OR REPLACE FUNCTION log_update()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO updated_log (table_name, record_id)
        VALUES (TG_TABLE_NAME, NEW.id);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;`);

    await pgClientOLTP.query(`CREATE OR REPLACE FUNCTION log_insert()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO created_log (table_name, record_id)
        VALUES (TG_TABLE_NAME, NEW.id);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;`);

    await pgClientOLTP.query(`CREATE OR REPLACE TRIGGER record_update
    AFTER UPDATE ON ticket
    FOR EACH ROW
    EXECUTE FUNCTION log_update();`);

    await pgClientOLTP.query(`CREATE OR REPLACE TRIGGER record_insert
    AFTER INSERT ON ticket
    FOR EACH ROW
    EXECUTE FUNCTION log_insert();`);
    await pgClientOLTP.query(`CREATE OR REPLACE FUNCTION extract_date_components(input_date DATE)
    RETURNS TEXT AS
    $$
    DECLARE
        day_component TEXT;
        month_component TEXT;
        year_component TEXT;
    BEGIN
        day_component := LPAD(EXTRACT(DAY FROM input_date)::TEXT, 2, '0');
        month_component := LPAD(EXTRACT(MONTH FROM input_date)::TEXT, 2, '0');
        year_component := EXTRACT(YEAR FROM input_date)::TEXT;

        RETURN year_component || '-' || month_component || '-' || day_component;
    END;
    $$
    LANGUAGE plpgsql;`);
    await pgClientOLTP.query("COMMIT TRANSACTION");
    console.log("Successfully created log tables and triggers.");
  } catch (e) {
    await pgClientOLTP.query(`ROLLBACK TRANSACTION`);
    console.error("Error while creating log tables and triggers:", e);
  }
}

module.exports = {
  create_log_tables,
};
