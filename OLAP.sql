CREATE TABLE wagon (
    id SERIAL PRIMARY KEY,
    wagon_type VARCHAR,
    train_number VARCHAR,
    train_type VARCHAR,
    wagon_number VARCHAR,
    train_class VARCHAR
);

CREATE TABLE station (
    id SERIAL PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE seat (
    id SERIAL PRIMARY KEY,
    number INTEGER
);

CREATE TABLE age (
    id SERIAL PRIMARY KEY,
    age_value INTEGER,
    age_group VARCHAR
);

CREATE TABLE service (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    price FLOAT
);

CREATE TABLE date (
    id SERIAL PRIMARY KEY,
    date VARCHAR,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    season VARCHAR,
    month_with_year VARCHAR
);

CREATE TABLE time (
    id SERIAL PRIMARY KEY,
    minutes INTEGER,
    hours INTEGER
);

CREATE TABLE fact_sales_and_usage (
    wagon INTEGER REFERENCES wagon(id) ON DELETE RESTRICT,
    age INTEGER REFERENCES age(id) ON DELETE RESTRICT,
    seat INTEGER REFERENCES seat(id) ON DELETE RESTRICT,
    date_sale INTEGER REFERENCES date(id) ON DELETE RESTRICT,
    time_sale INTEGER REFERENCES time(id) ON DELETE RESTRICT,
    date_usage INTEGER REFERENCES date(id) ON DELETE RESTRICT,
    start_station INTEGER REFERENCES station(id) ON DELETE RESTRICT,
    final_station INTEGER REFERENCES station(id) ON DELETE RESTRICT,
    ticket_cost FLOAT,
    days_diff INTEGER,
    PRIMARY KEY (wagon, age, seat, date_sale, time_sale, date_usage, start_station, final_station)
);

CREATE TABLE fact_wagon_efficiency (
  wagon INTEGER REFERENCES wagon(id) ON DELETE RESTRICT,
  date INTEGER REFERENCES date(id) ON DELETE RESTRICT,
  start_station INTEGER REFERENCES station(id) ON DELETE RESTRICT,
  final_station INTEGER REFERENCES station(id) ON DELETE RESTRICT,
  wagon_prime_cost FLOAT,
  tickets_income FLOAT,
  services_income FLOAT,
  marginal_income FLOAT,
  occupancy_percentage INTEGER,
  average_passenger_count INTEGER,
  PRIMARY KEY (wagon, date, start_station, final_station)
);

CREATE TABLE fact_sales_services (
    wagon INTEGER REFERENCES wagon(id) ON DELETE RESTRICT,
    date_usage INTEGER REFERENCES date(id) ON DELETE RESTRICT,
    seat INTEGER REFERENCES seat(id) ON DELETE RESTRICT,
    time_sale INTEGER REFERENCES time(id) ON DELETE RESTRICT,
    start_station INTEGER REFERENCES station(id) ON DELETE RESTRICT,
    final_station INTEGER REFERENCES station(id) ON DELETE RESTRICT,
    service INTEGER REFERENCES service(id) ON DELETE RESTRICT,
    service_price FLOAT,
    PRIMARY KEY (wagon, date_usage, seat, time_sale, start_station, final_station, service)
);