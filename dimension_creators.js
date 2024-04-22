const { pgClient } = require("./dbClient");

async function getOrCreateService(service) {
  let serviceId;
  const { rows: existingService, rowCount: serviceExists } =
    await pgClient.query(
      `SELECT id FROM service WHERE name = '${service.name}' AND price = ${service.price};`
    );
  if (serviceExists) {
    serviceId = existingService[0].id;
  } else {
    const { rows: insertedService } = await pgClient.query(
      `INSERT INTO service(name, price) VALUES ('${service.name}', ${service.price}) RETURNING id;`
    );
    serviceId = insertedService[0].id;
  }
  return serviceId;
}

async function getOrCreateWagon(wagon, train) {
  let wagonid;
  const { rows: existingWagons, rowCount: wagonExists } = await pgClient.query(
    `SELECT id FROM wagon WHERE wagon_number = '${wagon.number}' AND train_number = '${train.number}'`
  );
  if (wagonExists) {
    wagonid = existingWagons[0].id;
  } else {
    const { rows: insertedWagon } = await pgClient.query(
      `INSERT INTO wagon(wagon_type, train_number, train_type, wagon_number, train_class) VALUES
          ('${wagon.type}', '${train.number}', '${train.type}', '${wagon.number}', '${train.class}') RETURNING id;`
    );
    wagonid = insertedWagon[0].id;
  }
  return wagonid;
}

async function getOrCreateSeat(seat) {
  let seatId;
  const { rows: existingSeat, rowCount: seatExists } = await pgClient.query(
    `SELECT id FROM seat WHERE number = ${+seat.number};`
  );
  if (seatExists) {
    seatId = existingSeat[0].id;
  } else {
    const { rows: insertedSeat } = await pgClient.query(
      `INSERT INTO seat(number) VALUES (${+seat.number}) RETURNING id;`
    );
    seatId = insertedSeat[0].id;
  }
  return seatId;
}

async function getOrCreateAge(age) {
  let ageGroup;
  if (age < 18) {
    ageGroup = "Under 18";
  } else if (age > 18 && age < 25) {
    ageGroup = "18 to 25";
  } else if (age >= 25 && age < 65) {
    ageGroup = "25 to 65";
  } else {
    ageGroup = "Above 65";
  }
  let ageId;
  const { rows: existingAge, rowCount: ageExists } = await pgClient.query(
    `SELECT id FROM age WHERE age_value = ${age};`
  );
  if (ageExists) {
    ageId = existingAge[0].id;
  } else {
    const { rows: insertedAge } = await pgClient.query(
      `INSERT INTO age(age_value, age_group) VALUES (${age}, '${ageGroup}') RETURNING id;`
    );
    ageId = insertedAge[0].id;
  }
  return ageId;
}

async function getOrCreateDate(date) {
  let dateId;
  const { rows: existingDate, rowCount: dateExists } = await pgClient.query(
    `SELECT id FROM date d WHERE d.date = ${date.date()} 
    AND d.year = ${date.year()} 
    AND d.month = ${date.month()} 
    AND d.day = ${date.day()};`
  );
  if (dateExists) {
    dateId = existingDate[0].id;
  } else {
    const { rows: insertedDate } = await pgClient.query(
      `INSERT INTO date(date, year, month, day, season, month_with_year) 
          VALUES (${date.date()}, ${date.year()}, ${date.month()}, ${date.day()}, '${getSeason(
        date.month() + 1
      )}', '${date.month()}.${date.year()}') RETURNING id;`
    );
    dateId = insertedDate[0].id;
  }
  return dateId;
}

async function getOrCreateTime(date) {
  let timeId;
  const { rows: existingTime, rowCount: timeExists } = await pgClient.query(
    `SELECT id FROM time WHERE hours = ${date.hour()} AND minutes = ${date.minute()}`
  );
  if (timeExists) {
    timeId = existingTime[0].id;
  } else {
    const { rows: insertedTime } = await pgClient.query(
      `INSERT INTO time(minutes, hours) VALUES (${date.hour()}, ${date.minute()}) RETURNING id;`
    );
    timeId = insertedTime[0].id;
  }
  return timeId;
}

async function getOrCreateStation(name) {
  const { rows: existingStation, rowCount: stationExists } =
    await pgClient.query(`SELECT id FROM station WHERE name = '${name}';`);
  if (stationExists) {
    return existingStation[0].id;
  } else {
    const { rows: insertedStation } = await pgClient.query(
      `INSERT INTO station(name) VALUES ('${name}') RETURNING id;`
    );
    return insertedStation[0].id;
  }
}

async function getOrCrateNADate() {
  const { rows: NADate, rowCount: NADateExists } = await pgClient.query(
    `SELECT id FROM date d WHERE d.month_with_year  = 'N/A';`
  );
  if (NADateExists) {
    return NADate[0].id;
  } else {
    const { rows: insertedNADate } =
      await pgClient.query(`INSERT INTO date(date, year, month, day, season, month_with_year)
                            VALUES (0, 0, 0, 0, 'N/A', 'N/A');`);
    return insertedNADate[0].id;
  }
}

function getSeason(month) {
  if (month < 3) {
    return "Winter";
  } else if (month >= 3 && month < 6) {
    return "Spring";
  } else if (month >= 6 && month < 9) {
    return "Summer";
  } else if (month >= 9 && month < 12) {
    return "Autumn";
  } else {
    return "Winter";
  }
}

module.exports = {
  getOrCreateAge,
  getOrCreateDate,
  getOrCrateNADate,
  getOrCreateSeat,
  getOrCreateService,
  getOrCreateStation,
  getOrCreateTime,
  getOrCreateWagon,
};
