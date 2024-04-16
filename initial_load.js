const { pgClient, pgClientOLTP } = require("./dbClient");
const dayjs = require("dayjs");

pgClientOLTP.connect().then(() => {
  pgClient.connect().then(() => {
    lodatServicesAndTicketSalesFacts();
  });
});

async function lodatServicesAndTicketSalesFacts() {
  console.log("Started inital load");
  const { rows: ticketSales } =
    await pgClientOLTP.query(`SELECT json_build_object('ticket', t,
                         'seat_data', s,
                         'wagon', json_build_object(
                                      'id', w.id,
                                      'number', w.number,
                                      'type', w.type,
                                      'rental_price', w.rental_price,
                                      'train', tr
                                      ),
                         'passenger', p) as sale
                        FROM ticket t
                            JOIN passenger p ON t.passenger_id = p.id
                            JOIN seat s ON t.seat_id = s.id
                            JOIN wagon w ON s.wagon_id = w.id
                            JOIN train tr ON w.train_id = tr.id;`);

  const { rows: ticketRoutes } =
    await pgClientOLTP.query(`SELECT json_build_object('ticket_id', tr.ticket_id, 'route', json_agg(json_build_object(
                                                    'order', rp."order",
                                                    'a_station', ar_s,
                                                    'd_station', d_s
                                                    ) ORDER BY rp."order")) as route FROM ticket_route tr
                            INNER JOIN route_part rp ON tr.route_part_id = rp.id
                            JOIN segment sgm ON rp.segment_id = sgm.id
                            JOIN station ar_s ON sgm.a_station_id = ar_s.id
                            JOIN station d_s ON sgm.d_station_id = d_s.id GROUP BY tr.ticket_id;`);

  const { rows: servicesSales } = await pgClientOLTP.query(
    `SELECT * FROM tickets_services as ts INNER JOIN additional_service s ON ts.additional_service_id = s.id;`
  );
  for (let saleItem of ticketSales) {
    const ticketSale = saleItem.sale;
    try {
      console.log("Iteration " + ticketSale.ticket.id);
      await pgClient.query("BEGIN TRANSACTION;");
      const ticketRoute = ticketRoutes.find(
        (item) => item.route.ticket_id === ticketSale.ticket.id
      );
      if (!ticketRoute) {
        console.log("No route for ticket");
        await pgClient.query("ROLLBACK TRANSACTION;");
        continue;
      }
      const seat = ticketSale.seat_data;
      const wagon = ticketSale.wagon;
      const train = wagon.train;
      const age =
        dayjs().get("year") -
        dayjs(ticketSale.passenger.birth_date).get("year");

      const seatId = await getOrCreateSeat(seat);

      const ageId = await getOrCreateAge(age);

      const date_sale = dayjs(ticketSale.ticket.purchase_timestamp);
      const dateId = await getOrCreateDate(date_sale);
      const date_usage = ticketSale.ticket.usage_timestamp
        ? dayjs(ticketSale.ticket.usage_timestamp)
        : dayjs(ticketSale.ticket.purchase_timestamp);
      const usageDateid = await getOrCreateDate(date_usage);

      const timeId = await getOrCreateTime(date_sale);

      const wagonId = await getOrCreateWagon(wagon, train);

      const startStationid = await getOrCreateStation(
        ticketRoute.route.route[0].a_station.name
      );
      const finalStationId = await getOrCreateStation(
        ticketRoute.route.route[ticketRoute.route.route.length - 1].d_station
          .name
      );

      const days_diff = date_usage.diff(date_sale, "day");
      const res = await pgClient.query(
        `INSERT INTO fact_sales_and_usage(wagon, age, seat, date_sale, time_sale, date_usage, start_station, final_station, ticket_cost, days_diff) VALUES
        (${wagonId}, ${ageId}, ${seatId}, ${dateId}, ${timeId}, ${usageDateid}, ${startStationid}, ${finalStationId}, ${ticketSale.ticket.price}, ${days_diff});`
      );
      console.log("Inserted rows -", res.rowCount);
      await pgClient.query("COMMIT TRANSACTION");
    } catch (e) {
      console.log("Failed - ", e);
      await pgClient.query("ROLLBACK TRANSACTION");
    }
  }

  for (let service of servicesSales) {
    try {
      await pgClient.query("BEGIN TRANSACTION");

      const ticketRoute = ticketRoutes.find(
        (item) => service.ticket_id === item.route.ticket_id
      );
      if (!ticketRoute) {
        console.log("No route for ticket");
        await pgClient.query("ROLLBACK TRANSACTION;");
        continue;
      }

      const ticketSale = ticketSales.find(
        (item) => item.sale.ticket.id === service.ticket_id
      );
      if (!ticketRoute) {
        console.log("No ticket sales for this service");
        await pgClient.query("ROLLBACK TRANSACTION;");
        continue;
      }
      const seat = ticketSale.sale.seat_data;
      const wagon = ticketSale.sale.wagon;
      const train = wagon.train;

      const seatId = await getOrCreateSeat(seat);

      const date_sale = dayjs(service.sale_timestamp);
      const dateId = await getOrCreateDate(date_sale);

      const wagonId = await getOrCreateWagon(wagon, train);

      const timeId = await getOrCreateTime(date_sale);

      const startStationid = await getOrCreateStation(
        ticketRoute.route.route[0].a_station.name
      );
      const finalStationId = await getOrCreateStation(
        ticketRoute.route.route[ticketRoute.route.route.length - 1].d_station
          .name
      );

      const serviceId = await getOrCreateService({
        name: service.name,
        price: service.price,
      });

      const res = await pgClient.query(
        `INSERT INTO fact_sales_services(wagon, date_usage, seat, time_sale, start_station, final_station, service, service_price) VALUES 
      (${wagonId}, ${dateId}, ${seatId}, ${timeId}, ${startStationid}, ${finalStationId}, ${serviceId}, ${service.price_with_discount});`
      );
      console.log("Inserted rows -", res.rowCount);
      await pgClient.query("COMMIT TRANSACTION");
    } catch (e) {
      console.log("Failed - ", e);
      await pgClient.query("ROLLBACK TRANSACTION");
    }
  }
}

async function getOrCreateService(service) {
  let serviceId;
  const { rows: existingService, rowsCount: serviceExists } =
    await pgClient.query(
      `SELECT id FROM service WHERE name = '${service.name}';`
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
    `SELECT id FROM date d WHERE d.date = '${date.date()}' AND d.year = ${date.year()} AND d.month = ${date.month()};`
  );
  if (dateExists) {
    dateId = existingDate[0].id;
  } else {
    const { rows: insertedDate } = await pgClient.query(
      `INSERT INTO date(date, year, month, day, season, month_with_year) 
          VALUES ('${date.date()}', ${date.year()}, ${date.month()}, ${date.day()}, '${getSeason(
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
