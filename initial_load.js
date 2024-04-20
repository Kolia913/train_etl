const { pgClient, pgClientOLTP } = require("./dbClient");
const dayjs = require("dayjs");
const {
  getOrCreateAge,
  getOrCreateDate,
  getOrCreateSeat,
  getOrCreateService,
  getOrCreateStation,
  getOrCreateTime,
  getOrCreateWagon,
} = require("./dimension_creators");

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

  await pgClientOLTP.query(`CREATE OR REPLACE FUNCTION extract_date_components(input_date DATE)
                    RETURNS TEXT AS
                    $$
                    DECLARE
                        day_component TEXT;
                        month_component TEXT;
                        year_component TEXT;
                    BEGIN
                        -- Extract day, month, and year components
                        day_component := LPAD(EXTRACT(DAY FROM input_date)::TEXT, 2, '0');
                        month_component := LPAD(EXTRACT(MONTH FROM input_date)::TEXT, 2, '0');
                        year_component := EXTRACT(YEAR FROM input_date)::TEXT;

                        -- Return formatted date string
                        RETURN year_component || '-' || month_component || '-' || day_component;
                    END;
                    $$
                    LANGUAGE plpgsql;`);

  const { rows: wagonRoute } =
    await pgClientOLTP.query(`SELECT w.id as wagon_id,
       json_agg(json_build_object('order', rt."order", 'arrival_station', ar_s.name, 'departure_station', d_s.name) ORDER BY rt."order") as route
      FROM route_part rt
          JOIN wagon w ON rt.wagon_id = w.id
          JOIN segment sgm ON rt.segment_id = sgm.id
          JOIN station ar_s ON sgm.a_station_id = ar_s.id
          JOIN station d_s ON sgm.d_station_id = d_s.id
      GROUP BY w.id;`);

  const { rows: wagonsEfficiency } =
    await pgClientOLTP.query(`SELECT  w.id as wagon_id, json_build_object('wagon', w, 'train', tr) as w_data, extract_date_components(t.purchase_timestamp::DATE) as sale_date,
              ROUND(w.rental_price) as rental_price,
              COUNT(t)
                  as passenger_count,
              ROUND(COALESCE(SUM(t.price), 0)::NUMERIC, 2)
                  as tickets_income,
              COALESCE(SUM(ts.price_with_discount), 0)
                  as services_income,
              ROUND(CAST(COALESCE(COUNT(t), 0) * 100 as DECIMAL) / (SELECT COUNT(*) FROM seat WHERE wagon_id = w.id), 2)
                  as occupancy_percentage,
              ROUND((w.rental_price - COALESCE(SUM(t.price), 0) + COALESCE(SUM(ts.price_with_discount), 0))::NUMERIC, 2)
                  as marginal_income
        FROM ticket t
            JOIN seat st ON t.seat_id = st.id
            FULL JOIN wagon w ON st.wagon_id = w.id
            FULL JOIN tickets_services ts ON  ts.ticket_id = t.id
            JOIN train tr ON w.train_id = tr.id
        WHERE extract_date_components(t.purchase_timestamp::DATE) >=
              extract_date_components((SELECT MIN(purchase_timestamp)::DATE FROM ticket)) AND
            extract_date_components(t.purchase_timestamp::DATE) <=
            extract_date_components((SELECT MAX(purchase_timestamp)::DATE FROM ticket))
        GROUP BY w.id, tr.id, extract_date_components(t.purchase_timestamp::DATE);`);

  for (efficiencyUnit of wagonsEfficiency) {
    try {
      await pgClient.query("BEGIN TRANSACTION");
      const wagonRt = wagonRoute.find(
        (item) => item.wagon_id === efficiencyUnit.wagon_id
      );

      if (!wagonRt) {
        continue;
      }
      const wagonId = await getOrCreateWagon(
        efficiencyUnit.w_data.wagon,
        efficiencyUnit.w_data.train
      );
      const dateId = await getOrCreateDate(dayjs(efficiencyUnit.sale_date));
      const startStationid = await getOrCreateStation(
        wagonRt.route[0].arrival_station
      );
      const finalStationId = await getOrCreateStation(
        wagonRt.route[wagonRt.route.length - 1].departure_station
      );

      const res = await pgClient.query(
        `INSERT INTO
        fact_wagon_efficiency(wagon, date, start_station, final_station, wagon_prime_cost,
          tickets_income, services_income, marginal_income, occupancy_percentage, average_passenger_count)
        VALUES
        (${wagonId}, ${dateId}, ${startStationid}, ${finalStationId},${efficiencyUnit.rental_price}, ${efficiencyUnit.tickets_income},
        ${efficiencyUnit.services_income},
          ${efficiencyUnit.marginal_income}, ${efficiencyUnit.occupancy_percentage}, ${efficiencyUnit.passenger_count})`
      );
      console.log("Inserted rows:", res.rowCount);
      await pgClient.query("COMMIT TRANSACTION");
    } catch (e) {
      console.log(e);
      await pgClient.query("ROLLBACK TRANSACTION");
    }
  }
}
