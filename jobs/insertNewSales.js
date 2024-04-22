const { pgClient, pgClientOLTP } = require("../dbClient");
const dayjs = require("dayjs");
const {
  getOrCreateAge,
  getOrCreateDate,
  getOrCrateNADate,
  getOrCreateSeat,
  getOrCreateService,
  getOrCreateStation,
  getOrCreateTime,
  getOrCreateWagon,
} = require("../dimension_creators");

async function insertNewSales() {
  const { rowCount: isLoggedSales } = await pgClientOLTP.query(
    `SELECT id FROM created_log WHERE table_name = 'ticket';`
  );

  if (!isLoggedSales) {
    console.log("There are no new sales yet.");
    return;
  }

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
                            JOIN train tr ON w.train_id = tr.id WHERE t.id IN
                            (SELECT record_id FROM created_log WHERE table_name = 'ticket');`);

  const { rows: ticketRoutes } =
    await pgClientOLTP.query(`SELECT json_build_object('ticket_id', tr.ticket_id, 'route', json_agg(json_build_object(
                                                      'order', rp."order",
                                                      'a_station', ar_s,
                                                      'd_station', d_s
                                                      ) ORDER BY rp."order")) as route FROM ticket_route tr
                              INNER JOIN route_part rp ON tr.route_part_id = rp.id
                              JOIN segment sgm ON rp.segment_id = sgm.id
                              JOIN station ar_s ON sgm.a_station_id = ar_s.id
                              JOIN station d_s ON sgm.d_station_id = d_s.id
                              WHERE tr.ticket_id IN (SELECT record_id FROM created_log WHERE table_name = 'ticket')
                              GROUP BY tr.ticket_id;`);

  const { rows: servicesSales } = await pgClientOLTP.query(
    `SELECT * FROM tickets_services as ts INNER JOIN additional_service s ON ts.additional_service_id = s.id 
    WHERE ts.ticket_id IN (SELECT record_id FROM created_log WHERE table_name = 'ticket');`
  );

  await pgClient.query("BEGIN TRANSACTION;");

  for (let saleItem of ticketSales) {
    const ticketSale = saleItem.sale;
    try {
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
        : null;
      const usageDateid =
        date_usage && date_usage !== null
          ? await getOrCreateDate(date_usage)
          : await getOrCrateNADate();

      const timeId = await getOrCreateTime(date_sale);

      const wagonId = await getOrCreateWagon(wagon, train);

      const startStationid = await getOrCreateStation(
        ticketRoute.route.route[0].a_station.name
      );
      const finalStationId = await getOrCreateStation(
        ticketRoute.route.route[ticketRoute.route.route.length - 1].d_station
          .name
      );

      const days_diff =
        date_usage && date_usage !== null
          ? date_usage.diff(date_sale, "day")
          : -1;

      await pgClient.query(
        `INSERT INTO fact_sales_and_usage(wagon, age, seat, date_sale, time_sale, date_usage, start_station, final_station, ticket_cost, days_diff) VALUES
          (${wagonId}, ${ageId}, ${seatId}, ${dateId}, ${timeId}, ${usageDateid}, ${startStationid}, ${finalStationId}, ${ticketSale.ticket.price}, ${days_diff});`
      );
    } catch (e) {
      console.log("Failed - ", e);
      await pgClient.query("ROLLBACK TRANSACTION");
    }
  }

  for (let service of servicesSales) {
    try {
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

      await pgClient.query(
        `INSERT INTO fact_sales_services(wagon, date_usage, seat, time_sale, start_station, final_station, service, service_price) VALUES
      (${wagonId}, ${dateId}, ${seatId}, ${timeId}, ${startStationid}, ${finalStationId}, ${serviceId}, ${service.price_with_discount});`
      );
    } catch (e) {
      console.log("Failed - ", e);
      await pgClient.query("ROLLBACK TRANSACTION");
    }
  }

  await pgClientOLTP.query(
    `DELETE FROM created_log WHERE table_name = 'ticket';`
  );

  await pgClient.query("COMMIT TRANSACTION");

  console.log("Sales insertion task completed!");
}

module.exports = {
  insertNewSales,
};
