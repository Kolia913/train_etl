const { pgClient, pgClientOLTP } = require("../dbClient");
const dayjs = require("dayjs");
const {
  getOrCreateAge,
  getOrCreateDate,
  getOrCrateNADate,
  getOrCreateSeat,
  getOrCreateStation,
  getOrCreateTime,
  getOrCreateWagon,
} = require("../dimension_creators");

async function updateTicketUsage() {
  const { rowCount: isUpdates } = await pgClientOLTP.query(
    `SELECT id FROM updated_log WHERE table_name = 'ticket';`
  );

  if (!isUpdates) {
    console.log("There are no updates yet.");
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
                            (SELECT record_id FROM updated_log WHERE table_name = 'ticket');`);

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
                              WHERE tr.ticket_id IN (SELECT record_id FROM updated_log WHERE table_name = 'ticket')
                              GROUP BY tr.ticket_id;`);

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
        `UPDATE fact_sales_and_usage SET
             date_usage = ${usageDateid},
             days_diff = ${days_diff}
             WHERE wagon = ${wagonId} AND age = ${ageId}
             AND seat = ${seatId} AND date_sale = ${dateId}
             AND time_sale = ${timeId} AND start_station = ${startStationid} 
             AND final_station = ${finalStationId};`
      );
    } catch (e) {
      console.log("Failed - ", e);
      await pgClient.query("ROLLBACK TRANSACTION");
    }
  }

  await pgClientOLTP.query(
    `DELETE FROM updated_log WHERE table_name = 'ticket';`
  );

  await pgClient.query("COMMIT TRANSACTION;");
  console.log("Sales update task completed!");
}

module.exports = {
  updateTicketUsage,
};
