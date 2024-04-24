const { pgClient, pgClientOLTP } = require("../dbClient");
const dayjs = require("dayjs");
const {
  getOrCreateDate,
  getOrCreateStation,
  getOrCreateWagon,
} = require("../dimension_creators");

async function collectDailyStats() {
  const { rows: wagonsEfficiency } =
    await pgClientOLTP.query(`SELECT  w.id as wagon_id, json_build_object('wagon', w, 'train', tr) as w_data, 
              extract_date_components(t.purchase_timestamp::DATE) as sale_date,
              CAST(ROUND(w.rental_price) as DECIMAL) / 30 as rental_price,
              COUNT(t)
                  as passenger_count,
              ROUND(COALESCE(SUM(t.price), 0)::NUMERIC, 2)
                  as tickets_income,
              COALESCE(SUM(ts.price_with_discount), 0)
                  as services_income,
              ROUND(CAST(COALESCE(COUNT(t), 0) * 100 as DECIMAL) / (SELECT COUNT(*) FROM seat WHERE wagon_id = w.id), 2)
                  as occupancy_percentage,
              ROUND((COALESCE(SUM(t.price), 0) + COALESCE(SUM(ts.price_with_discount), 0) 
              - CAST(w.rental_price as DECIMAL) / 30)::NUMERIC, 2)
                  as marginal_income
        FROM ticket t
            JOIN seat st ON t.seat_id = st.id
            FULL JOIN wagon w ON st.wagon_id = w.id
            FULL JOIN tickets_services ts ON ts.ticket_id = t.id
            JOIN train tr ON w.train_id = tr.id
        WHERE extract_date_components(t.purchase_timestamp::DATE) = '${dayjs().format(
          "YYYY-MM-DD"
        )}'
        GROUP BY w.id, tr.id, extract_date_components(t.purchase_timestamp::DATE);`);

  const { rows: wagonRoute } =
    await pgClientOLTP.query(`SELECT w.id as wagon_id,
       json_agg(json_build_object('order', rt."order", 'arrival_station', ar_s, 'departure_station', d_s) ORDER BY rt."order") as route
      FROM route_part rt
          JOIN wagon w ON rt.wagon_id = w.id
          JOIN segment sgm ON rt.segment_id = sgm.id
          JOIN station ar_s ON sgm.a_station_id = ar_s.id
          JOIN station d_s ON sgm.d_station_id = d_s.id
      GROUP BY w.id;`);

  await pgClient.query("BEGIN TRANSACTION");
  for (efficiencyUnit of wagonsEfficiency) {
    try {
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
        wagonRt.route[0].arrival_station.name,
        wagonRt.route[0].arrival_station.lon,
        wagonRt.route[0].arrival_station.lat
      );
      const finalStationId = await getOrCreateStation(
        wagonRt.route[wagonRt.route.length - 1].departure_station.name,
        wagonRt.route[wagonRt.route.length - 1].departure_station.lon,
        wagonRt.route[wagonRt.route.length - 1].departure_station.lat
      );

      const res = await pgClient.query(
        `INSERT INTO
        fact_wagon_efficiency(wagon, date, start_station, final_station, wagon_prime_cost,
          tickets_income, services_income, marginal_income, occupancy_percentage, passenger_count)
        VALUES
        (${wagonId}, ${dateId}, ${startStationid}, ${finalStationId},${efficiencyUnit.rental_price},
        ${efficiencyUnit.tickets_income}, ${efficiencyUnit.services_income}, ${efficiencyUnit.marginal_income},
        ${efficiencyUnit.occupancy_percentage}, ${efficiencyUnit.passenger_count})`
      );
    } catch (e) {
      console.log(e);
      await pgClient.query("ROLLBACK TRANSACTION");
    }
  }

  await pgClient.query("COMMIT TRANSACTION");
  console.log("Daily stats collected!");
}

module.exports = {
  collectDailyStats,
};
