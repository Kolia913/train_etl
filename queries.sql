SELECT json_build_object('ticket', t,
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
    JOIN train tr ON w.train_id = tr.id;

SELECT json_build_object('ticket_id', tr.ticket_id, 'route', json_agg(json_build_object(
                              'order', rp."order",
                              'a_station', ar_s,
                              'd_station', d_s
                              ) ORDER BY rp."order")) as route FROM ticket_route tr
    INNER JOIN route_part rp ON tr.route_part_id = rp.id
    JOIN segment sgm ON rp.segment_id = sgm.id
    JOIN station ar_s ON sgm.a_station_id = ar_s.id
    JOIN station d_s ON sgm.d_station_id = d_s.id GROUP BY tr.ticket_id;

SELECT * FROM tickets_services as ts INNER JOIN additional_service s ON ts.additional_service_id = s.id;


SELECT w.id as wagon_id, json_build_object('wagon', w, 'train', tr), w.rental_price, CAST(COUNT(*)  as DECIMAL) / (SELECT COUNT(*) FROM seat st WHERE st.wagon_id = w.id)
       as occupancy_percentage,
       COUNT(*) as passenger_count, COALESCE(SUM(t.price *
       (SELECT COUNT(*) FROM ticket t JOIN seat s ON t.seat_id = s.id JOIN wagon wag ON s.wagon_id = w.id WHERE wag.id = w.id)), 0)
       as tickets_income,
  COALESCE((SELECT SUM(srv.price) FROM tickets_services ts
        JOIN ticket tck ON ts.ticket_id = tck.id FULL OUTER JOIN additional_service srv ON srv.id = ts.additional_service_id
        JOIN seat st ON tck.seat_id = st.id JOIN wagon wgn ON st.wagon_id = wgn.id WHERE wgn.id = w.id), 0) as services_income
FROM ticket t
    JOIN seat s ON t.seat_id = s.id
    FULL OUTER JOIN wagon w ON s.wagon_id = w.id
    JOIN train tr ON w.train_id = tr.id
    GROUP BY w.id, tr.id;

SELECT '1970-04-16T14:59:40.380Z'::timestamptz;
SELECT '2024-04-16T14:59:40.381Z'::timestamptz;

SELECT w.id as wagon_id,
       json_agg(json_build_object('order', rt."order", 'arrival_station', ar_s.name, 'departure_station', d_s.name) ORDER BY rt."order")
FROM route_part rt
    JOIN wagon w ON rt.wagon_id = w.id
    JOIN segment sgm ON rt.segment_id = sgm.id
    JOIN station ar_s ON sgm.a_station_id = ar_s.id
    JOIN station d_s ON sgm.d_station_id = d_s.id
GROUP BY w.id;

SELECT w.id as wagon_id, ROUND(w.rental_price),
       COUNT(t)
           as passenger_count,
       ROUND(COALESCE(SUM(t.price), 0)::NUMERIC, 2)
           as tickets_income,
       COALESCE(SUM(ts.price_with_discount), 0)
           as services_income,
       ROUND(CAST(COALESCE(COUNT(t), 0) * 100 as DECIMAL) / (SELECT COUNT(*) FROM seat WHERE wagon_id = w.id), 2)
           as occupancy_percentage,
       ROUND((w.rental_price - COALESCE(SUM(t.price), 0))::NUMERIC, 2)
           as marginal_income
FROM ticket t
    JOIN seat st ON t.seat_id = st.id
    FULL JOIN wagon w ON st.wagon_id = w.id
    FULL JOIN tickets_services ts ON  ts.ticket_id = t.id
GROUP BY w.id;

CREATE OR REPLACE FUNCTION extract_date_components(input_date DATE)
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
LANGUAGE plpgsql;

SELECT MIN(purchase_timestamp) AS first_sale, MAX(purchase_timestamp) AS last_sale  FROM ticket;


SELECT  w.id as wagon_id, extract_date_components(t.purchase_timestamp::DATE),
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
GROUP BY w.id, tr.id, extract_date_components(t.purchase_timestamp::DATE);