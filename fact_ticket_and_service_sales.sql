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

