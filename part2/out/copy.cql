COPY aviation.airport_carrier_departure FROM '/home/ubuntu/cs598/part2/out/2_1_aiport_carrier_departure.csv' WITH DELIMITER=',' AND HEADER=TRUE;

COPY aviation.airport_airport_departure FROM '/home/ubuntu/cs598/part2/out/2_2_aiport_departure.csv' WITH DELIMITER=',' AND HEADER=TRUE;

COPY aviation.airport_airport_arrival FROM '/home/ubuntu/cs598/part2/out/2_4_airport_airport_arrival.csv' WITH DELIMITER=',' AND HEADER=TRUE;

COPY aviation.best_flights_2008 (airport_from,airport_to,given_date,carrier,flight_num,departure_time,arr_delay) FROM '/home/ubuntu/cs598/part2/out/3_2_best_flight_2008.csv' WITH DELIMITER=',' AND HEADER=TRUE;
