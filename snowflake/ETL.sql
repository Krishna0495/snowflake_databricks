insert into PH_DATA.NORTHWOODS.CANCEL_REASON_CODE
values ('A','Airline/Carrier');
insert into PH_DATA.NORTHWOODS.CANCEL_REASON_CODE
values ('B','Weather');
insert into PH_DATA.NORTHWOODS.CANCEL_REASON_CODE
values ('C','National Air System');
insert into PH_DATA.NORTHWOODS.CANCEL_REASON_CODE
values ('D','Security');

-- Update for PH_DATA.NORTHWOODS.AIRLINES

Update
    PH_DATA.NORTHWOODS.AIRLINES
set
    AIRLINE = src.AIRLINE
from
    PH_DATA.NORTHWOODS.AIRLINES tgt
    right join PH_DATA_RAW.NORTHWOODS_RAW_LAYER.AIRLINES src on src.IATA_CODE = tgt.IATA_CODE
where
    src.AIRLINE != tgt.AIRLINE;


-- Insert for PH_DATA.NORTHWOODS.AIRLINES

Insert into
    PH_DATA.NORTHWOODS.AIRLINES (IATA_CODE, AIRLINE)
select
    src.IATA_CODE,
    src.AIRLINE
from
    PH_DATA_RAW.NORTHWOODS_RAW_LAYER.AIRLINES src
where
    src.IATA_CODE not in (
        select
            tgt.IATA_CODE
        from
            PH_DATA.NORTHWOODS.AIRLINES tgt
    );

-- Update for PH_DATA.NORTHWOODS.AIRPORTS
Update
    PH_DATA.NORTHWOODS.AIRPORTS
set
    AIRPORT = src.AIRPORT,
    CITY = src.CITY,
    STATE = src.STATE,
    COUNTRY = src.COUNTRY,
    LATITUDE = try_to_decimal(src.LATITUDE, 38, 10),
    LONGITUDE = try_to_decimal(src.LONGITUDE, 38, 10)
from
    PH_DATA.NORTHWOODS.AIRPORTS tgt
    right join (
        select
            *,
            ROW_NUMBER() over(
                partition by IATA_CODE
                order by
                    AIRPORT,
                    CITY,
                    STATE,
                    COUNTRY
            ) as rnk
        from
            PH_DATA_RAW.NORTHWOODS_RAW_LAYER.AIRPORTS
    ) src on src.IATA_CODE = tgt.IATA_CODE
where
    src.rnk = 1
    and (
        src.AIRPORT != tgt.AIRPORT
        or src.CITY != tgt.CITY
        or src.STATE != tgt.STATE
        or src.COUNTRY != tgt.COUNTRY
        or src.AIRPORT != tgt.AIRPORT
        or try_to_decimal(src.LATITUDE, 38, 10) != tgt.LATITUDE
        or try_to_decimal(src.LONGITUDE, 38, 10) != tgt.LONGITUDE
    );

-- Insert for PH_DATA.NORTHWOODS.AIRPORTS
Insert into
    PH_DATA.NORTHWOODS.AIRPORTS (
        IATA_CODE,
        AIRPORT,
        CITY,
        STATE,
        COUNTRY,
        LATITUDE,
        LONGITUDE
    )
select
    unq.IATA_CODE,
    unq.AIRPORT,
    unq.CITY,
    unq.STATE,
    unq.COUNTRY,
    unq.LATITUDE,
    unq.LONGITUDE
from
    (
        select
            src.IATA_CODE,
            src.AIRPORT,
            src.CITY,
            src.STATE,
            src.COUNTRY,
            try_to_decimal(src.LATITUDE, 38, 10) LATITUDE,
            try_to_decimal(src.LONGITUDE, 38, 10) LONGITUDE,
            ROW_NUMBER() over(
                partition by src.IATA_CODE
                order by
                    src.AIRPORT,
                    src.CITY,
                    src.STATE,
                    src.COUNTRY
            ) as rnk
        from
            PH_DATA_RAW.NORTHWOODS_RAW_LAYER.AIRPORTS src
    ) unq
where
    unq.rnk = 1
    and unq.IATA_CODE not in (
        select
            tgt.IATA_CODE
        from
            PH_DATA.NORTHWOODS.AIRPORTS tgt
    )
    --and IATA_CODE in ('STT','YUM')
;

-- Update for PH_DATA.NORTHWOODS.FLIGHTS

Update
    PH_DATA.NORTHWOODS.FLIGHTS
set
    DIVERTED = try_to_number(src.DIVERTED, 38, 10),
    DAY_OF_WEEK = try_to_number(src.DAY_OF_WEEK, 38, 10),
    WEATHER_DELAY = try_to_number(src.WEATHER_DELAY, 38, 10),
    CANCELLED = try_to_number(src.CANCELLED, 38, 10),
    DEPARTURE_DELAY = try_to_number(src.DEPARTURE_DELAY, 38, 10),
    SCHEDULED_ARRIVAL = try_to_number(src.SCHEDULED_ARRIVAL, 38, 10),
    TAXI_OUT = try_to_number(src.TAXI_OUT, 38, 10),
    DESTINATION_AIRPORT = src.DESTINATION_AIRPORT,
    SCHEDULED_TIME = try_to_number(src.SCHEDULED_TIME, 38, 10),
    DISTANCE = try_to_number(src.DISTANCE, 38, 10),
    TAIL_NUMBER = src.TAIL_NUMBER,
    AIR_TIME = try_to_number(src.AIR_TIME, 38, 10),
    ARRIVAL_DELAY = src.ARRIVAL_DELAY,
    SCHEDULED_DEPARTURE = src.SCHEDULED_DEPARTURE,
    LATE_AIRCRAFT_DELAY = try_to_number(src.LATE_AIRCRAFT_DELAY, 38, 10),
    YEAR = try_to_number(src.YEAR, 38, 10),
    AIR_SYSTEM_DELAY = try_to_number(src.AIR_SYSTEM_DELAY, 38, 10),
    CANCELLATION_REASON = src.CANCELLATION_REASON,
    ARRIVAL_TIME = src.ARRIVAL_TIME,
    SECURITY_DELAY = try_to_number(src.SECURITY_DELAY, 38, 10),
    MONTH = try_to_number(src.MONTH, 38, 10),
    ORIGIN_AIRPORT = src.ORIGIN_AIRPORT,
    DEPARTURE_TIME = src.DEPARTURE_TIME,
    WHEELS_ON = try_to_number(src.WHEELS_ON, 38, 10),
    ELAPSED_TIME = try_to_number(src.ELAPSED_TIME, 38, 10),
    AIRLINE = src.AIRLINE,
    WHEELS_OFF = src.WHEELS_OFF,
    AIRLINE_DELAY = try_to_number(src.AIRLINE_DELAY, 38, 10),
    TAXI_IN = try_to_number(src.TAXI_IN, 38, 10),
    DAY = try_to_number(src.DAY, 38, 10),
    FLIGHT_NUMBER = src.FLIGHT_NUMBER
from
    PH_DATA.NORTHWOODS.FLIGHTS tgt
    right join PH_DATA_RAW.NORTHWOODS_RAW_LAYER.FLIGHTS src on src.YEAR = tgt.YEAR
    and src.MONTH = tgt.MONTH
    and src.DAY = tgt.DAY
    and src.FLIGHT_NUMBER = tgt.FLIGHT_NUMBER
    and src.AIRLINE = tgt.AIRLINE
    and src.ORIGIN_AIRPORT = tgt.ORIGIN_AIRPORT
    and src.DESTINATION_AIRPORT = tgt.DESTINATION_AIRPORT
where
    (
        tgt.AIR_TIME != try_to_number(src.AIR_TIME, 38, 10)
        or tgt.TAIL_NUMBER != src.TAIL_NUMBER
        or tgt.LATE_AIRCRAFT_DELAY != try_to_number(src.LATE_AIRCRAFT_DELAY, 38, 10)
        or tgt.SCHEDULED_DEPARTURE != src.SCHEDULED_DEPARTURE
        or tgt.ARRIVAL_DELAY != src.ARRIVAL_DELAY
        or tgt.AIR_SYSTEM_DELAY != try_to_number(src.AIR_SYSTEM_DELAY, 38, 10)
        or tgt.SCHEDULED_ARRIVAL != try_to_number(src.SCHEDULED_ARRIVAL, 38, 10)
        or tgt.WEATHER_DELAY != try_to_number(src.WEATHER_DELAY, 38, 10)
        or tgt.DAY_OF_WEEK != try_to_number(src.DAY_OF_WEEK, 38, 10)
        or tgt.CANCELLED != try_to_number(src.CANCELLED, 38, 10)
        or tgt.DEPARTURE_DELAY != try_to_number(src.DEPARTURE_DELAY, 38, 10)
        or tgt.DISTANCE != try_to_number(src.DISTANCE, 38, 10)
        or tgt.TAXI_OUT != try_to_number(src.TAXI_OUT, 38, 10)
        or tgt.SCHEDULED_TIME != try_to_number(src.SCHEDULED_TIME, 38, 10)
        or tgt.WHEELS_ON != try_to_number(src.WHEELS_ON, 38, 10)
        or tgt.DEPARTURE_TIME != src.DEPARTURE_TIME
        or tgt.WHEELS_OFF != src.WHEELS_OFF
        or tgt.AIRLINE_DELAY != try_to_number(src.AIRLINE_DELAY, 38, 10)
        or tgt.ELAPSED_TIME != try_to_number(src.ELAPSED_TIME, 38, 10)
        or tgt.TAXI_IN != try_to_number(src.TAXI_IN, 38, 10)
        or tgt.DIVERTED != try_to_number(src.DIVERTED, 38, 10)
        or tgt.SECURITY_DELAY != try_to_number(src.SECURITY_DELAY, 38, 10)
        or tgt.CANCELLATION_REASON != src.CANCELLATION_REASON
        or tgt.ARRIVAL_TIME != src.ARRIVAL_TIME
    );

-- Insert for PH_DATA.NORTHWOODS.FLIGHTS
Insert into
    PH_DATA.NORTHWOODS.FLIGHTS (
        AIRLINE,
        AIRLINE_DELAY,
        AIR_SYSTEM_DELAY,
        AIR_TIME,
        ARRIVAL_DELAY,
        ARRIVAL_TIME,
        CANCELLATION_REASON,
        CANCELLED,
        DAY,
        DAY_OF_WEEK,
        DEPARTURE_DELAY,
        DEPARTURE_TIME,
        DESTINATION_AIRPORT,
        DISTANCE,
        DIVERTED,
        ELAPSED_TIME,
        FLIGHT_NUMBER,
        ORIGIN_AIRPORT,
        LATE_AIRCRAFT_DELAY,
        MONTH,
        SCHEDULED_ARRIVAL,
        SCHEDULED_DEPARTURE,
        SCHEDULED_TIME,
        SECURITY_DELAY,
        TAIL_NUMBER,
        TAXI_IN,
        TAXI_OUT,
        WEATHER_DELAY,
        WHEELS_OFF,
        WHEELS_ON,
        YEAR
    )
select
    src.AIRLINE,
    try_to_number(src.AIRLINE_DELAY, 38, 10),
    try_to_number(src.AIR_SYSTEM_DELAY, 38, 10),
    try_to_number(src.AIR_TIME, 38, 10),
    src.ARRIVAL_DELAY,
    src.ARRIVAL_TIME,
    src.CANCELLATION_REASON,
    try_to_number(src.CANCELLED, 38, 10),
    try_to_number(src.DAY, 38, 10),
    try_to_number(src.DAY_OF_WEEK, 38, 10),
    try_to_number(src.DEPARTURE_DELAY, 38, 10),
    src.DEPARTURE_TIME,
    src.DESTINATION_AIRPORT,
    try_to_number(src.DISTANCE, 38, 10),
    try_to_number(src.DIVERTED, 38, 10),
    try_to_number(src.ELAPSED_TIME, 38, 10),
    src.FLIGHT_NUMBER,
    src.ORIGIN_AIRPORT,
    try_to_number(src.LATE_AIRCRAFT_DELAY, 38, 10),
    try_to_number(src.MONTH, 38, 10),
    try_to_number(src.SCHEDULED_ARRIVAL, 38, 10),
    src.SCHEDULED_DEPARTURE,
    try_to_number(src.SCHEDULED_TIME, 38, 10),
    try_to_number(src.SECURITY_DELAY, 38, 10),
    src.TAIL_NUMBER,
    try_to_number(src.TAXI_IN, 38, 10),
    try_to_number(src.TAXI_OUT, 38, 10),
    try_to_number(src.WEATHER_DELAY, 38, 10),
    src.WHEELS_OFF,
    try_to_number(src.WHEELS_ON, 38, 10),
    try_to_number(src.YEAR, 38, 10)
from
    PH_DATA_RAW.NORTHWOODS_RAW_LAYER.FLIGHTS src
    -- on src.YEAR=tgt.YEAR
    -- and src.MONTH=tgt.MONTH
    -- and src.DAY=tgt.DAY
    -- and src.FLIGHT_NUMBER=tgt.FLIGHT_NUMBER
    -- and src.AIRLINE=tgt.AIRLINE
    -- and src.ORIGIN_AIRPORT=tgt.ORIGIN_AIRPORT
    -- and src.DESTINATION_AIRPORT=tgt.DESTINATION_AIRPORT
where
    concat(
        src.YEAR,
        src.MONTH,
        src.DAY,
        src.FLIGHT_NUMBER,
        src.AIRLINE,
        src.ORIGIN_AIRPORT,
        src.DESTINATION_AIRPORT
    ) not in (
        select
            concat(
                tgt.YEAR,
                tgt.MONTH,
                tgt.DAY,
                tgt.FLIGHT_NUMBER,
                tgt.AIRLINE,
                tgt.ORIGIN_AIRPORT,
                tgt.DESTINATION_AIRPORT
            )
        from
            PH_DATA.NORTHWOODS.FLIGHTS tgt
    );