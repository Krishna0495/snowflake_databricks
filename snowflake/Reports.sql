use database PH_DATA
;
create or replace schema NORTHWOODS_REPORT
;
-- Total number of flights by airline and airport on a monthly basis 
-- On-time percentage of each airline for the year 2015
Create or replace view PH_DATA.NORTHWOODS_REPORT.MONTHLY_FLIGHTS as (

select
monthly.YEAR,
monthly.MONTH,
concat(YEAR,'-',MONTH) as YEAR_MONTH,
monthly.AIRLINE,
airlines.airline as airline_name,
monthly.ORIGIN_AIRPORT,
airports.airport as ORIGIN_AIRPORT_NAME,
airports.country,
airports.state,
airports.city,
monthly.total_number_of_flights,
monthly.on_time_flights,
monthly.on_time_percentage



from
(
select 
YEAR,
MONTH,
AIRLINE,
ORIGIN_AIRPORT,
count (distinct (FLIGHT_NUMBER)) as total_number_of_flights,
count (distinct ( case when DEPARTURE_DELAY=0 and ARRIVAL_DELAY=0
then FLIGHT_NUMBER else null end
)) as on_time_flights,
on_time_flights/total_number_of_flights*100 as on_time_percentage
from 
PH_DATA.NORTHWOODS.FLIGHTS
group by
AIRLINE,
ORIGIN_AIRPORT,
YEAR,
MONTH
order by 
YEAR,
MONTH,
AIRLINE,
ORIGIN_AIRPORT
) monthly
left join
PH_DATA.NORTHWOODS.AIRLINES airlines
on monthly.AIRLINE=airlines.IATA_CODE
left join
PH_DATA.NORTHWOODS.AIRPORTS airports
on monthly.ORIGIN_AIRPORT=airports.IATA_CODE
)
;

-- Airlines with the largest number of delays
Create or replace view PH_DATA.NORTHWOODS_REPORT.AIRLINES_DELAY as (

with cte
as
(
select 
AIRLINE,
COUNT(DISTINCT(FLIGHT_NUMBER)) delay_flight_count,
ROW_NUMBER() over(order by delay_flight_count desc) as airline_rank_by_delay
from
PH_DATA.NORTHWOODS.FLIGHTS
where 
--DEPARTURE_DELAY is null or ARRIVAL_DELAY is null
(
DEPARTURE_DELAY>0 or 
ARRIVAL_DELAY>0
)
group by
AIRLINE

)

select 
airlines.airline as airline_name,
cte.airline,
cte.delay_flight_count,
cte.airline_rank_by_delay
from
cte
left join
PH_DATA.NORTHWOODS.AIRLINES airlines
on cte.AIRLINE=airlines.IATA_CODE

);

-- Cancellation reasons by airport

Create or replace view PH_DATA.NORTHWOODS_REPORT.AIRLINES_CANCEL_REASON as (
select

cancel.year,
cancel.month,
cancel.day,

cancel.flight_number,

cancel.airline,
airlines.airline as airline_name,

cancel.origin_airport,
airports_org.airport as ORIGIN_AIRPORT_NAME,
airports_org.country as ORIGIN_AIRPORT_COUNTRY,
airports_org.state as ORIGIN_AIRPORT_STATE,
airports_org.city as ORIGIN_AIRPORT_CITY,

cancel.destination_airport,
airports_dest.airport as DESTINATION_AIRPORT_NAME,
airports_dest.country as DESTINATION_AIRPORT_COUNTRY,
airports_dest.state AS DESTINATION_AIRPORT_STATE,
airports_dest.city AS DESTINATION_AIRPORT_CITY,
CANCEL_REASON_CODE.CANCEL_CODE,
CANCEL_REASON_CODE.CANCELLATION_REASON
from
(
select 
-- AIRLINE,
-- COUNT(DISTINCT(FLIGHT_NUMBER)) delay_flight_count,
-- ROW_NUMBER() over(order by delay_flight_count desc) as airline_rank_by_delay
year,
month,
day,
airline,
origin_airport,
destination_airport,
flight_number,
cancellation_reason

from
PH_DATA.NORTHWOODS.FLIGHTS
where 
cancelled=1
) cancel
left join
PH_DATA.NORTHWOODS.CANCEL_REASON_CODE
on cancel.cancellation_reason=CANCEL_REASON_CODE.CANCEL_CODE
left join
PH_DATA.NORTHWOODS.AIRLINES airlines
on cancel.AIRLINE=airlines.IATA_CODE
left join
PH_DATA.NORTHWOODS.AIRPORTS airports_org
on cancel.ORIGIN_AIRPORT=airports_org.IATA_CODE
left join
PH_DATA.NORTHWOODS.AIRPORTS airports_dest
on cancel.destination_airport=airports_dest.IATA_CODE
)
;

-- Delay reasons by airport
Create or replace view PH_DATA.NORTHWOODS_REPORT.AIRLINES_DELAYL_REASON as (

select

delay.year,
delay.month,
delay.day,

delay.flight_number,

delay.airline,
airlines.airline as airline_name,

delay.origin_airport,
airports_org.airport as ORIGIN_AIRPORT_NAME,
airports_org.country as ORIGIN_AIRPORT_COUNTRY,
airports_org.state as ORIGIN_AIRPORT_STATE,
airports_org.city as ORIGIN_AIRPORT_CITY,

delay.destination_airport,
airports_dest.airport as DESTINATION_AIRPORT_NAME,
airports_dest.country as DESTINATION_AIRPORT_COUNTRY,
airports_dest.state AS DESTINATION_AIRPORT_STATE,
airports_dest.city AS DESTINATION_AIRPORT_CITY,
delay.delay_reasons,
delay.total_delay,
delay.delay_breakup
from
(
select
year,
month,
day,
airline,
origin_airport,
destination_airport,
flight_number,
listagg(
distinct(
case when col_val>0 then col_name
else null end
)
,' ,') as delay_reasons,
sum(col_val) as total_delay,
listagg(
concat('{"',col_name,'":',col_val,'}'
),','
) as delay_breakup

from
(
select *
from (
    select
    
        *
    from
PH_DATA.NORTHWOODS.FLIGHTS
where 
air_system_delay>0
or 
security_delay>0
or
airline_delay>0
or
late_aircraft_delay>0
or
weather_delay>0
) unpivot (col_val for col_name in (air_system_delay,security_delay, airline_delay, late_aircraft_delay, weather_delay))

)
group
by
year,
month,
day,
airline,
origin_airport,
destination_airport,
flight_number
) delay
left join
PH_DATA.NORTHWOODS.AIRLINES airlines
on delay.AIRLINE=airlines.IATA_CODE
left join
PH_DATA.NORTHWOODS.AIRPORTS airports_org
on delay.ORIGIN_AIRPORT=airports_org.IATA_CODE
left join
PH_DATA.NORTHWOODS.AIRPORTS airports_dest
on delay.destination_airport=airports_dest.IATA_CODE

)
;

Create or replace view PH_DATA.NORTHWOODS_REPORT.AIRLINES_MOST_UNQ_ROUTES as (

select
f.*,
airlines.airline
from
(
select
unq_route.AIRLINE,
count(distinct(concat(ORIGIN_AIRPORT,DESTINATION_AIRPORT))) as unique_routes,
ROW_NUMBER() over(order by unique_routes desc) as rnk
from
(
SELECT AIRLINE,ORIGIN_AIRPORT, DESTINATION_AIRPORT 
FROM PH_DATA.NORTHWOODS.FLIGHTS
UNION -- Will remove duplicates
SELECT AIRLINE,DESTINATION_AIRPORT, ORIGIN_AIRPORT
FROM PH_DATA.NORTHWOODS.FLIGHTS
) unq_route
group by
unq_route.AIRLINE
) f

left join
PH_DATA.NORTHWOODS.AIRLINES airlines
on f.AIRLINE=airlines.IATA_CODE
order by
rnk desc


)
;