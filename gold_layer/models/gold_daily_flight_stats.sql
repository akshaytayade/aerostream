{{ config(materialized='table') }}

select
    date_trunc('day', to_timestamp(last_contact)) as flight_date,
    count(*) as total_flights,
    count(distinct icao24) as unique_aircraft,
    avg(altitude) as avg_altitude,
    avg(velocity) as avg_velocity
from read_parquet('s3://silver/flight_states/**/*.parquet')
where altitude is not null
group by 1
order by 1 desc
