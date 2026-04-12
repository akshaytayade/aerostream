{{ config(materialized='view') }}

select
    country,
    count(*) as total_signals,
    count(distinct icao24) as distinct_aircraft
from read_parquet('s3://silver/flight_states/**/*.parquet')
group by 1
order by total_signals desc
limit 10