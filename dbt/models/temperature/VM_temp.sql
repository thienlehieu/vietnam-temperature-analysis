{{ config(materialized='table') }}

with station_info as (
    select * 
    from {{ source('noaa-ghcnd-raw', 'stations') }}
),
VM_raw as (
    select *
    from {{ source('noaa-ghcnd-raw', 'VM_raw') }}
    where Element = 'TAVG'
)

select
    VM_raw.StationId as Station_id,
    {{ bigquery_to_date('VM_raw.DateStr') }} as Created_date,
    VM_raw.Element,
    VM_raw.Value,
    VM_raw.M_Flag,
    VM_raw.Q_Flag,
    VM_raw.S_Flag,
    VM_raw.Obs_Time,
    cast(station_info.lat as numeric) as Lat,
    cast(station_info.long as numeric) as Long,
    station_info.name as City
from VM_raw 
inner join station_info 
on VM_raw.StationId = station_info.stationId