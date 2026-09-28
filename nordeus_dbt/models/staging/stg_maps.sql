{{ config(materialized='view') }}

select
    map_id,
    map_name

from read_parquet('../data/silver/maps/*.parquet')