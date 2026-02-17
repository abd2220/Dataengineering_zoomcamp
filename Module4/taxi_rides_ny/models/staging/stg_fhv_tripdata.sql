with source as (
    -- Assuming the table follows a similar naming convention in your dataset
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        
        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(sr_flag as string) as sr_flag,
        cast(affiliated_base_number as string) as affiliated_base_number

    from source
    -- Data quality: Filter out records where dispatching_base_num is null 
    -- (Equivalent to your vendorid check)
    where dispatching_base_num is not null
)

select * from renamed