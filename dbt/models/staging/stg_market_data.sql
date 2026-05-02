{{
    config(
        materialized='view',
        description='Staged market data - cleaned and typed from bronze layer'
    )
}}

/*
    Staging Model: stg_market_data

    Transforms raw JSON payloads from bronze_raw_data into typed columns.
    This is the Silver layer preparation step.
*/

with source as (
    select
        id,
        raw_payload,
        source,
        ingested_at,
        sentiment_score,
        sentiment_reasoning,
        processing_metadata
    from {{ source('aether', 'bronze_raw_data') }}
    where ingested_at >= timestamp_sub(current_timestamp(), interval {{ var('lookback_hours') }} hour)
),

parsed as (
    select
        id,

        -- Extract fields from JSON payload
        json_extract_scalar(raw_payload, '$.symbol') as symbol,
        safe_cast(json_extract_scalar(raw_payload, '$.price_usd') as float64) as price_usd,
        safe_cast(json_extract_scalar(raw_payload, '$.volume_24h') as float64) as volume_24h,
        safe_cast(json_extract_scalar(raw_payload, '$.market_cap') as float64) as market_cap,
        safe_cast(json_extract_scalar(raw_payload, '$.percent_change_24h') as float64) as percent_change_24h,
        json_extract_scalar(raw_payload, '$.news_headline') as news_headline,

        -- AI sentiment fields
        sentiment_score,
        sentiment_reasoning,

        -- Metadata
        source,
        ingested_at,
        current_timestamp() as processed_at,

        -- Processing metadata extraction
        safe_cast(json_extract_scalar(processing_metadata, '$.function_version') as string) as function_version

    from source
)

select * from parsed
where symbol is not null  -- Filter out malformed records
