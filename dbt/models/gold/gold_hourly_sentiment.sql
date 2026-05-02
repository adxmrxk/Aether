{{
    config(
        materialized='table',
        partition_by={
            'field': 'hour_timestamp',
            'data_type': 'timestamp',
            'granularity': 'hour'
        },
        cluster_by=['symbol'],
        description='Gold layer: Hourly aggregated sentiment metrics by cryptocurrency'
    )
}}

/*
    Gold Model: gold_hourly_sentiment

    Aggregates market data and sentiment scores by hour and symbol.
    This is the final analytical layer consumed by the API.

    Metrics:
    - Average sentiment score
    - Min/Max sentiment range
    - Average price and volume
    - Record count for data quality
*/

with staged_data as (
    select * from {{ ref('stg_market_data') }}
    where sentiment_score is not null
),

hourly_aggregates as (
    select
        -- Time dimension
        timestamp_trunc(ingested_at, hour) as hour_timestamp,

        -- Symbol dimension
        upper(symbol) as symbol,

        -- Sentiment metrics
        round(avg(sentiment_score), 2) as avg_sentiment_score,
        round(min(sentiment_score), 2) as min_sentiment_score,
        round(max(sentiment_score), 2) as max_sentiment_score,
        round(stddev(sentiment_score), 2) as sentiment_stddev,

        -- Sentiment classification
        case
            when avg(sentiment_score) >= 7 then 'BULLISH'
            when avg(sentiment_score) >= 4 then 'NEUTRAL'
            else 'BEARISH'
        end as sentiment_category,

        -- Price metrics
        round(avg(price_usd), 2) as avg_price_usd,
        round(min(price_usd), 2) as min_price_usd,
        round(max(price_usd), 2) as max_price_usd,

        -- Volume metrics
        round(avg(volume_24h), 0) as avg_volume_24h,
        round(sum(volume_24h), 0) as total_volume_24h,

        -- Change metrics
        round(avg(percent_change_24h), 2) as avg_percent_change_24h,

        -- Data quality metrics
        count(*) as record_count,
        count(distinct source) as source_count,

        -- Latest reasoning sample (for context)
        array_agg(sentiment_reasoning order by ingested_at desc limit 1)[offset(0)] as latest_reasoning,

        -- Metadata
        current_timestamp() as aggregated_at

    from staged_data
    group by 1, 2
)

select * from hourly_aggregates
order by hour_timestamp desc, symbol
