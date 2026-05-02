{{
    config(
        materialized='view',
        description='Gold layer: Latest sentiment for each cryptocurrency (real-time view)'
    )
}}

/*
    Gold Model: gold_latest_sentiment

    Provides the most recent sentiment data for each cryptocurrency.
    Optimized for API consumption with minimal latency.
*/

with ranked_data as (
    select
        *,
        row_number() over (
            partition by symbol
            order by hour_timestamp desc
        ) as rn
    from {{ ref('gold_hourly_sentiment') }}
),

latest as (
    select
        symbol,
        hour_timestamp as last_updated,
        avg_sentiment_score,
        sentiment_category,
        avg_price_usd,
        avg_percent_change_24h,
        avg_volume_24h,
        record_count,
        latest_reasoning,

        -- Calculate trend (compare to previous hour)
        lag(avg_sentiment_score) over (
            partition by symbol
            order by hour_timestamp
        ) as prev_hour_sentiment,

        case
            when avg_sentiment_score > lag(avg_sentiment_score) over (
                partition by symbol order by hour_timestamp
            ) then 'IMPROVING'
            when avg_sentiment_score < lag(avg_sentiment_score) over (
                partition by symbol order by hour_timestamp
            ) then 'DECLINING'
            else 'STABLE'
        end as sentiment_trend

    from ranked_data
    where rn = 1
)

select
    symbol,
    last_updated,
    avg_sentiment_score as sentiment_score,
    sentiment_category,
    sentiment_trend,
    avg_price_usd as price_usd,
    avg_percent_change_24h as percent_change_24h,
    avg_volume_24h as volume_24h,
    record_count as data_points,
    latest_reasoning as ai_reasoning
from latest
order by sentiment_score desc
