/*
    Custom Test: Ensure sentiment scores are always between 1 and 10

    This test validates data quality by checking that no sentiment scores
    fall outside the valid range. This is critical because:
    1. AI models can occasionally produce outliers
    2. JSON parsing errors could result in invalid values
    3. The API depends on consistent scoring for trend analysis
*/

select
    id,
    symbol,
    sentiment_score,
    ingested_at
from {{ ref('stg_market_data') }}
where sentiment_score is not null
  and (sentiment_score < 1 or sentiment_score > 10)
