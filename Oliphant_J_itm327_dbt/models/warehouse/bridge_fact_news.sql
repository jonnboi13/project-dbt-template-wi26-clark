SELECT DISTINCT
    f.SYMBOL,
    f.CALENDAR_DATE,
    n.NEWS_ID
FROM {{ ref('fact_ml_analysis') }} f
JOIN {{ source('raw', 'NEWS_API_DAG_OLIPHANT_J') }} r
    ON r.RELATED = f.SYMBOL
JOIN {{ ref('dim_news') }} n
    ON n.NEWS_ID = r.ID
