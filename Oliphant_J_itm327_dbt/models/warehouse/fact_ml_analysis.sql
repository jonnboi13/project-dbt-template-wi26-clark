SELECT
    d.CALENDAR_DATE,
    s.SYMBOL,
    w.CITY,
    r.OPEN,
    r.HIGH,
    r.LOW,
    r.CLOSE,
    r.VOLUME,
    rw.MAX_TEMP,
    rw.PRECIP AS PRECIPITATION,
    (
        SELECT COUNT(*) 
        FROM {{ source('raw', 'NEWS_API_DAG_OLIPHANT_J') }} n
        WHERE CAST(n.DATETIME AS DATE) = CAST(r.DATETIME AS DATE)
    ) AS NEWS_COUNT_TOTAL
FROM {{ source('raw', 'MONGO_DAG_OLIPHANT_J') }} r
JOIN {{ ref('dim_date') }} d 
    ON CAST(r.DATETIME AS DATE) = d.CALENDAR_DATE
JOIN {{ ref('dim_stocks') }} s 
    ON r.SYMBOL = s.SYMBOL
LEFT JOIN {{ source('raw', 'WEATHER_API_DAG_OLIPHANT_J') }} rw 
    ON CAST(r.DATETIME AS DATE) = rw.DATE
LEFT JOIN {{ ref('dim_weather_location') }} w 
    ON rw.CITY = w.CITY