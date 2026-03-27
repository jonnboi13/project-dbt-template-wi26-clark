
  
    

        create or replace transient table SNOWBEARAIR_DB.RAW.bridge_fact_news
         as
        (SELECT DISTINCT
    f.SYMBOL,
    f.CALENDAR_DATE,
    n.NEWS_ID
FROM SNOWBEARAIR_DB.RAW.fact_ml_analysis f
JOIN SNOWBEARAIR_DB.RAW.NEWS_API_DAG_OLIPHANT_J r
    ON r.RELATED = f.SYMBOL
JOIN SNOWBEARAIR_DB.RAW.dim_news n
    ON n.NEWS_ID = r.ID
        );
      
  