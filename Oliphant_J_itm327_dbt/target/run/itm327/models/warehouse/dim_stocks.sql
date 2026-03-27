
  
    

        create or replace transient table SNOWBEARAIR_DB.RAW.dim_stocks
         as
        (SELECT DISTINCT
    SYMBOL,
    MIN(DATETIME) AS START_DATE,
    TRUE AS IS_CURRENT
FROM SNOWBEARAIR_DB.RAW.MONGO_DAG_OLIPHANT_J
GROUP BY SYMBOL
        );
      
  