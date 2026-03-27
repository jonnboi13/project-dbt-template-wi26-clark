
  
    

        create or replace transient table SNOWBEARAIR_DB.RAW.dim_news
         as
        (SELECT DISTINCT
    ID AS NEWS_ID,
    HEADLINE,
    SUMMARY,
    SOURCE,
    CATEGORY,
    URL,
    IMAGE
FROM SNOWBEARAIR_DB.RAW.NEWS_API_DAG_OLIPHANT_J
WHERE ID IS NOT NULL
        );
      
  