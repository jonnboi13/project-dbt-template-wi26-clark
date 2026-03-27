
  
    

        create or replace transient table SNOWBEARAIR_DB.RAW.dim_weather_location
         as
        (SELECT DISTINCT
    CITY
FROM SNOWBEARAIR_DB.RAW.WEATHER_API_DAG_OLIPHANT_J
        );
      
  