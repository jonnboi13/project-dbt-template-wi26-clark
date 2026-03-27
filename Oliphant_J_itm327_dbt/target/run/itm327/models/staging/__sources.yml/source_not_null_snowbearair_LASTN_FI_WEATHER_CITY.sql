select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select CITY
from SNOWBEARAIR_DB.RAW.LASTN_FI_WEATHER
where CITY is null



      
    ) dbt_internal_test