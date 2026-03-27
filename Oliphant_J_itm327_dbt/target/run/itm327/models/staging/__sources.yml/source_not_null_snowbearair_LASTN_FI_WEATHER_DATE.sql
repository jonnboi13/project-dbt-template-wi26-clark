select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select DATE
from SNOWBEARAIR_DB.RAW.LASTN_FI_WEATHER
where DATE is null



      
    ) dbt_internal_test