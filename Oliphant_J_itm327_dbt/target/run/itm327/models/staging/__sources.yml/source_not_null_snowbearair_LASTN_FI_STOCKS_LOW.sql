select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select LOW
from SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS
where LOW is null



      
    ) dbt_internal_test