select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select CLOSE
from SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS
where CLOSE is null



      
    ) dbt_internal_test