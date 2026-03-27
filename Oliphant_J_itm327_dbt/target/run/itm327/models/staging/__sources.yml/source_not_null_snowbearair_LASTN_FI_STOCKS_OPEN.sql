select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select OPEN
from SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS
where OPEN is null



      
    ) dbt_internal_test