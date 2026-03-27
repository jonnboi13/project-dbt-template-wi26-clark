select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select TRADE_DATE
from SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS
where TRADE_DATE is null



      
    ) dbt_internal_test