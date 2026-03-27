select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select HEADLINE
from SNOWBEARAIR_DB.RAW.LASTN_FI_NEWS
where HEADLINE is null



      
    ) dbt_internal_test