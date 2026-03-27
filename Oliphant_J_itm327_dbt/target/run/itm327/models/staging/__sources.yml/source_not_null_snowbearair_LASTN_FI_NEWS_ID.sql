select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ID
from SNOWBEARAIR_DB.RAW.LASTN_FI_NEWS
where ID is null



      
    ) dbt_internal_test