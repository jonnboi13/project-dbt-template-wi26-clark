select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

select *
from SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS
where OPEN is not null and OPEN <= 0


      
    ) dbt_internal_test