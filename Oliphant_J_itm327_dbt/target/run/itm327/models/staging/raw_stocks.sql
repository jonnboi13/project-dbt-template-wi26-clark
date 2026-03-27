
  create or replace   view SNOWBEARAIR_DB.RAW.raw_stocks
  
   as (
    -- TODO: Update the source table name to match your prefix (e.g., SMITHJ_STOCKS)
select *
from SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS
  );

