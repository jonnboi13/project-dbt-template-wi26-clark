
  
    
    
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table")

    fact = dbt.ref("fact_ml_analysis").to_pandas()
    news = dbt.ref("dim_news").to_pandas()
    bridge = dbt.ref("bridge_fact_news").to_pandas()

    # Join bridge to news
    news_per_stock = bridge.merge(news, on="NEWS_ID")

    # Aggregate to weekly grain — different from SQL mart which is daily
    fact["CALENDAR_DATE"] = pd.to_datetime(fact["CALENDAR_DATE"])
    fact["WEEK"] = fact["CALENDAR_DATE"].dt.isocalendar().week
    fact["YEAR"] = fact["CALENDAR_DATE"].dt.year

    weekly = fact.groupby(["SYMBOL", "YEAR", "WEEK", "CITY"]).agg(
        AVG_CLOSE=("CLOSE", "mean"),
        AVG_VOLUME=("VOLUME", "mean"),
        AVG_TEMP=("MAX_TEMP", "mean"),
        TOTAL_NEWS=("NEWS_COUNT_TOTAL", "sum"),
        WEEK_PRICE_CHANGE=("CLOSE", lambda x: x.iloc[-1] - x.iloc[0])
    ).reset_index()

    return weekly


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"bridge_fact_news": "SNOWBEARAIR_DB.RAW.bridge_fact_news", "dim_news": "SNOWBEARAIR_DB.RAW.dim_news", "fact_ml_analysis": "SNOWBEARAIR_DB.RAW.fact_ml_analysis"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "SNOWBEARAIR_DB"
    schema = "RAW"
    identifier = "mart_news_sentiment"
    
    def __repr__(self):
        return 'SNOWBEARAIR_DB.RAW.mart_news_sentiment'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



def materialize(session, df, target_relation):
    # make sure pandas exists
    import importlib.util
    package_name = 'pandas'
    if importlib.util.find_spec(package_name):
        import pandas
        if isinstance(df, pandas.core.frame.DataFrame):
          session.use_database(target_relation.database)
          session.use_schema(target_relation.schema)
          # session.write_pandas does not have overwrite function
          df = session.createDataFrame(df)
    
    df.write.mode("overwrite").save_as_table('SNOWBEARAIR_DB.RAW.mart_news_sentiment', table_type='transient')

def main(session):
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"

  