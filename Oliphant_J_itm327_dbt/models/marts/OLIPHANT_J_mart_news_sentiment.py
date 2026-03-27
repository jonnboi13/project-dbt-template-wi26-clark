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
