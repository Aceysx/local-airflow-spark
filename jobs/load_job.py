import pandas as pd
from sqlalchemy import create_engine


def load_data_to_rdb(db_url):
    daily_data = pd.read_csv('/opt/data/transformed/drive_data_daily_summary.csv')
    annual_data = pd.read_csv('/opt/data/transformed/drive_data_annual_summary.csv')

    engine = create_engine(db_url)

    daily_data.to_sql('drive_data_daily_summary', engine, if_exists='replace', index=False)
    annual_data.to_sql('drive_data_annual_summary', engine, if_exists='replace', index=False)
