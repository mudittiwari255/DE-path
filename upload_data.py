#!/usr/bin/env python
# coding: utf-8


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(url, iterator=True, chunksize=100000)
    df = next(df_iter)
    # df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    header = df.head(n = 0)
    header.to_sql(name = table_name, con = engine, if_exists='replace')
    df.to_sql(name = table_name, con = engine, if_exists='append')
    

    while True:
        start = time()
        df = next(df_iter)
        # df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df.to_sql(name = table_name, con = engine, if_exists='append')
        end = time()
        print(f"inserted another chunk .. , took {end-start} seconds")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Data Injestion")

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='user name for postgres')
    parser.add_argument('--port', help='user name for postgres')
    parser.add_argument('--db', help='db name')
    parser.add_argument('--table_name', help='table name')
    parser.add_argument('--url', help='url of the csv')

    args = parser.parse_args()

    main(args)