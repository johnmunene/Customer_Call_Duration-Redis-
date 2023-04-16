#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import prerequisite module

import pandas as pd
import psycopg2
import redis

import sqlite3
import wget
import zipfile
from io import BytesIO
# Redis Cloud Instance Information
redis_host = 'localhost'
redis_port = 6379
redis_password = 'Nevergiveup.1'



# Postgres Database Information
pg_host = 'localhost'
pg_database = 'Redis_db'
pg_user = 'postgres'
pg_password = 'Nevergiveup.1'

# Redis Client Object

redis_cache = redis.Redis(host="localhost", port=6379, password ="Nevergiveup.1")

#extract function
def extract_data(file_path):
    wget.download(file_path)
    unzipcsvfile = zipfile.ZipFile('./redis_project.zip')

    
    
    df = pd.read_csv(unzipcsvfile.open('customer_call_logs.csv'))
       
    return df

#get function

def get_data():
    """
    Get data from Redis cache if available, else extract data from Postgres database.
    """
    #cache date into Redis
    
    data = redis_cache.get('customer_data')
    if data is None:
        data = extract_data()
        redis_cache.set('customer_data', data)
    return data

#transforming data

def transform_data(data):
    # Retrieve data from Redis cache
    new = redis_cache.get(data)
    dt = pd.read_json(BytesIO(new))
    
    # Transform data (clean, structure, format)
    # remove the dollar sign that is repetitive
    dt['call_cost'] = dt['call_cost'].str.replace('$', '')
    dt2 = dt
    # rename column call_cost to call_cost_usd

    dt2.rename(columns = {'call_cost':'call_cost_usd'}, inplace = True)
    
    #convert the call duration to minutes(decimal)
    
    dt2[['h', 'm' , 's']] = dt2["call_duration"].apply(lambda x: pd.Series(str(x).split(":")))
    dt2 = dt2.astype({'h':'float','m':'float', 's':'float'})
    dt2['call_duration_min'] = dt2['h']*60 + dt2['m'] + dt2['s']/60
    
    #output transformed data and dropping irrelevant columns
    transformed_data = dt2.drop(['h', 's', 'm', 'call_duration'], axis=1)

    
    
    

    return transformed_data

#loading data to postgres database
def load_data(transformed_data):
    # Connect to Postgres database
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)

    # Create a cursor object
    cur = conn.cursor()

    # Create a table to store the data
    cur.execute('CREATE TABLE IF NOT EXISTS customer_call_logs (\
                 customer_id INT,\
                 call_cost_usd FLOAT,\
                 call_destination VARCHAR,\
                 call_date TIMESTAMP,\
                 call_duration_min FLOAT\
                 )')

    # Insert the transformed data into the database
    for i, row in transformed_data.iterrows():
        cur.execute(f"INSERT INTO customer_call_logs (customer_id, call_cost_usd, call_destination, call_date, call_duration_min) VALUES ({row['customer_id']}, {row['call_cost_usd']}, '{row['call_destination']}', '{row['call_date']}', {row['call_duration_min']})")

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

#creating pipeline
def data_pipeline():
    # Data pipeline function
    extract_data('https://archive.org/download/redis_project/redis_project.zip')
    get_data()
    transformed_data = transform_data('customer_data')
    load_data(transformed_data)

#running pipeline function
if __name__ == '__main__':
    # Run the data pipeline function
    data_pipeline()


# In[ ]:




