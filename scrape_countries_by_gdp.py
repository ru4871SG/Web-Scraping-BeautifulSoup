"""
Scrape the GDP of countries from the Wikipedia page - using BeautifulSoup
"""

# Importing the required libraries
from bs4 import BeautifulSoup
from datetime import datetime

import pandas as pd
import requests
import re
import sqlite3

## Set all the default variables
url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_name = 'Countries_by_GDP'
csv_path = './csv_result/Countries_by_GDP.csv'
table_attribs = ["Country", "GDP_USD_millions"]
sql_connection = sqlite3.connect('./db_result/Countries_by_GDP.db')


# Extract
def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    # access the required web page using BeautifulSoup
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                country = col[0].a.get_text(strip=True)
                gdp_usd_millions = col[1].get_text(strip=True)
                data_dict = {"Country": country, "GDP_USD_millions": gdp_usd_millions}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df


# Transform
def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, and transforms the information of GDP from
    USD (Millions) to USD (Billions).
    The function returns the transformed dataframe.'''
    # Remove non-numeric characters
    df['GDP_USD_millions'] = df['GDP_USD_millions'].apply(lambda x: re.sub(r'[^\d.]', '', x))
    df['GDP_USD_millions'] = df['GDP_USD_millions'].replace('', '0').astype(float)
    df['GDP_USD_billions'] = (df['GDP_USD_millions'] / 1000).round(2)

    return df


# Load to CSV
def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


# Load to DB using SQLite3
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)


# Test query with run_query function
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# Logging
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./countries_log.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


## Execute and log the steps in one section
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete!')

sql_connection.close()
log_progress('Server Connection closed')
