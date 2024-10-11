"""
Scrape the largest banks by assets from the Wikipedia page - using BeautifulSoup
"""

# Importing the required libraries
from bs4 import BeautifulSoup
from datetime import datetime

import requests
import pandas as pd
import sqlite3

## Set all the default variables
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Bank_name", "MC_USD_Billion"]
output_path = './csv_result/Banks_by_Asset.csv'
table_name = 'Largest_banks'
sql_connection = sqlite3.connect('./db_result/Banks_by_Asset.db')


# Logging
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    time_stamp = now.strftime(time_stamp_format)
    with open("./banks_log.txt","a") as f:
        f.write(time_stamp + ':' + message + '\n')


# Check index from the URL that we are about to scrap using BeautifulSoup - in case we need to check it
def check_index(url):
    ''' This function aims to check the index of the table
    '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    for index, table in enumerate(tables):
        print(f"Table {index} content:")
        print(table)
        print("-" * 80)  # Just a separator for clarity. 


# Extract
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    table_data = []

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows[1:]:  # skipping the header row
        col = row.find_all('td')
        if len(col) > 1:
            row_data = {
                table_attribs[0]: col[1].get_text(strip=True),
                table_attribs[1]: col[2].get_text(strip=True)
            }
            table_data.append(row_data)

    df = pd.DataFrame(table_data)
    return df


# Transform
def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    #need to convert the data type first
    df['MC_USD_Billion'] = df['MC_USD_Billion'].replace(',', '', regex=True).astype(float)

    return df


# Load to CSV
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


# Load to DB using SQLite3
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)


# Try SQL Queries from the DB
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


## Execute and log the steps in one section
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

log_progress('SQL Connection initiated')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)
log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')
