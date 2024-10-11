"""
Scrape top films from the Wikipedia page - using BeautifulSoup
"""

# Libraries
from bs4 import BeautifulSoup

import requests
import sqlite3
import pandas as pd

## Set all the default variables
url = 'https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = './db_result/Top_Films.db'
table_name = 'Top_Films'
csv_path = './csv_result/Top_Films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

## Access the required web page using BeautifulSoup
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

# Extract data and save it to a Pandas dataframe
for row in rows:
    if count < 50:
        col = row.find_all('td')
        if len(col) != 0 and col[2].contents[0].isdigit() and int(col[2].contents[0]) >= 1990: # Check if the year is a digit and >= 1990
            data_dict = {
                "Average Rank": int(col[0].contents[0]),
                "Film": col[1].text.strip(),  # Extract text content and strip any extra whitespace
                "Year": int(col[2].contents[0])
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break


print(df)

# Store the data in a CSV file and a SQLite database
df.to_csv(csv_path)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
