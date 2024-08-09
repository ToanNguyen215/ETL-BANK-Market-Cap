import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import sqlite3

#define information
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Banks.csv'
sql_connection = sqlite3.connect('./Banks.db')
text_path = "./code_log.txt"
exchange = r'C:\Users\Admin\Desktop\CV\final_project\exchange_rate.csv'

#extract the web page as text
def log_progress(message, text_path):
    time_stamp_format= '%Y-%h-%d--%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(time_stamp_format)
    with open (text_path, 'a') as f:
        f.write(timestamp + ':'+' '+ message + '\n')


def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')

    df = pd.DataFrame(columns=table_attribs)
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if 1:
                dir = {'Name':col[1].get_text(strip=True),
                       'MC_USD_Billion':col[2].get_text(strip=True)}
                df1 = pd.DataFrame(dir, index=[0])
                df = pd.concat([df1,df],ignore_index=True)           
    return df

def transform(df,exchange):
    exchange = pd.read_csv(exchange)
    USD = df['MC_USD_Billion'].tolist()

    # EUR
    RATE_EUR = exchange.iloc[0, 1]   
    EUR = [np.round(float(x) * float(RATE_EUR), 2) for x in USD]
    df['MC_EUR_Billion'] = EUR
       
    # GBP
    RATE_GBP = exchange.iloc[1, 1]   
    GBP = [np.round(float(x) * float(RATE_GBP),2) for x in USD]
    df['MC_GBP_Billion'] = GBP
   
    # INR
    RATE_INR = exchange.iloc[2, 1]   
    INR = [np.round(float(x) * float(RATE_INR),2) for x in USD]
    df['MC_INR_Billion'] = INR

    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index =True)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False )

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)




log_progress('Preliminaries complete. Initiating ETL process', text_path)
df = extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process', text_path)
df= transform(df,exchange)

log_progress('Data transformation complete. Initiating loading process', text_path)
load_to_csv(df, csv_path)

log_progress('Data saved to CSV file', text_path)
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query', text_path)
stt1=  "SELECT * FROM Largest_banks"
stt2=  "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
stt3=  "SELECT Name from Largest_banks LIMIT 5"
run_query(stt1, sql_connection)
run_query(stt2, sql_connection)
run_query(stt3, sql_connection)
log_progress('Process Complete', text_path)










