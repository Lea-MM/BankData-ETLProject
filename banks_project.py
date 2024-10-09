## Code for ETL operations on Country-GDP data
import requests 
import pandas 
import numpy 
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

## TASK 1: LOG ENTRIES
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the code execution to a log file.
        Funtion returns nothing. '''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    
    log_file = 'code_log.txt'
    with open(log_file, "a") as file:
        output = timestamp + " : " + message + "\n"
        file.write(output)
        print(output)
    
## TASK 2: EXTRACT INFO
def extract(url, attributes):
    ''' This function aims to extract the required information fromt the website and save it to a data frame.
        The function returns the data frame for further processing. '''
    
    web_page_txt = requests.get(url).text
    parsed_web_page = BeautifulSoup(web_page_txt, "html.parser")
    tables = parsed_web_page.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pandas.DataFrame(columns=attributes)
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 and col[1].find('a') is not None:
            data_dict = {
                'Name': (col[1].text).split('\n')[0],
                'MC_USD_Billion': float((col[2].text).split('\n')[0])
            }
            cur_df = pandas.DataFrame(data_dict, index=[0])
            df = pandas.concat([df, cur_df], ignore_index=True)
    
    print(df)
    return df

## TASK 3: TRANSFORM DATAFRAME
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, 
        each containing the transformed version of Market Cap column to respective currencies. '''
    
    exchange_rate_dict = {}
    with open(exchange_rate_file, 'r') as file:
        file.readline()
        line = file.readline()        
        while line:
            contents = line.split(',')
            exchange_rate_dict[contents[0]] = float(contents[1].split('\n')[0])
            line = file.readline()           
        # print(exchange_rate_dict)
    
    df['MC_GBP_Billion'] = [numpy.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [numpy.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [numpy.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    print(df)
    return df

## TASK 4: LOAD TO CSV AND SQL
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in the provided path.
        Function returns nothing. '''
    
    df.to_csv(output_path)
    
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database table with the provided name.
        Function returns nothing. '''
        
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

## TASK 5: RUN SQL QUERIES
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and prints the output on the terminal.
        Function returns nothing. '''
        
    query_output = pandas.read_sql(query_statement, sql_connection)
    print(query_output)
        

# Declaring known values
data_url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes = ['Name', 'MC_USD_Billion']
exchange_rate_file = 'exchange_rate.csv'
output_csv_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'


## TASK 6: VERIFY LOGS
# declaring known values
log_progress('Preliminaries complete. Initiating ETL process')

# call extract() function
df = extract(data_url, table_attributes)
log_progress('Data extraction complete. Initiating Transformation process')

# call transform() function
df = transform(df, exchange_rate_file)
log_progress('Data transformation complete. Initiating Loading process')

# call load_to_csv()
load_to_csv(df, output_csv_path)
log_progress('Data saved to CSV file')

# initiating SQLite3 connection
sql_connection = sqlite3.connect(db_name)
log_progress('SQL connection initiated')

# # call load_to_db()
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

# call run_query()
query_statement1 = 'SELECT * FROM Largest_banks' # print the contents of the entire table
query_statement2 = 'SELECT Name, AVG(MC_USD_Billion) FROM Largest_banks' # print the average market capitalization of all the banks in Billion USD
query_statement3 = 'SELECT Name FROM Largest_banks LIMIT 5' # print only the names of the top 5 banks 
run_query(query_statement1, sql_connection)
run_query(query_statement2, sql_connection)
run_query(query_statement3, sql_connection)
log_progress('Process Complete')

# close SQLite3 connection
log_progress('Server Connection closed')