# Code for ETL operations on Country-GDP data

# Importing the required libraries
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import requests 
import re 



def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    # Getting the current date and time
    dt = datetime.now()
    # getting the timestamp
    ts = datetime.timestamp(dt)

    with open('code_log.txt', 'a') as log_file:
        log_file.write(f'{ts} : {message}\n')
 

def extract(url, table_attribs=None):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    # request for HTML document of given url 
    response = requests.get(url) 
      
    # text to BS4
    soup = BeautifulSoup(response.text , 'html.parser') 
    table = soup.find_all('table')
    df = pd.read_html(str(table))[0]

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')
df = extract('https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks')
print(df)
log_progress('Preliminaries complete. Initiating ETL process')
# transform()
# log_progress('Preliminaries complete. Initiating ETL process')
# load_to_csv()
# log_progress('Preliminaries complete. Initiating ETL process')
# #init sql connection

# log_progress('SQL Connection initiated')
# load_to_db()
# log_progress('Data loaded to Database as a table, Executing queries')
# run_query()
# log_progress('Process Complete')
# #close the sql connection
# log_progress('Server Connection closed')
