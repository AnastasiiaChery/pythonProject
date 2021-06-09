import csv
from pathlib import Path

import mysql.connector
from mysql.connector import Error, cursor


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


connection = create_connection("localhost", "root", "root", "test")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


create_table = """
CREATE TABLE IF NOT EXISTS stock (
  id INT AUTO_INCREMENT, 
  name_company TEXT NOT NULL, 
  date  DATE ,
  open FLOAT, 
  high FLOAT,
  low FLOAT,
  close FLOAT,
  adj_close FLOAT,
  volume FLOAT,
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""

execute_query(connection, create_table)
def insert_funct(file):
    with open(file) as f:
        referrals = csv.DictReader(f)
        for items in referrals:
            date=items['Date']
            open_=items['Open']
            high=items['High']
            low=items['Low']
            close=items['Close']
            adj_close=items['Adj Close']
            volume=items['Volume']
            name_company=Path(file).resolve().stem


            sql=  "INSERT INTO `stock`  ( 'name_company' , 'date',' open', 'high','low','close','adj_close','volume') VALUES (  %s, %s, %s, %s, %s, %s, %s, %s )"
            val=('ZUO', date, open_, high, low, close, adj_close, volume )

            cursor = connection.cursor()
            cursor.execute("INSERT INTO `stock` VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )", (0, name_company, date, open_, high, low, close, adj_close, volume ))
            connection.commit()

insert_funct('./informations/ZUO.csv')
insert_funct('./informations/CLDR.csv')
insert_funct('./informations/DOCU.csv')
insert_funct('./informations/PD.csv')
insert_funct('./informations/PINS.csv')
insert_funct('./informations/RUN.csv')
insert_funct('./informations/ZM.csv')

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

select_users = "SELECT * FROM stock"
stocks = execute_read_query(connection, select_users)

for st in stocks:
    print(st)