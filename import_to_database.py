import pandas as pd
import mysql.connector
from mysql.connector import errorcode

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'bankdb',
    'raise_on_warnings': True,
    'autocommit': True
}

CSV_FILE = "bank_clean.csv"

df = pd.read_csv(CSV_FILE)

try:
    conn = mysql.connector.connect(**DB_CONFIG)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        raise SystemExit("Error: Access denied (check user/password).")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        raise SystemExit("Error: Database does not exist.")
    else:
        raise

cursor = conn.cursor()

create_table = """
CREATE TABLE IF NOT EXISTS bank_customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    age INT NOT NULL,
    balance DOUBLE,
    housing TINYINT,
    loan TINYINT,
    campaign INT
) ENGINE=InnoDB;
"""
cursor.execute(create_table)

insert_sql = """
INSERT INTO bank_customers (age, balance, housing, loan, campaign)
VALUES (%s, %s, %s, %s, %s)
"""
data = [tuple(x) for x in df[['age','balance','housing','loan','campaign']].to_numpy()]
batch_size = 1000
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    cursor.executemany(insert_sql, batch)

cursor.close()
conn.close()
print("Import finished.")
