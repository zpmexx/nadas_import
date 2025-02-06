"""nadas import"""
import time
import os
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from dotenv import load_dotenv
import pyodbc
import sys

# Wczytanie daty
try:
    now = datetime.now()
    current_year = datetime.today().year # Current year variable
    current_month = datetime.today().month # Current month variable
    formatDateTime = now.strftime("%d/%m/%Y %H:%M")
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wczytyaniem daty - {str(e)}\n""")
    sys.exit(0)

# Wczytanie zmiennych środowiskowych
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(base_dir, '.env')
    load_dotenv(dotenv_path)

    db_db = os.environ['db_db']
    db_server = os.environ['db_server']
    db_driver = os.environ['db_driver']
    
    azure_server = os.environ['azure_server']
    azure_user = os.environ['azure_user']
    azure_password = os.environ['azure_password']
    azure_db = os.environ['azure_db']

except Exception as e:

    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wczytywaniem zmiennych środowiskowych - {str(e)}\n""")
    sys.exit(0)
        
# Rozpoczęcie pomiaru czasu
try:
    start_time = time.time()
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wczytaniem czasu początkowego - {str(e)}\n""")
    sys.exit(0)

#Połączenie z bazą danych AZURE
try:
    azure_conn = pyodbc.connect(
        f'DRIVER={db_driver};'
        f'SERVER=tcp:{azure_server},1433;'
        f'DATABASE={azure_db};'
        f'UID={azure_user};'
        f'PWD={azure_password};'
        'Encrypt=yes;'               # encryption is required for Azure SQL
        'TrustServerCertificate=no;' # validate the server certificate
        'Connection Timeout=30;'
    )
    azure_cursor = azure_conn.cursor()
    
    cz_data = azure_cursor.execute("SELECT * FROM SalesData_CZ").fetchall()
    sk_data = azure_cursor.execute("SELECT * FROM SalesData_SK").fetchall()
    
    print("Zamykam połączenie z bazą danych")
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z połączneiem z baza danych azure - {str(e)}\n""")
    
finally:
    if 'azure_cursor' in locals() and azure_cursor:
        print("kursor")
        azure_cursor.close()
    if 'azure_conn' in locals() and azure_conn:
        print("conn")
        azure_conn.close()
    print("Połączenie z bazą danych zakończone")
    
print(len(cz_data))
print(len(sk_data))

# Zakończenie pomiaru czasu
try:
    end_time = time.time()
    seconds_sum = end_time - start_time
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wczytaniem czasu końcowego - {str(e)}\n""")




# Połączenie z bazą danych lokalną
try:
    local_cnxn = pyodbc.connect(f"Driver={{ODBC Driver 17 for SQL Server}};Server={db_server};Database={db_db};Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;")
    local_cursor = local_cnxn.cursor()

except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z połączeniem z baza danych - {str(e)}\n""")
    sys.exit(0)
    
# Wgrywanie danych do bazy danych lokalnej
try:
    table_names = {'SalesData_CZ': cz_data, 'SalesData_SK': sk_data}
    
    insert_query_template = """INSERT INTO {table_name} (
    DocumentNumber,
    IssueDate,
    BranchCode,
    Refund,
    ItemNumber,
    Ean,
    ProductCode,
    ProductName,
    PurchasePriceExcludingVat,
    Quantity,
    VatRate,
    PriceExcludingVat,
    VatPrice,
    TotalPrice,
    Margin,
    Timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    
    for table_name, data in table_names.items():
        print(f"Processing table: {table_name}")
        if data:
            # Format insert query for the specific table
            insert_query = insert_query_template.format(table_name=table_name)

            # Insert data into the local database
            local_cursor.executemany(insert_query, data)
            local_cnxn.commit()
            print(f"Inserted {len(data)} rows into {table_name}")
        else:
            print(f"No data found for {table_name}")
 
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wgyrwaniem danych do lokalnej bazy danych - {str(e)}\n""")
    sys.exit(0)


# Wgranie czasu wykonywania do bazy danych
try:
    local_cursor.execute("""
    MERGE INTO executions_time AS target
    USING (VALUES (?, ?, ?)) AS source (year, month, seconds_sum)
    ON target.year = source.year AND target.month = source.month
    WHEN MATCHED THEN
        UPDATE SET target.seconds_sum = target.seconds_sum + source.seconds_sum
    WHEN NOT MATCHED THEN
        INSERT (year, month, seconds_sum)
        VALUES (source.year, source.month, source.seconds_sum);
""", (current_year, current_month, seconds_sum))
    local_cnxn.commit()
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wgraniem czasu wykonywania do bazy danych - {str(e)}\n""")

if 'local_cursor' in locals() and local_cursor:
    local_cursor.close()
if 'local_cnxn' in locals() and local_cnxn:
    local_cnxn.close()