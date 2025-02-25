"""nadas import"""
import time
import os
from datetime import datetime, date
import smtplib
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import sys
import csv

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

# Wczytanie zmiennych środowiskowych z pliku .env
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

try:
    today = date.today()

    iso_year, iso_week, iso_weekday = today.isocalendar()

    if iso_week == 1 and today.month == 1:
        iso_year -= 1
    # Tutaj bierze do selecta z db, wiec jak mamy 2025 i 14 tydizeń to powinno być 2025_13
    week_number = f"{iso_year}_{iso_week-1:02d}"
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Brak możliwości pobrania danych. Problem z ustawieniem nazwy YearWeak z datatime - {str(e)}\n""")
    sys.exit(0)    


#Połączenie z bazą danych AZURE
counter = 1 # Licznik prób połączenia z bazą danych
while counter < 10:
    try:
        azure_conn = pyodbc.connect(
        f'DRIVER={db_driver};'
        f'SERVER=tcp:{azure_server},1433;'
        f'DATABASE={azure_db};'
        f'UID={azure_user};'
        f'PWD={azure_password};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'Connection Timeout=30;'
        )

        azure_cursor = azure_conn.cursor()
        
        cz_data = azure_cursor.execute("SELECT * FROM WarehousesData_CZ WHERE YearWeek LIKE ?", (week_number,)).fetchall()
        sk_data = azure_cursor.execute("SELECT * FROM WarehousesData_SK WHERE YearWeek LIKE ?", (week_number,)).fetchall()
    
        print(f"Zamykam połączenie z bazą danych. Udana próba połączenia: {counter}") 
        break
    except Exception as e:
        print(f"Nieudana próba połączenia: {counter}")
        with open ('logfile.log', 'a', encoding='utf-8') as file:
            file.write(f"""{formatDateTime} Problem z połączneiem z baza danych azure próba {counter} - {str(e)}\n""")
        
    finally:
        if 'azure_cursor' in locals() and azure_cursor:
            print("kursor")
            azure_cursor.close()
        if 'azure_conn' in locals() and azure_conn:
            print("conn")
            azure_conn.close()
        print(f"Połączenie z bazą danych zakończone próba {counter}")
        counter += 1
        time.sleep(15)
    
print(len(cz_data))
print(len(sk_data))

# Zakończenie pomiaru czasu
try:
    end_time = time.time()
    seconds_sum = end_time - start_time
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wczytaniem czasu końcowego - {str(e)}\n""")

print("czas trwania: ", seconds_sum)

print("First 5 rows of WarehousesData_CZ:")
for row in cz_data[:5]:
    print(row)

print("\nFirst 5 rows of WarehousesData_SK:")
for row in sk_data[:5]:
    print(row)

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
    # Data to be inserted (replace with your actual data source)
    table_names = {'WarehousesData_CZ': cz_data, 'WarehousesData_SK': sk_data}

    insert_query_template = """INSERT INTO {table_name} (
    BranchCode,
    ProductCode,
    YearWeek,
    Quantity,
    Sales,
    Timestamp
    ) VALUES (?, ?, ?, ?, ?, ?)"""

    for table_name, data in table_names.items():
        print(f"Processing table: {table_name}")
        
        if data:
            # Convert numeric values to decimals for consistency
            formatted_data = [
                (row[0], row[1], row[2], float(row[3]), float(row[4]), row[5]) for row in data
            ]

            # Format insert query for the specific table
            insert_query = insert_query_template.format(table_name=table_name)

            # Insert data into the local database
            local_cursor.executemany(insert_query, formatted_data)
            local_cnxn.commit()
            print(f"Inserted {len(data)} rows into {table_name}")
        else:
            print(f"No data found for {table_name}")

except Exception as e:
    with open('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"Problem z wgrywaniem danych do lokalnej bazy danych - {str(e)}\n")
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
    

print("czas trwania: ", seconds_sum)