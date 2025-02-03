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
    
    x = azure_cursor.execute("SELECT count(*) FROM SalesData_CZ").fetchall()
    
    print(x)
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
    
    


# Zakończenie pomiaru czasu
try:
    end_time = time.time()
    seconds_sum = end_time - start_time
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Problem z wczytaniem czasu końcowego - {str(e)}\n""")


