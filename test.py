from datetime import datetime,date
import sys
try:
    today = date.today()

    iso_year, iso_week, iso_weekday = today.isocalendar()

    if iso_week == 1 and today.month == 1:
        iso_year -= 1
    # Tutaj bierze do selecta z db, wiec jak mamy 2025 i 14 tydizeń to powinno być 2025_13
    week_number = f"{iso_year}_{iso_week-1:02d}"
    print(week_number)
except Exception as e:
    with open ('logfile.log', 'a', encoding='utf-8') as file:
        file.write(f"""Brak możliwości pobrania danych. Problem z ustawieniem nazwy YearWeak z datatime - {str(e)}\n""")
    sys.exit(0)    
