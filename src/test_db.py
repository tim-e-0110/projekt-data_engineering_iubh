import os
import psycopg2
import sys
import time

# Lese DB-Infos aus Umgebungsvariablen
db_host = os.environ.get('DB_HOST', 'postgres_db')
db_name = os.environ.get('DB_NAME', 'manufacturing_db')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')

if not db_user or not db_pass:
    print("FEHLER: DB_USER oder DB_PASS Umgebungsvariable nicht gesetzt!")
    print("Stelle sicher, dass eine .env Datei existiert und die Variablen enthält.")
    sys.exit(1)

conn_params = {
    "host": db_host,
    "database": db_name,
    "user": db_user,
    "password": db_pass,
    "port": 5432 # Standard Postgres Port
}

print(f"Versuche Verbindung zur Datenbank: dbname='{db_name}' user='{db_user}' host='{db_host}'")


time.sleep(5)

conn = None 
try:
    conn = psycopg2.connect(**conn_params)
    print("Verbindung zur Datenbank erfolgreich hergestellt!")

    with conn.cursor() as cur:
        
        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print(f"PostgreSQL Version: {db_version[0]}")

        cur.execute("SELECT 1 + 1;")
        result = cur.fetchone()
        print(f"Testabfrage 'SELECT 1 + 1;' Ergebnis: {result[0]}")

    print("Datenbankabfrage erfolgreich ausgeführt.")
    sys.exit(0) # Erfolgreich beenden

except psycopg2.OperationalError as e:
    print(f"FEHLER beim Verbinden zur Datenbank: {e}")
    sys.exit(1) 
except Exception as e:
    print(f"Ein unerwarteter Fehler ist aufgetreten: {e}")
    sys.exit(1) 

finally:
    if conn is not None:
        conn.close()
        print("Datenbankverbindung geschlossen.")