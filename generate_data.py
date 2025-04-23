import sys
import datetime
from datetime import timedelta
import random
import csv
import os

MACHINE_ID = "DieBonder_01"
SimDauer = 1*24    # Angabe in Stunden.

if len(sys.argv) != 2:
    print("Benutzung: python generate_data.py <Startdatum im Format YYYY-MM-DD>")
    sys.exit(1)

datum_input = sys.argv[1]
StartZeit_STR = datum_input + "T00:00"

try:
    dt_naive = datetime.datetime.strptime(StartZeit_STR, "%Y-%m-%dT%H:%M")
except ValueError:
    print(f"Fehler: Ungültiges Datumsformat '{datum_input}'. Erwartet z. B. 2024-10-16")
    sys.exit(1)
    
StartZeit = dt_naive.replace(second=0, microsecond=0, tzinfo=datetime.UTC)
EndeZeit = StartZeit + datetime.timedelta(hours=SimDauer)

fehlerRate_AS_Vacuum = 0.01
fehlerRate_PP_Vacuum = 0.02
fehlerRate_AS_Blow = 0.01
fehlerRate_PP_Blow = 0.01
fehlerRate_Pick_Force = 0.01
fehlerRate_Place_Force = 0.01

AS_Vacuum_ok_range = (40.0,70.0)
AS_Vacuum_error_range = (70.1, 100.0)
PP_Vacuum_ok_range = (50.0, 75.0)
PP_Vacuum_error_range = (75.01, 120)
Pick_Force_ok_range = (60.0, 120.0)
Pick_Force_error_range = (120.01, 250)
Place_Force_ok_range = (60.0, 120.0)
Place_Force_error_range = (120.01, 250)
AS_Blow_ok_range = (450.0,550.0)
AS_Blow_error_range = (350.0, 449.99)
PP_Blow_ok_range = (450.0, 550.0)
PP_Blow_error_range = (350.0, 449.99)

delta1 = (130, 150)
delta2 = (95, 105)
delta3 = (50, 55)
delta4 = (220, 250)
delta5 = (80, 100)


output_filename = f"machine_event_logs_{MACHINE_ID}_{StartZeit.strftime('%Y-%m-%d_%H-%M')}_to_{EndeZeit.strftime('%Y-%m-%d_%H-%M')}.csv"
data_dir = "./raw_data"
os.makedirs(data_dir, exist_ok=True)
output_filepath = os.path.join(data_dir, output_filename)

print(f"Schreibe Daten in: {output_filepath}")

Simulationszeit_aktuell = StartZeit

                
with open(output_filepath, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["timestamp", "machine_id", "event_name", "parameter_name", "value"]) # Header
    
    GesamtDauer = (EndeZeit - StartZeit).total_seconds()
    Prozentanzeige = -1
    Datenzeilen_counter=0
    
    while Simulationszeit_aktuell < EndeZeit:
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "Cycle_Start", None, None])
        Simulationszeit_aktuell += timedelta(milliseconds=random.uniform(*delta1)) # PP fährt zur Abholposition
        
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "AS_Check", "AS_VacuumUnits", round(random.uniform(*(AS_Vacuum_error_range if random.random() <= fehlerRate_AS_Vacuum else AS_Vacuum_ok_range)),2)])
        Simulationszeit_aktuell += timedelta(milliseconds=random.uniform(*delta2)) # Ausstechen und Pickup-Verzögerung
        
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "Pick_Check", "PP_VacuumUnits", round(random.uniform(*(PP_Vacuum_error_range if random.random() <= fehlerRate_PP_Vacuum else PP_Vacuum_ok_range)),2)])
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "Pick_Check", "PP_Force", round(random.uniform(*(Pick_Force_error_range if random.random() <= fehlerRate_Pick_Force else Pick_Force_ok_range)), 2)])
        Simulationszeit_aktuell += timedelta(milliseconds=random.uniform(*delta3)) # Maschinenverzögerung zw. Pickup und AS-BlowOff
        
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "AS_Blowoff_Check", "AS_VacuumUnits", round(random.uniform(*(AS_Blow_error_range if random.random() <= fehlerRate_AS_Blow else AS_Blow_ok_range)), 2)])
        Simulationszeit_aktuell += timedelta(milliseconds=random.uniform(*delta4)) # PP fährt zur Bestückposition
        
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "Place_Check", "PP_Force", round(random.uniform(*(Place_Force_error_range if random.random() <= fehlerRate_Place_Force else Place_Force_ok_range)), 2)])
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "Place_Check", "PP_VacuumUnits", round(random.uniform(*(PP_Blow_error_range if random.random() <= fehlerRate_PP_Blow else PP_Blow_ok_range)), 2)])
        Simulationszeit_aktuell += timedelta(milliseconds=random.uniform(*delta5)) # Bauteilsuche/Fahrt zur Warteposition
        
        writer.writerow([Simulationszeit_aktuell.isoformat(timespec='milliseconds').replace('+00:00', 'Z'), MACHINE_ID, "Cycle_End", None, None])
        Datenzeilen_counter+=8
        
        Fortschritt_abs = (Simulationszeit_aktuell - StartZeit).total_seconds()
        Fortschritt_rel = int((Fortschritt_abs / GesamtDauer) * 100)
        if Fortschritt_rel // 10 > Prozentanzeige // 10:
            Prozentanzeige = Fortschritt_rel
            print(f"Fortschritt: {Fortschritt_rel}%")
print(f"Simulation abgeschlossen. {Datenzeilen_counter} Datenzeilen erzeugt.")