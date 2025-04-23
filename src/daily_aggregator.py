import sys
import os
import json
import traceback
from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType, FloatType, IntegerType, DateType)


SCHWELLWERTE_PFAD = "/app/config/schwellwerte.json"
INPUT_DATA_PFAD_TEMPLATE = "/data/raw/{}"
PROCESSED_EVENTS_TABLE = "processed_machine_events"
HOURLY_SUMMARY_TABLE = "hourly_machine_summary"
DB_DRIVER = "org.postgresql.Driver"
TIMESTAMP_FORMAT_INPUT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" # ISO 8601 UTC
DATE_FORMAT_SUMMARY = "yyyy-MM-dd"
CYCLE_START_EVENT = "Cycle_Start"
CYCLE_END_EVENT = "Cycle_End"

# Mapping: Schwellwert-Konfiguration -> DataFrame Parameter/Event für die prüfung Fehlerprüfung
PARAMETER_EVENT_MAPPING = {
    "AS_VacuumUnits":         {"df_param_name": "AS_VacuumUnits", "event_name": "AS_Check"},
    "PP_VacuumUnits":         {"df_param_name": "PP_VacuumUnits", "event_name": "Pick_Check"},
    "AS_VacuumUnits_Release": {"df_param_name": "AS_VacuumUnits", "event_name": "AS_Blowoff_Check"},
    "PP_VacuumUnits_Release": {"df_param_name": "PP_VacuumUnits", "event_name": "Place_Check"},
    "PickForce":              {"df_param_name": "PP_Force",       "event_name": "Pick_Check"},
    "PlaceForce":             {"df_param_name": "PP_Force",       "event_name": "Place_Check"}
}

# Hilfsfunktionen

def berechne_zyklen(roh_events_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df_mit_id = roh_events_df.withColumn("eindeutige_id", F.monotonically_increasing_id())
    window_spec = Window.partitionBy("machine_id").orderBy("event_timestamp", "eindeutige_id")

    df_mit_start_flag = df_mit_id.withColumn(
        "is_start_flag",
        F.when(F.col("event_name") == CYCLE_START_EVENT, 1).otherwise(0)
    )
    events_mit_zyklus_nr_df = df_mit_start_flag.withColumn(
        "cycle_seq",
        F.sum("is_start_flag").over(window_spec)
    )
    
    events_in_zyklen_df = events_mit_zyklus_nr_df.filter(F.col("cycle_seq") > 0)

    nur_start_und_end_events_df = events_in_zyklen_df.filter(
        F.col("event_name").isin(CYCLE_START_EVENT, CYCLE_END_EVENT)
    )

    zyklus_grenzen_df = nur_start_und_end_events_df.groupBy("machine_id", "cycle_seq").agg(
        F.min("event_timestamp").alias("cycle_start_ts"),
        F.max("event_timestamp").alias("cycle_end_ts")
    )

    zyklus_zeiten_df = zyklus_grenzen_df.withColumn(
        "cycle_time_seconds",
        F.when(
            F.col("cycle_start_ts").isNotNull() & F.col("cycle_end_ts").isNotNull(),
            F.col("cycle_end_ts").cast("double") - F.col("cycle_start_ts").cast("double")
        ).otherwise(None).cast(FloatType())
    )

    events_mit_zyklus_nr_df = events_mit_zyklus_nr_df.drop("eindeutige_id", "is_start_flag")
    zyklus_zeiten_df = zyklus_zeiten_df.select("machine_id", "cycle_seq", "cycle_start_ts", "cycle_time_seconds")

    return events_mit_zyklus_nr_df, zyklus_zeiten_df


def finde_fehler_basierend_auf_schwellwerten(events_df: DataFrame, schwellwerte_config: dict) -> DataFrame:
    
    fehler_bedingungen_liste = []

    for config_key, regel in schwellwerte_config.items():
        if config_key.startswith("_") or not isinstance(regel, dict):
            continue

        mapping_info = PARAMETER_EVENT_MAPPING.get(config_key)
        param_name_im_df = mapping_info.get("df_param_name")
        relevanter_event_name = mapping_info.get("event_name")
        
        ist_relevantes_event_und_parameter = (
            (F.col("parameter_name") == param_name_im_df) &
            (F.col("event_name") == relevanter_event_name) &
            (F.col("value").isNotNull())
        )

        fehler_bedingung_fuer_regel = None
        if "error_if_above" in regel:
            fehler_bedingung_fuer_regel = (F.col("value") > regel["error_if_above"])
        elif "error_if_below" in regel:
            fehler_bedingung_fuer_regel = (F.col("value") < regel["error_if_below"])
        elif "error_if_outside_range" in regel:
            grenzen = regel["error_if_outside_range"]
            
            fehler_bedingung_fuer_regel = (F.col("value") < grenzen[0]) | (F.col("value") > grenzen[1])
        else:
             continue

        if fehler_bedingung_fuer_regel is not None:
            gesamte_bedingung_fuer_diese_regel = ist_relevantes_event_und_parameter & fehler_bedingung_fuer_regel
            fehler_bedingungen_liste.append(gesamte_bedingung_fuer_diese_regel)

    if fehler_bedingungen_liste:
        gesamte_fehler_bedingung = fehler_bedingungen_liste[0]
        for bedingung in fehler_bedingungen_liste[1:]:
            gesamte_fehler_bedingung = gesamte_fehler_bedingung | bedingung
    else:
        gesamte_fehler_bedingung = F.lit(False)

    events_mit_fehler_spalte = events_df.withColumn(
        "is_error",
        F.when(gesamte_fehler_bedingung, 1).otherwise(0).cast(IntegerType())
    )

    return events_mit_fehler_spalte


# Hauptfunktion
def main(input_datei_name: str):
    
    spark: SparkSession = None

    try:
        app_name = f"MaschinenEventVerarbeitung_{os.path.basename(input_datei_name)}"
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        
        schwellwerte = {}
        try:
            with open(SCHWELLWERTE_PFAD, 'r', encoding='utf-8') as f:
                schwellwerte = json.load(f)
        except FileNotFoundError:
            print(f"Schwellwert-Datei '{SCHWELLWERTE_PFAD}' nicht gefunden.")
        except json.JSONDecodeError as e:
            print(f"Schwellwert-Datei '{SCHWELLWERTE_PFAD}' ist ungültiges JSON: {e}.")
            raise
        except Exception as e:
            print(f"Unerwarteter Fehler beim Laden der Schwellwerte: {e}")

        db_host = os.environ.get('DB_HOST')
        db_name = os.environ.get('DB_NAME')
        db_user = os.environ.get('DB_USER')
        db_pass = os.environ.get('DB_PASS')
        if not all([db_host, db_name, db_user, db_pass]):
            raise ValueError("FEHLER: DB-Zugangsdaten (DB_HOST, DB_NAME, DB_USER, DB_PASS) nicht vollständig gesetzt!")
        db_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"
        db_properties = {"user": db_user, "password": db_pass, "driver": DB_DRIVER}

        input_schema = StructType([
            StructField("timestamp", StringType(), True), StructField("machine_id", StringType(), True),
            StructField("event_name", StringType(), True), StructField("parameter_name", StringType(), True),
            StructField("value", StringType(), True)
        ])
        input_pfad = INPUT_DATA_PFAD_TEMPLATE.format(input_datei_name)
        print(f"Lese Rohdaten aus CSV: '{input_pfad}'")
        rohdaten_df = spark.read.csv(input_pfad, header=True, schema=input_schema, timestampFormat=TIMESTAMP_FORMAT_INPUT)

        basis_events_df = rohdaten_df \
            .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp"), TIMESTAMP_FORMAT_INPUT)) \
            .withColumn("value_float", F.col("value").cast(FloatType())) \
            .fillna("", subset=["parameter_name"]) \
            .dropna(subset=["event_timestamp", "machine_id", "event_name"]) \
            .drop("timestamp", "value") \
            .withColumnRenamed("value_float", "value") \
            .filter(F.col("event_timestamp").isNotNull())

        events_mit_zyklus_nr_df, zyklus_zeiten_df = berechne_zyklen(basis_events_df)
        events_mit_fehler_df = finde_fehler_basierend_auf_schwellwerten(events_mit_zyklus_nr_df, schwellwerte)

        finale_events_df = events_mit_fehler_df.join(
            zyklus_zeiten_df.select("machine_id", "cycle_seq", "cycle_time_seconds"),
            on=["machine_id", "cycle_seq"],
            how="left"
        )
        finale_events_gerundet_df = finale_events_df.withColumn(
            "cycle_time_seconds", F.round(F.col("cycle_time_seconds"), 3)
        )
        events_zum_speichern_df = finale_events_gerundet_df.select(
            "event_timestamp", "machine_id", "event_name", "parameter_name",
            "value", "is_error", "cycle_seq", "cycle_time_seconds"
        )
        try:
            events_zum_speichern_df.write.jdbc(
                url=db_url, table=PROCESSED_EVENTS_TABLE, mode="append", properties=db_properties
            )
        except Exception as e:
            print(f"FEHLER beim Speichern der Events in '{PROCESSED_EVENTS_TABLE}': {e}")
            traceback.print_exc()

        zyklen_mit_stunde_df = zyklus_zeiten_df \
            .withColumn("summary_date", F.date_format(F.col("cycle_start_ts"), DATE_FORMAT_SUMMARY).cast(DateType())) \
            .withColumn("hour_of_day", F.hour(F.col("cycle_start_ts"))) \
            .filter(F.col("summary_date").isNotNull())

        zyklus_summary_df = zyklen_mit_stunde_df \
            .groupBy("summary_date", "hour_of_day", "machine_id") \
            .agg(
                F.countDistinct("cycle_seq").alias("cycle_count"),
                F.avg("cycle_time_seconds").alias("avg_cycle_time_seconds"),
                F.min("cycle_time_seconds").alias("min_cycle_time_seconds"), 
                F.max("cycle_time_seconds").alias("max_cycle_time_seconds") 
            )

        events_mit_zyklus_start_df = events_mit_fehler_df.join(
            zyklus_zeiten_df.select("machine_id", "cycle_seq", "cycle_start_ts"),
            on=["machine_id", "cycle_seq"], how="left"
        )
        events_mit_stunde_df = events_mit_zyklus_start_df \
             .withColumn("summary_date", F.date_format(F.col("cycle_start_ts"), DATE_FORMAT_SUMMARY).cast(DateType())) \
             .withColumn("hour_of_day", F.hour(F.col("cycle_start_ts"))) \
             .filter(F.col("summary_date").isNotNull())

        event_summary_df = events_mit_stunde_df \
             .groupBy("summary_date", "hour_of_day", "machine_id") \
             .agg(
                 F.avg(F.when((F.col("event_name") == "Pick_Check") & (F.col("parameter_name") == "PP_Force"), F.col("value"))).alias("avg_pick_force"),
                 F.max(F.when((F.col("event_name") == "Pick_Check") & (F.col("parameter_name") == "PP_Force"), F.col("value"))).alias("max_pick_force"),
                 F.min(F.when((F.col("event_name") == "Pick_Check") & (F.col("parameter_name") == "PP_Force"), F.col("value"))).alias("min_pick_force"),
                 F.avg(F.when((F.col("event_name") == "Place_Check") & (F.col("parameter_name") == "PP_Force"), F.col("value"))).alias("avg_place_force"),
                 F.max(F.when((F.col("event_name") == "Place_Check") & (F.col("parameter_name") == "PP_Force"), F.col("value"))).alias("max_place_force"),
                 F.min(F.when((F.col("event_name") == "Place_Check") & (F.col("parameter_name") == "PP_Force"), F.col("value"))).alias("min_place_force"),
                 F.sum(F.when((F.col("parameter_name") == "AS_VacuumUnits") & (F.col("event_name") == "AS_Check") & (F.col("is_error") == 1), 1).otherwise(0)).alias("as_vacuum_error_count"),
                 F.sum(F.when((F.col("parameter_name") == "PP_VacuumUnits") & (F.col("event_name") == "Pick_Check") & (F.col("is_error") == 1), 1).otherwise(0)).alias("pp_vacuum_error_count"),
                 F.sum(F.when((F.col("parameter_name") == "AS_VacuumUnits") & (F.col("event_name") == "AS_Blowoff_Check") & (F.col("is_error") == 1), 1).otherwise(0)).alias("as_release_error_count"),
                 F.sum(F.when((F.col("parameter_name") == "PP_VacuumUnits") & (F.col("event_name") == "Place_Check") & (F.col("is_error") == 1), 1).otherwise(0)).alias("pp_release_error_count"),
                 F.sum(F.when((F.col("parameter_name") == "PP_Force") & (F.col("event_name") == "Pick_Check") & (F.col("is_error") == 1), 1).otherwise(0)).alias("pick_force_error_count"),
                 F.sum(F.when((F.col("parameter_name") == "PP_Force") & (F.col("event_name") == "Place_Check") & (F.col("is_error") == 1), 1).otherwise(0)).alias("place_force_error_count"),
                 F.sum("is_error").alias("total_error_count")
             )

        stunden_zusammenfassung_df = zyklus_summary_df.join(
            event_summary_df,
            on=["summary_date", "hour_of_day", "machine_id"],
            how="outer"
        ).fillna(0, subset=[
            "cycle_count", "avg_cycle_time_seconds", "min_cycle_time_seconds", "max_cycle_time_seconds",
            "avg_pick_force", "max_pick_force", "min_pick_force", "avg_place_force", "max_place_force", "min_place_force",
            "as_vacuum_error_count", "pp_vacuum_error_count", "as_release_error_count", "pp_release_error_count",
            "pick_force_error_count", "place_force_error_count", "total_error_count"
        ])
        
        stunden_zusammenfassung_gerundet_df = stunden_zusammenfassung_df \
            .withColumn("min_cycle_time_seconds", F.round(F.col("min_cycle_time_seconds"), 3)) \
            .withColumn("max_cycle_time_seconds", F.round(F.col("max_cycle_time_seconds"), 3)) \
            .withColumn("avg_cycle_time_seconds", F.round(F.col("avg_cycle_time_seconds"), 3)) \
            .withColumn("avg_pick_force", F.round(F.col("avg_pick_force"), 2)) \
            .withColumn("max_pick_force", F.round(F.col("max_pick_force"), 2)) \
            .withColumn("min_pick_force", F.round(F.col("min_pick_force"), 2)) \
            .withColumn("avg_place_force", F.round(F.col("avg_place_force"), 2)) \
            .withColumn("max_place_force", F.round(F.col("max_place_force"), 2)) \
            .withColumn("min_place_force", F.round(F.col("min_place_force"), 2))

        zusammenfassung_zum_speichern_df = stunden_zusammenfassung_gerundet_df.select(
            "summary_date", "hour_of_day", "machine_id",
            "avg_pick_force", "max_pick_force", "min_pick_force",
            "avg_place_force", "max_place_force", "min_place_force",
            "as_vacuum_error_count", "pp_vacuum_error_count",
            "as_release_error_count", "pp_release_error_count",
            "pick_force_error_count", "place_force_error_count",
            "cycle_count", "min_cycle_time_seconds",
            "max_cycle_time_seconds", "avg_cycle_time_seconds"
        )
        try:
            zusammenfassung_zum_speichern_df.write.jdbc(
                url=db_url, table=HOURLY_SUMMARY_TABLE, mode="append", properties=db_properties
            )
        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e).lower() and "hourly_machine_summary_pkey" in str(e).lower():
                 print(f"Primary Key-Verletzung beim einfügen in '{HOURLY_SUMMARY_TABLE}'Übersprungen.")
            else:
                print(f"FEHLER beim Anhängen der Zusammenfassung an '{HOURLY_SUMMARY_TABLE}': {e}")
                traceback.print_exc()

    except ValueError as ve:
        print(f"Konfigurations- oder Daten-Fehler: {ve}")
        if spark: spark.stop()
        sys.exit(1)
    except Exception as e:
        print(f"Unerwarteter Fehler: {e}")
        traceback.print_exc()
        if spark: spark.stop()
        sys.exit(1)

    finally:
        if spark and spark.getActiveSession():
            print("Spark Session wird beendet.")
            spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        datei_name_arg = sys.argv[1]
        if not datei_name_arg or "/" in datei_name_arg or "\\" in datei_name_arg or not datei_name_arg.lower().endswith(".csv"):
             print(f"FEHLER: Ungültiger Dateiname '{datei_name_arg}'. Nur Dateiname erwartet.")
             sys.exit(1)
    else:
        print("FEHLER: Name der CSV-Datei als Argument benötigt.")
        print("Beispiel: python daily_aggregator.py daten.csv")
        sys.exit(1)
    print("Starte Verarbeitung der Datei '{datei_name_arg}'")
    main(datei_name_arg)

    print(f"Verarbeitung der Datei '{datei_name_arg}' abgeschlossen.")
    