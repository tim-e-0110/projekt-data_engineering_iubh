import streamlit as st
import pandas as pd
import psycopg2
import os
import traceback
import datetime
import plotly.graph_objects as go


@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST', 'postgres_db'),
            database=os.environ.get('DB_NAME', 'manufacturing_db'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASS'),
            port=5432
        )
        return conn
    except Exception as e:
        st.error(f"Fehler bei Datenbankverbindung: {e}")
        traceback.print_exc()
        return None


@st.cache_data(ttl=600)
def get_available_dates(_conn):
    try:
        query = """
            SELECT DISTINCT summary_date
            FROM hourly_machine_summary
            ORDER BY summary_date DESC;
        """
        df = pd.read_sql_query(query, _conn)
        df['summary_date'] = pd.to_datetime(df['summary_date']).dt.date
        return df
    except Exception as e:
        st.error(f"Fehler beim Laden der verfügbaren Daten: {e}")
        traceback.print_exc()
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_summary_data(_conn, selected_date):
    try:
        query = """
            SELECT
                summary_date, hour_of_day, machine_id,
                avg_pick_force, max_pick_force, min_pick_force,
                avg_place_force, max_place_force, min_place_force,
                as_vacuum_error_count, pp_vacuum_error_count,
                as_release_error_count, pp_release_error_count,
                pick_force_error_count, place_force_error_count,
                cycle_count, avg_cycle_time_seconds,
                min_cycle_time_seconds, max_cycle_time_seconds
            FROM hourly_machine_summary
            WHERE summary_date = %(date)s
            ORDER BY hour_of_day ASC;
        """
        df = pd.read_sql_query(query, _conn, params={"date": selected_date})
        if not df.empty:
            df['timestamp_hour'] = pd.to_datetime(df['summary_date']) + pd.to_timedelta(df['hour_of_day'], unit='h')
        return df
    except Exception as e:
        st.error(f"Fehler beim Laden der Daten für {selected_date}: {e}")
        traceback.print_exc()
        return pd.DataFrame()


def create_timeseries_plot(df, x_col, y_cols, title, y_axis_title, height=400, custom_names=None):
    fig = go.Figure()
    for col in y_cols:
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[col],
            mode='lines+markers',
            name=custom_names[col] if custom_names and col in custom_names else col.replace('_', ' ').title()
        ))
    fig.update_layout(
        title=title,
        xaxis_title='Zeit',
        yaxis_title=y_axis_title,
        yaxis=dict(fixedrange=True),
        height=height,
        legend_title_text='Metriken'
    )
    return fig



st.set_page_config(page_title="Maschinen-Dashboard", layout="wide")
st.title("⚙️ Maschinen-Event Dashboard")
st.caption("Tägliche Prozessdatenübersicht")

conn = get_db_connection()

if not conn:
    st.error("Datenbankverbindung nicht hergestellt!")
    st.stop() 

date_df = get_available_dates(conn)

if date_df.empty:
    st.warning("Keine Daten in der Datenbank gefunden.")
    st.stop() 

available_years = sorted(date_df['summary_date'].apply(lambda d: d.year).unique(), reverse=True)
selected_year = st.sidebar.selectbox("📅 Jahr", options=available_years)

months_in_year = sorted(date_df[date_df['summary_date'].apply(lambda d: d.year) == selected_year]['summary_date'].apply(lambda d: d.month).unique())
selected_month = st.sidebar.selectbox("📅 Monat", options=months_in_year, format_func=lambda m: f"{m:02d}")

days_in_month = sorted(date_df[
    (date_df['summary_date'].apply(lambda d: d.year) == selected_year) &
    (date_df['summary_date'].apply(lambda d: d.month) == selected_month)
]['summary_date'].apply(lambda d: d.day).unique())
selected_day = st.sidebar.selectbox("📅 Tag", options=days_in_month, format_func=lambda d: f"{d:02d}")

selected_date = datetime.date(selected_year, selected_month, selected_day)
st.sidebar.success(f"Zeige Daten für: {selected_date}")

summary_df = load_summary_data(conn, selected_date)

available_machines = summary_df['machine_id'].unique()
selected_machines = st.sidebar.multiselect("🛠️ Maschinen auswählen", options=available_machines, default=available_machines)
summary_df = summary_df[summary_df['machine_id'].isin(selected_machines)]


if summary_df.empty:
    st.info(f"Keine Daten für das ausgewählte Datum ({selected_date}) vorhanden.")
else:
    st.header("Übersicht der stündlichen Aggregate")

    display_columns_map = {
        "timestamp_hour": "Zeitstempel",
        "machine_id": "Maschine",
        "cycle_count": "Zyklen",
        "min_cycle_time_seconds": "Min Zeit (s)",
        "avg_cycle_time_seconds": "Avg Zeit (s)",
        "max_cycle_time_seconds": "Max Zeit (s)",
        "as_vacuum_error_count": "AS Vac Fehler",
        "pp_vacuum_error_count": "PP Vac Fehler",
        "as_release_error_count": "AS Blow Fehler",
        "pp_release_error_count": "PP Blow Fehler",
        "pick_force_error_count": "Pick F Fehler", 
        "place_force_error_count": "Place F Fehler",
        "avg_pick_force": "Avg Pick F (g)",
        "avg_place_force": "Avg Place F (g)"
    }
    display_df = summary_df[list(display_columns_map.keys())].copy()
    display_df = display_df.rename(columns=display_columns_map)
	
    st.dataframe(display_df.sort_values(by="Zeitstempel", ascending=True), use_container_width=True, hide_index=True)

    st.header("Zeitreihenanalyse")
  
    st.subheader("Fehleranalyse")
    error_columns = [
        'as_vacuum_error_count', 'pp_vacuum_error_count',
        'as_release_error_count', 'pp_release_error_count',
        'pick_force_error_count', 'place_force_error_count'
    ]
    error_legend_names = {
        col: display_columns_map.get(col, col)
        for col in error_columns
    }
    fig_error = create_timeseries_plot(
        summary_df,
        x_col='timestamp_hour',
        y_cols=error_columns,
        title='Fehleranzahl über Zeit',
        y_axis_title='Anzahl',
        custom_names=error_legend_names
    )
    st.plotly_chart(fig_error, use_container_width=True)

    st.subheader("Zykluszeiten")
    cycle_time_columns = [
        'min_cycle_time_seconds',
        'avg_cycle_time_seconds',
        'max_cycle_time_seconds'
    ]
    cycle_time_legend_names = {
         col: display_columns_map.get(col, col)
         for col in cycle_time_columns
    }
    fig_cycle = create_timeseries_plot(
        summary_df,
        x_col='timestamp_hour',
        y_cols=cycle_time_columns,
        title='Zykluszeit (min/avg/max) über Zeit',
        y_axis_title='Zykluszeit (s)',
        custom_names=cycle_time_legend_names
    )
    st.plotly_chart(fig_cycle, use_container_width=True)

st.caption("Dashboard Ende.")