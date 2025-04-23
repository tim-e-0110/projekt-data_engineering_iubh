DROP TABLE IF EXISTS hourly_machine_summary;
DROP TABLE IF EXISTS processed_machine_events;

CREATE TABLE processed_machine_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    machine_id VARCHAR(50) NOT NULL,
    event_name VARCHAR(50) NOT NULL,
    parameter_name VARCHAR(50) NULL,
    value NUMERIC(7, 2) NULL,
    is_error INT CHECK (is_error IN (0, 1)) NOT NULL,
    cycle_seq BIGINT NOT NULL,
    cycle_time_seconds NUMERIC(10, 3) NULL
);

-- Indizes
CREATE INDEX idx_processed_events_time ON processed_machine_events (event_timestamp);
CREATE INDEX idx_processed_events_machine_param ON processed_machine_events (machine_id, parameter_name, event_timestamp);
CREATE INDEX idx_processed_events_cycle ON processed_machine_events (machine_id, cycle_seq);

CREATE TABLE hourly_machine_summary (
    summary_date DATE NOT NULL,
    hour_of_day INT NOT NULL CHECK (hour_of_day >= 0 AND hour_of_day <= 23),
    machine_id VARCHAR(50) NOT NULL,
    avg_pick_force NUMERIC(7, 2),
    max_pick_force NUMERIC(7, 2),
    min_pick_force NUMERIC(7, 2),
    avg_place_force NUMERIC(7, 2),
    max_place_force NUMERIC(7, 2),
    min_place_force NUMERIC(7, 2),
    as_vacuum_error_count INT,
    pp_vacuum_error_count INT,
    as_release_error_count INT,
    pp_release_error_count INT,
    pick_force_error_count INT,
    place_force_error_count INT,
    cycle_count INT,
    min_cycle_time_seconds NUMERIC(10, 3),
	max_cycle_time_seconds NUMERIC(10, 3),
	avg_cycle_time_seconds NUMERIC(10, 3),
    PRIMARY KEY (summary_date, hour_of_day, machine_id)
);

-- Indizes
CREATE INDEX idx_hourly_summary_time_agg ON hourly_machine_summary (summary_date, hour_of_day);
CREATE INDEX idx_hourly_summary_machine_agg ON hourly_machine_summary (machine_id);

COMMENT ON TABLE processed_machine_events IS 'Einzelne Maschinen-Events nach minimaler Bereinigung, Fehlerprüfung und Anreicherung um Zyklus-Sequenz.';
COMMENT ON TABLE hourly_machine_summary IS 'Stündlich aggregierte Kennzahlen und Fehlerzählungen für DieBonder Maschinen-Events.';