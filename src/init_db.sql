DROP TABLE IF EXISTS hourly_machine_summary;
DROP TABLE IF EXISTS processed_machine_events;

CREATE TABLE processed_machine_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    machine_id VARCHAR(50) NOT NULL,
    event_name VARCHAR(50) NOT NULL,
    parameter_name VARCHAR(50) NULL,
    value FLOAT NULL,
    is_error INT CHECK (is_error IN (0, 1)) NOT NULL,
    cycle_seq BIGINT NOT NULL,
	cycle_time_seconds FLOAT NULL
);

-- Indizes
CREATE INDEX idx_processed_events_time ON processed_machine_events (event_timestamp);
CREATE INDEX idx_processed_events_machine_param ON processed_machine_events (machine_id, parameter_name, event_timestamp);
CREATE INDEX idx_processed_events_cycle ON processed_machine_events (machine_id, cycle_seq);

CREATE TABLE hourly_machine_summary (
    summary_date DATE NOT NULL,
    hour_of_day INT NOT NULL CHECK (hour_of_day >= 0 AND hour_of_day <= 23),
    machine_id VARCHAR(50) NOT NULL,
    avg_pick_force FLOAT,
    max_pick_force FLOAT,
	min_pick_force FLOAT,
    avg_place_force FLOAT,
    max_place_force FLOAT,
	min_place_force FLOAT,
    as_vacuum_error_count INT,
    pp_vacuum_error_count INT,
    as_release_error_count INT,
    pp_release_error_count INT,
    pick_force_error_count INT,
    place_force_error_count INT,
    cycle_count INT,
    avg_cycle_time_seconds FLOAT,
    PRIMARY KEY (summary_date, hour_of_day, machine_id)
);

-- Indizes
CREATE INDEX idx_hourly_summary_time_agg ON hourly_machine_summary (summary_date, hour_of_day);
CREATE INDEX idx_hourly_summary_machine_agg ON hourly_machine_summary (machine_id);

COMMENT ON TABLE processed_machine_events IS 'Einzelne Maschinen-Events nach minimaler Bereinigung, Fehlerprüfung und Anreicherung um Zyklus-Sequenz.';
COMMENT ON TABLE hourly_machine_summary IS 'Stündlich aggregierte Kennzahlen und Fehlerzählungen für DieBonder Maschinen-Events.';