CREATE TABLE IF NOT EXISTS aircraft_states (
    hex VARCHAR(10) NOT NULL,
    dag_id VARCHAR(255) NOT NULL,
    registration VARCHAR(50),
    callsign VARCHAR(50),
    status VARCHAR(50),
    last_seen_time TIMESTAMP,
    last_alert_time TIMESTAMP,
    PRIMARY KEY (hex, dag_id)
);
