CREATE TABLE IF NOT EXISTS sensor_data (
    id INTEGER NOT NULL,
    velocity FLOAT NULL,
    temperature FLOAT NULL,
    humidity FLOAT NULL,
    last_seen TIMESTAMP NOT NULL,
    battery_level FLOAT NULL,
    PRIMARY KEY (id, last_seen)
);


SELECT create_hypertable('sensor_data', by_range('last_seen'), if_not_exists => TRUE);