CREATE TABLE weather_data (
    time        TIMESTAMP WITH TIME ZONE NOT NULL,
    location    TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity    DOUBLE PRECISION,
    pressure    DOUBLE PRECISION,
    PRIMARY KEY (time, location)
);

SELECT create_hypertable('weather_data', 'time');
