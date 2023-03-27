CREATE TABLE weather (
  sensorId TEXT NOT NULL,
  location TEXT NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  temperature DOUBLE PRECISION NOT NULL,
  humidity DOUBLE PRECISION NOT NULL,
  windSpeed DOUBLE PRECISION NOT NULL,
  windDirection TEXT NOT NULL
);

SELECT create_hypertable('weather', 'timestamp');
