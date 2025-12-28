-- PostgreSQL Database Schema for SkyLogix Weather Data Warehouse
-- File: sql/create_tables.sql

-- Create database (run as superuser)
-- CREATE DATABASE skylogix_warehouse;

-- Connect to the database
-- \c skylogix_warehouse

-- Create weather_readings table
CREATE TABLE IF NOT EXISTS weather_readings (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10) NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    lat NUMERIC(10, 6),
    lon NUMERIC(10, 6),
    temp_c NUMERIC(5, 2),
    feels_like_c NUMERIC(5, 2),
    pressure_hpa INTEGER,
    humidity_pct INTEGER,
    wind_speed_ms NUMERIC(5, 2),
    wind_deg INTEGER,
    cloud_pct INTEGER,
    visibility_m INTEGER,
    rain_1h_mm NUMERIC(5, 2) DEFAULT 0.0,
    snow_1h_mm NUMERIC(5, 2) DEFAULT 0.0,
    condition_main VARCHAR(50),
    condition_description VARCHAR(255),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint to prevent duplicate readings for same city at same time
    CONSTRAINT unique_city_observation UNIQUE (city, observed_at)
);

-- Create indexes for optimal query performance
CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_readings(city);
CREATE INDEX IF NOT EXISTS idx_weather_observed_at ON weather_readings(observed_at);
CREATE INDEX IF NOT EXISTS idx_weather_city_observed ON weather_readings(city, observed_at);
CREATE INDEX IF NOT EXISTS idx_weather_condition ON weather_readings(condition_main);
CREATE INDEX IF NOT EXISTS idx_weather_ingested_at ON weather_readings(ingested_at);

-- Create index for time-series queries
CREATE INDEX IF NOT EXISTS idx_weather_city_time_range ON weather_readings(city, observed_at DESC);

-- Comments for documentation
COMMENT ON TABLE weather_readings IS 'Real-time weather data for SkyLogix operational cities';
COMMENT ON COLUMN weather_readings.city IS 'City name (Nairobi, Lagos, Accra, Johannesburg)';
COMMENT ON COLUMN weather_readings.observed_at IS 'Time when weather was observed (from API)';
COMMENT ON COLUMN weather_readings.temp_c IS 'Temperature in Celsius';
COMMENT ON COLUMN weather_readings.feels_like_c IS 'Feels like temperature in Celsius';
COMMENT ON COLUMN weather_readings.pressure_hpa IS 'Atmospheric pressure in hPa';
COMMENT ON COLUMN weather_readings.humidity_pct IS 'Humidity percentage';
COMMENT ON COLUMN weather_readings.wind_speed_ms IS 'Wind speed in meters per second';
COMMENT ON COLUMN weather_readings.wind_deg IS 'Wind direction in degrees';
COMMENT ON COLUMN weather_readings.cloud_pct IS 'Cloud coverage percentage';
COMMENT ON COLUMN weather_readings.visibility_m IS 'Visibility in meters';
COMMENT ON COLUMN weather_readings.rain_1h_mm IS 'Rain volume in last hour (mm)';
COMMENT ON COLUMN weather_readings.snow_1h_mm IS 'Snow volume in last hour (mm)';
COMMENT ON COLUMN weather_readings.ingested_at IS 'Time when record was loaded into warehouse';

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE ON weather_readings TO airflow_user;
-- GRANT USAGE, SELECT ON SEQUENCE weather_readings_id_seq TO airflow_user;