# SkyLogix Weather Data Pipeline

A real-time weather data pipeline for SkyLogix Transportation, ingesting weather data from OpenWeatherMap API, storing raw data in MongoDB, and loading transformed analytics data into PostgreSQL, all orchestrated by Apache Airflow.

## Overview

SkyLogix Transportation operates delivery fleets across four major African cities:
- **Nairobi, Kenya** (KE)
- **Lagos, Nigeria** (NG)
- **Accra, Ghana** (GH)
- **Johannesburg, South Africa** (ZA)

This pipeline provides real-time weather intelligence to optimize routing, minimize risks, and improve operational decisions.

## Architecture

```
OpenWeatherMap API
        ↓
  Python Ingestor
        ↓
   MongoDB (Raw)
        ↓
  Transformation
        ↓
  PostgreSQL (Analytics)
        ↓
  Dashboards & Reports
```

**Orchestration:** Apache Airflow DAG runs every 15 minutes

## Project Structure

```
skylogix-weather-pipeline/
├── config/
│   └── __init__.py            
│   └── config.py              # Configuration settings
├── scripts/
│   └── __init__.py             
│   ├── ingest_weather.py      # API → MongoDB ingestion
│   └── transform_load.py      # MongoDB → PostgreSQL ETL
├── dags/
│   └── weather_pipeline_dag.py # Airflow orchestration
├── sql/
│   ├── create_tables.sql      # PostgreSQL schema
│   └── analytics_queries.sql  # Sample queries
├── .env               # Environment variables
├── requirements.txt           # Python dependencies
├── setup.sh                   # Setup script
└── README.md                  # This file
```

## Quick Start

### Prerequisites

- Python 3.8+
- MongoDB 4.4+
- PostgreSQL 12+
- OpenWeatherMap API key (free tier available)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/skylogix/weather-pipeline.git
cd weather-pipeline
```

2. **Run setup script**
```bash
chmod +x setup.sh
./setup.sh
```

3. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your credentials
nano .env
```

4. **Get OpenWeatherMap API Key**
- Visit https://openweathermap.org/api
- Sign up for a free account
- Copy your API key to `.env`

### Manual Setup (Alternative)

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create PostgreSQL tables
psql -U postgres -d skylogix_warehouse -f sql/create_tables.sql
```

## Configuration

### Environment Variables (.env)

```bash
# OpenWeatherMap API
OPENWEATHER_API_KEY=your_api_key_here

# MongoDB
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE=skylogix_weather

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=skylogix_warehouse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

### Target Cities

Configured in `config/config.py`:
```python
TARGET_CITIES = [
    {"city": "Nairobi", "country": "KE"},
    {"city": "Lagos", "country": "NG"},
    {"city": "Accra", "country": "GH"},
    {"city": "Johannesburg", "country": "ZA"}
]
```

## Running the Pipeline

### Manual Execution

**1. Ingest Weather Data (API → MongoDB)**
```bash
python3 scripts/ingest_weather.py
```

**2. Transform and Load (MongoDB → PostgreSQL)**
```bash
python3 scripts/transform_load.py
```

### Airflow Orchestration

**1. Start Airflow**
```bash
# Set Airflow home
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize database (first time only)
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@skylogix.com

# Start webserver
airflow webserver -p 8080

# In another terminal, start scheduler
airflow scheduler
```

**2. Access Airflow UI**
- Open http://localhost:8080
- Login with your credentials
- Enable the `weather_data_pipeline` DAG

**3. DAG Configuration**
- **Schedule:** Every 15 minutes (`*/15 * * * *`)
- **Tasks:**
  1. `task_fetch_and_upsert_raw` - Fetch from API, upsert to MongoDB
  2. `task_transform_and_load_postgres` - Transform and load to PostgreSQL
  3. `task_validate_pipeline` - Validate execution

## Data Models

### MongoDB Collection: `weather_raw`

Stores raw JSON responses from OpenWeatherMap API.

**Key Fields:**
- `_id`: MongoDB ObjectId
- `id`: OpenWeatherMap city ID
- `dt`: Unix timestamp
- `name`: City name
- `coord`, `main`, `weather`, `wind`, `clouds`, `rain`, `snow`
- `updatedAt`: Timestamp when record was updated

### PostgreSQL Table: `weather_readings`

Normalized, analytics-ready weather data.

**Schema:**
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| city | VARCHAR | City name |
| country | VARCHAR | Country code |
| observed_at | TIMESTAMP | Observation time |
| lat | NUMERIC | Latitude |
| lon | NUMERIC | Longitude |
| temp_c | NUMERIC | Temperature (°C) |
| feels_like_c | NUMERIC | Feels like (°C) |
| pressure_hpa | INTEGER | Pressure (hPa) |
| humidity_pct | INTEGER | Humidity (%) |
| wind_speed_ms | NUMERIC | Wind speed (m/s) |
| wind_deg | INTEGER | Wind direction |
| cloud_pct | INTEGER | Cloud cover (%) |
| visibility_m | INTEGER | Visibility (m) |
| rain_1h_mm | NUMERIC | Rain (mm) |
| snow_1h_mm | NUMERIC | Snow (mm) |
| condition_main | VARCHAR | Main condition |
| condition_description | VARCHAR | Description |
| ingested_at | TIMESTAMP | Load timestamp |

**Indexes:**
- `(city, observed_at)` - Time-series queries
- `city` - City filtering
- `observed_at` - Time range queries

## Analytics Queries

### Current Weather Conditions
```sql
SELECT city, temp_c, humidity_pct, wind_speed_ms, condition_main
FROM weather_readings
WHERE observed_at = (SELECT MAX(observed_at) FROM weather_readings)
ORDER BY city;
```

### Extreme Weather Detection
```sql
SELECT city, observed_at, wind_speed_ms, rain_1h_mm, visibility_m
FROM weather_readings
WHERE wind_speed_ms > 15 OR rain_1h_mm > 5 OR visibility_m < 1000
ORDER BY observed_at DESC;
```

### Temperature Trends
```sql
SELECT city, DATE_TRUNC('hour', observed_at) as hour, AVG(temp_c) as avg_temp
FROM weather_readings
WHERE observed_at >= NOW() - INTERVAL '24 hours'
GROUP BY city, hour
ORDER BY city, hour;
```

More examples in `sql/analytics_queries.sql`

## Monitoring & Validation

### Pipeline Health Checks

The Airflow DAG includes validation tasks that check:
- **Ingestion Success Rate:** Must be ≥75%
- **Transformation Success Rate:** Must be ≥90%
- **Data Freshness:** Latest reading within 30 minutes

### Logging

All scripts use Python's logging module:
```python
import logging
logger = logging.getLogger(__name__)
```

Logs include:
- INFO: Normal operations
- WARNING: Degraded performance
- ERROR: Failures requiring attention

### XCom Variables

Airflow tasks push metrics to XCom:
- `ingestion_results`: API fetch statistics
- `etl_summary`: Transformation statistics
- `validation_results`: Pipeline health status

## 🔗 Integration with Logistics Data

### Example: Weather-Aware Route Optimization

```sql
-- Correlate trips with weather conditions
SELECT 
    t.trip_id,
    t.origin_city,
    t.scheduled_departure,
    t.delay_minutes,
    w.rain_1h_mm,
    w.wind_speed_ms,
    w.visibility_m
FROM trips t
LEFT JOIN weather_readings w 
    ON t.origin_city = w.city 
    AND DATE_TRUNC('hour', t.scheduled_departure) = DATE_TRUNC('hour', w.observed_at)
WHERE t.delay_minutes > 30;
```

### Risk Levels

**High Risk:** rain > 5mm/h OR wind > 15 m/s OR visibility < 1km
**Moderate Risk:** rain > 2mm/h OR wind > 10 m/s
**Low Risk:** Normal conditions

## Troubleshooting

### MongoDB Connection Issues
```bash
# Check MongoDB status
sudo systemctl status mongod

# Restart MongoDB
sudo systemctl restart mongod

# Test connection
mongo --eval "db.version()"
```

### PostgreSQL Connection Issues
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Test connection
psql -U postgres -c "SELECT version();"
```

### API Rate Limits

OpenWeatherMap free tier allows:
- 60 calls/minute
- 1,000,000 calls/month

Current setup: 4 cities × 4 calls/hour = 16 calls/hour = 11,520 calls/month ✓

### Common Errors

**"API key is invalid"**
- Verify your API key in `.env`
- Ensure the key is activated (may take a few hours)

**"pymongo.errors.ServerSelectionTimeoutError"**
- Check MongoDB is running
- Verify `MONGODB_URI` in `.env`

**"psycopg2.OperationalError"**
- Check PostgreSQL is running
- Verify database exists: `createdb skylogix_warehouse`

## Development

### Adding New Cities

Edit `config/config.py`:
```python
TARGET_CITIES.append({"city": "Cape Town", "country": "ZA"})
```

### Adjusting Schedule

Edit `dags/weather_pipeline_dag.py`:
```python
schedule_interval='*/30 * * * *'  # Every 30 minutes
```

### Custom Transformations

Modify `transform_document()` in `scripts/transform_load.py`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is proprietary to SkyLogix Transportation (Project_name).

## 👥 Contact

**Data Engineering Team**
- Email: ukosarahiniobong@gmail.com
- Slack: #data-engineering

## 🔄 Version History

- **v1.0.0** (2024-12) - Initial release
  - OpenWeatherMap integration
  - MongoDB staging layer
  - PostgreSQL warehouse
  - Airflow orchestration
