import logging
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
import sys
import os

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    MONGODB_URI,
    MONGODB_DATABASE,
    MONGODB_COLLECTION,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DATABASE,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_TABLE,
    PROVIDER_NAME
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WeatherETL:
    """Handles ETL from MongoDB to PostgreSQL"""
    
    def __init__(self, last_run_time=None):
        """
        Initialize connections to MongoDB and PostgreSQL
        
        Args:
            last_run_time (datetime): Only process documents updated after this time
        """
        self.last_run_time = last_run_time or (datetime.now() - timedelta(hours=1))
        self.mongo_client = None
        self.pg_conn = None
        self._connect_mongodb()
        self._connect_postgres()
    
    def _connect_mongodb(self):
        """Establish MongoDB connection"""
        try:
            self.mongo_client = MongoClient(MONGODB_URI)
            self.mongo_db = self.mongo_client[MONGODB_DATABASE]
            self.mongo_collection = self.mongo_db[MONGODB_COLLECTION]
            logger.info("MongoDB connection established")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _connect_postgres(self):
        """Establish PostgreSQL connection"""
        try:
            self.pg_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DATABASE,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            self.pg_conn.autocommit = False
            logger.info("PostgreSQL connection established")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def fetch_raw_documents(self):
        """
        Fetch raw weather documents from MongoDB that need processing
        
        Returns:
            list: List of raw weather documents
        """
        try:
            query = {"updatedAt": {"$gte": self.last_run_time}}
            documents = list(self.mongo_collection.find(query))
            logger.info(f"Fetched {len(documents)} documents from MongoDB")
            return documents
        except PyMongoError as e:
            logger.error(f"Failed to fetch documents from MongoDB: {e}")
            return []
    
    def transform_document(self, raw_doc):
        """
        Transform raw weather document into structured format
        
        Args:
            raw_doc (dict): Raw weather document from MongoDB
            
        Returns:
            dict: Transformed weather data
        """
        try:
            # Extract nested data with safe defaults
            coord = raw_doc.get("coord", {})
            main = raw_doc.get("main", {})
            wind = raw_doc.get("wind", {})
            clouds = raw_doc.get("clouds", {})
            rain = raw_doc.get("rain", {})
            snow = raw_doc.get("snow", {})
            weather_list = raw_doc.get("weather", [])
            first_weather = weather_list[0] if weather_list else {}
            sys_info = raw_doc.get("sys", {})
            
            # Convert Unix timestamp to datetime
            observed_at = datetime.fromtimestamp(raw_doc.get("dt", 0))
            
            transformed = {
                "city": raw_doc.get("name"),
                "country": sys_info.get("country"),
                "observed_at": observed_at,
                "lat": coord.get("lat"),
                "lon": coord.get("lon"),
                "temp_c": main.get("temp"),
                "feels_like_c": main.get("feels_like"),
                "pressure_hpa": main.get("pressure"),
                "humidity_pct": main.get("humidity"),
                "wind_speed_ms": wind.get("speed"),
                "wind_deg": wind.get("deg"),
                "cloud_pct": clouds.get("all"),
                "visibility_m": raw_doc.get("visibility"),
                "rain_1h_mm": rain.get("1h", 0.0),
                "snow_1h_mm": snow.get("1h", 0.0),
                "condition_main": first_weather.get("main"),
                "condition_description": first_weather.get("description"),
                "ingested_at": datetime.utcnow()
            }
            
            return transformed
        except Exception as e:
            logger.error(f"Failed to transform document {raw_doc.get('id')}: {e}")
            return None
    
    def load_to_postgres(self, transformed_data):
        """
        Load transformed data into PostgreSQL
        
        Args:
            transformed_data (list): List of transformed weather records
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not transformed_data:
            logger.warning("No data to load into PostgreSQL")
            return False
        
        insert_query = sql.SQL("""
            INSERT INTO {table} (
                city, country, observed_at, lat, lon,
                temp_c, feels_like_c, pressure_hpa, humidity_pct,
                wind_speed_ms, wind_deg, cloud_pct, visibility_m,
                rain_1h_mm, snow_1h_mm, condition_main, condition_description,
                ingested_at
            ) VALUES %s
            ON CONFLICT (city, observed_at) 
            DO UPDATE SET
                temp_c = EXCLUDED.temp_c,
                feels_like_c = EXCLUDED.feels_like_c,
                pressure_hpa = EXCLUDED.pressure_hpa,
                humidity_pct = EXCLUDED.humidity_pct,
                wind_speed_ms = EXCLUDED.wind_speed_ms,
                wind_deg = EXCLUDED.wind_deg,
                cloud_pct = EXCLUDED.cloud_pct,
                visibility_m = EXCLUDED.visibility_m,
                rain_1h_mm = EXCLUDED.rain_1h_mm,
                snow_1h_mm = EXCLUDED.snow_1h_mm,
                condition_main = EXCLUDED.condition_main,
                condition_description = EXCLUDED.condition_description,
                ingested_at = EXCLUDED.ingested_at
        """).format(table=sql.Identifier(POSTGRES_TABLE))
        
        # Prepare values for bulk insert
        values = [
            (
                record["city"],
                record["country"],
                record["observed_at"],
                record["lat"],
                record["lon"],
                record["temp_c"],
                record["feels_like_c"],
                record["pressure_hpa"],
                record["humidity_pct"],
                record["wind_speed_ms"],
                record["wind_deg"],
                record["cloud_pct"],
                record["visibility_m"],
                record["rain_1h_mm"],
                record["snow_1h_mm"],
                record["condition_main"],
                record["condition_description"],
                record["ingested_at"]
            )
            for record in transformed_data
        ]
        
        try:
            cursor = self.pg_conn.cursor()
            execute_values(cursor, insert_query, values)
            self.pg_conn.commit()
            cursor.close()
            logger.info(f"Successfully loaded {len(values)} records into PostgreSQL")
            return True
        except psycopg2.Error as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to load data into PostgreSQL: {e}")
            return False
    
    def run_etl(self):
        """
        Execute the complete ETL process
        
        Returns:
            dict: ETL execution summary
        """
        summary = {
            "fetched": 0,
            "transformed": 0,
            "loaded": 0,
            "failed": 0
        }
        
        # Fetch raw documents
        raw_documents = self.fetch_raw_documents()
        summary["fetched"] = len(raw_documents)
        
        if not raw_documents:
            logger.info("No new documents to process")
            return summary
        
        # Transform documents
        transformed_data = []
        for raw_doc in raw_documents:
            transformed = self.transform_document(raw_doc)
            if transformed:
                transformed_data.append(transformed)
                summary["transformed"] += 1
            else:
                summary["failed"] += 1
        
        # Load to PostgreSQL
        if self.load_to_postgres(transformed_data):
            summary["loaded"] = len(transformed_data)
        
        logger.info(f"ETL Summary - Fetched: {summary['fetched']}, "
                   f"Transformed: {summary['transformed']}, "
                   f"Loaded: {summary['loaded']}, "
                   f"Failed: {summary['failed']}")
        
        return summary
    
    def close(self):
        """Close database connections"""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("PostgreSQL connection closed")


def main(last_run_time=None):
    """
    Main execution function
    
    Args:
        last_run_time (datetime): Process documents updated after this time
    """
    etl = None
    try:
        etl = WeatherETL(last_run_time)
        summary = etl.run_etl()
        
        if summary["failed"] > 0:
            logger.warning(f"{summary['failed']} documents failed to transform")
            sys.exit(1)
        else:
            logger.info("ETL completed successfully")
            sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error during ETL: {e}")
        sys.exit(1)
    finally:
        if etl:
            etl.close()


if __name__ == "__main__":
    main()