import requests
import logging
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
import sys
import os

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    OPENWEATHER_API_KEY,
    OPENWEATHER_BASE_URL,
    TARGET_CITIES,
    MONGODB_URI,
    MONGODB_DATABASE,
    MONGODB_COLLECTION
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WeatherIngestor:
    """Handles fetching and upserting weather data to MongoDB"""
    
    def __init__(self):
        """Initialize MongoDB connection"""
        try:
            self.client = MongoClient(MONGODB_URI)
            self.db = self.client[MONGODB_DATABASE]
            self.collection = self.db[MONGODB_COLLECTION]
            self._create_indexes()
            logger.info("MongoDB connection established successfully")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """Create necessary indexes on the collection"""
        try:
            self.collection.create_index([("city", ASCENDING)])
            self.collection.create_index([("updatedAt", ASCENDING)])
            self.collection.create_index([("id", ASCENDING), ("dt", ASCENDING)], unique=True)
            logger.info("Indexes created successfully")
        except PyMongoError as e:
            logger.warning(f"Index creation warning: {e}")
    
    def fetch_weather_data(self, city, country):
        """
        Fetch weather data from OpenWeatherMap API
        
        Args:
            city (str): City name
            country (str): Country code
            
        Returns:
            dict: Weather data or None if failed
        """
        params = {
            "q": f"{city},{country}",
            "appid": OPENWEATHER_API_KEY,
            "units": "metric"
        }
        
        try:
            response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched weather data for {city}, {country}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather data for {city}, {country}: {e}")
            return None
    
    def upsert_raw_data(self, raw_data):
        """
        Upsert raw weather data into MongoDB
        
        Args:
            raw_data (dict): Raw JSON response from API
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not raw_data:
            return False
        
        try:
            # Add metadata
            raw_data["updatedAt"] = datetime.now()
            raw_data["city"] = raw_data.get("name")
            
            # Use id and dt as composite key for upsert
            filter_query = {
                "id": raw_data.get("id"),
                "dt": raw_data.get("dt")
            }
            
            # Upsert the document
            result = self.collection.update_one(
                filter_query,
                {"$set": raw_data},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Inserted new document for {raw_data.get('name')}")
            else:
                logger.info(f"Updated existing document for {raw_data.get('name')}")
            
            return True
        except PyMongoError as e:
            logger.error(f"Failed to upsert data: {e}")
            return False
    
    def ingest_all_cities(self):
        """
        Fetch and upsert weather data for all target cities
        
        Returns:
            dict: Summary of ingestion results
        """
        results = {
            "successful": 0,
            "failed": 0,
            "total": len(TARGET_CITIES)
        }
        
        for city_info in TARGET_CITIES:
            city = city_info["city"]
            country = city_info["country"]
            
            logger.info(f"Processing {city}, {country}...")
            raw_data = self.fetch_weather_data(city, country)
            
            if self.upsert_raw_data(raw_data):
                results["successful"] += 1
            else:
                results["failed"] += 1
        
        logger.info(f"Ingestion complete. Success: {results['successful']}, Failed: {results['failed']}")
        return results
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


def main():
    """Main execution function"""
    ingestor = None
    try:
        ingestor = WeatherIngestor()
        results = ingestor.ingest_all_cities()
        
        if results["failed"] > 0:
            logger.warning(f"{results['failed']} cities failed to ingest")
            sys.exit(1)
        else:
            logger.info("All cities ingested successfully")
            sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error during ingestion: {e}")
        sys.exit(1)
    finally:
        if ingestor:
            ingestor.close()


if __name__ == "__main__":
    main()