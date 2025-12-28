# Replace your old imports with these
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PythonOperator
import pendulum
import logging
import sys
import os

# ... path config remains the same ...

# Import pipeline functions
from scripts.ingest_weather import WeatherIngestor
from scripts.transform_load import WeatherETL

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'skylogix-data-team',
    'depends_on_past': False,
    'email': ['data-eng@skylogix.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='Real-time weather data pipeline for SkyLogix operations',
    schedule='*/15 * * * *',
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'etl', 'skylogix', 'production'],
)

# DAG definition - UPDATED FOR AIRFLOW 3.0

def fetch_and_upsert_weather(**context):
    """
    Task function to fetch weather data from OpenWeatherMap API
    and upsert into MongoDB raw collection
    """
    logger.info("Starting weather data ingestion task")
    ingestor = None
    
    try:
        ingestor = WeatherIngestor()
        results = ingestor.ingest_all_cities()
        
        # Push results to XCom for monitoring
        context['task_instance'].xcom_push(key='ingestion_results', value=results)
        
        logger.info(f"Ingestion completed: {results}")
        
        if results['failed'] > 0:
            logger.warning(f"{results['failed']} cities failed to ingest")
            # Don't fail the task if at least some cities succeeded
            if results['successful'] == 0:
                raise Exception("All cities failed to ingest")
        
        return results
        
    except Exception as e:
        logger.error(f"Weather ingestion task failed: {e}")
        raise
    finally:
        if ingestor:
            ingestor.close()


def transform_and_load_weather(**context):
    """
    Task function to read from MongoDB, transform data,
    and load into PostgreSQL warehouse
    """
    logger.info("Starting weather data transformation and loading task")
    etl = None
    
    try:
        # Get last execution time from previous run
        # This ensures we only process new/updated documents
        execution_date = context['execution_date']
        last_run_time = execution_date - timedelta(minutes=20)  # Small buffer
        
        etl = WeatherETL(last_run_time=last_run_time)
        summary = etl.run_etl()
        
        # Push results to XCom for monitoring
        context['task_instance'].xcom_push(key='etl_summary', value=summary)
        
        logger.info(f"ETL completed: {summary}")
        
        if summary['failed'] > 0:
            logger.warning(f"{summary['failed']} documents failed to transform")
        
        return summary
        
    except Exception as e:
        logger.error(f"Weather ETL task failed: {e}")
        raise
    finally:
        if etl:
            etl.close()


def validate_pipeline(**context):
    """
    Task function to validate pipeline execution
    Checks ingestion and ETL results
    """
    logger.info("Validating pipeline execution")
    
    try:
        # Pull results from previous tasks
        ti = context['task_instance']
        ingestion_results = ti.xcom_pull(
            task_ids='task_fetch_and_upsert_raw',
            key='ingestion_results'
        )
        etl_summary = ti.xcom_pull(
            task_ids='task_transform_and_load_postgres',
            key='etl_summary'
        )
        
        # Validation checks
        validation_results = {
            'pipeline_status': 'success',
            'checks': []
        }
        
        # Check 1: Ingestion success rate
        if ingestion_results:
            success_rate = (ingestion_results['successful'] / 
                          ingestion_results['total']) * 100
            check = {
                'name': 'ingestion_success_rate',
                'value': success_rate,
                'status': 'pass' if success_rate >= 75 else 'fail'
            }
            validation_results['checks'].append(check)
            
            if success_rate < 75:
                validation_results['pipeline_status'] = 'degraded'
        
        # Check 2: ETL processing
        if etl_summary:
            if etl_summary['fetched'] > 0:
                transform_rate = (etl_summary['transformed'] / 
                                etl_summary['fetched']) * 100
                check = {
                    'name': 'transformation_success_rate',
                    'value': transform_rate,
                    'status': 'pass' if transform_rate >= 90 else 'fail'
                }
                validation_results['checks'].append(check)
                
                if transform_rate < 90:
                    validation_results['pipeline_status'] = 'degraded'
        
        logger.info(f"Pipeline validation: {validation_results}")
        
        # Push validation results
        ti.xcom_push(key='validation_results', value=validation_results)
        
        # Fail if pipeline is in bad state
        if validation_results['pipeline_status'] == 'fail':
            raise Exception("Pipeline validation failed")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Pipeline validation failed: {e}")
        raise


# Define tasks
task_fetch_and_upsert = PythonOperator(
    task_id='task_fetch_and_upsert_raw',
    python_callable=fetch_and_upsert_weather,
    dag=dag,
)

task_transform_and_load = PythonOperator(
    task_id='task_transform_and_load_postgres',
    python_callable=transform_and_load_weather,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='task_validate_pipeline',
    python_callable=validate_pipeline,
    dag=dag,
)

# Set task dependencies
task_fetch_and_upsert >> task_transform_and_load >> task_validate