"""
Dagster Pipeline for Telegram Health Data Platform

This pipeline orchestrates the entire data workflow:
1. Scrape Telegram data
2. Load raw data to PostgreSQL
3. Run dbt transformations
4. Run YOLO enrichment
"""

import os
import subprocess
import logging
from pathlib import Path
from typing import List
from dagster import (
    job,
    op,
    OpExecutionContext,
    get_dagster_logger,
    DefaultSensorStatus,
    schedule,
    ScheduleEvaluationContext,
    DefaultScheduleStatus,
)
from dotenv import load_dotenv

load_dotenv()

logger = get_dagster_logger()


@op
def scrape_telegram_data(context: OpExecutionContext) -> str:
    """
    Scrape data from Telegram channels.
    
    Returns:
        Status message
    """
    context.log.info("Starting Telegram data scraping...")
    
    try:
        result = subprocess.run(
            ["python", "src/scraper.py"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path.cwd()
        )
        
        context.log.info(f"Scraping completed successfully")
        context.log.info(result.stdout)
        
        return "Telegram scraping completed successfully"
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Scraping failed: {e.stderr}")
        raise Exception(f"Scraping failed: {e.stderr}")


@op
def load_raw_to_postgres(context: OpExecutionContext) -> str:
    """
    Load raw JSON data from data lake to PostgreSQL.
    
    Returns:
        Status message
    """
    context.log.info("Loading raw data to PostgreSQL...")
    
    try:
        result = subprocess.run(
            ["python", "src/load_to_postgres.py"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path.cwd()
        )
        
        context.log.info(f"Data loading completed successfully")
        context.log.info(result.stdout)
        
        return "Data loaded to PostgreSQL successfully"
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Data loading failed: {e.stderr}")
        raise Exception(f"Data loading failed: {e.stderr}")


@op
def run_dbt_transformations(context: OpExecutionContext) -> str:
    """
    Run dbt transformations to build the data warehouse.
    
    Returns:
        Status message
    """
    context.log.info("Running dbt transformations...")
    
    dbt_project_dir = Path("medical_warehouse")
    
    try:
        # Run dbt deps first
        context.log.info("Installing dbt dependencies...")
        subprocess.run(
            ["dbt", "deps"],
            cwd=dbt_project_dir,
            check=True,
            capture_output=True
        )
        
        # Run dbt models
        context.log.info("Running dbt models...")
        result = subprocess.run(
            ["dbt", "run"],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
            check=True
        )
        
        context.log.info("dbt run completed successfully")
        context.log.info(result.stdout)
        
        # Run dbt tests
        context.log.info("Running dbt tests...")
        test_result = subprocess.run(
            ["dbt", "test"],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
            check=True
        )
        
        context.log.info("dbt tests completed successfully")
        context.log.info(test_result.stdout)
        
        return "dbt transformations completed successfully"
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt transformation failed: {e.stderr}")
        raise Exception(f"dbt transformation failed: {e.stderr}")


@op
def run_yolo_enrichment(context: OpExecutionContext) -> str:
    """
    Run YOLO object detection on images and load results.
    
    Returns:
        Status message
    """
    context.log.info("Running YOLO object detection...")
    
    try:
        result = subprocess.run(
            ["python", "src/yolo_detect.py"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path.cwd()
        )
        
        context.log.info("YOLO enrichment completed successfully")
        context.log.info(result.stdout)
        
        # After YOLO detection, we need to rebuild the fct_image_detections model
        context.log.info("Rebuilding fct_image_detections model...")
        dbt_result = subprocess.run(
            ["dbt", "run", "--select", "fct_image_detections"],
            cwd=Path("medical_warehouse"),
            capture_output=True,
            text=True,
            check=True
        )
        
        context.log.info("fct_image_detections model rebuilt")
        
        return "YOLO enrichment completed successfully"
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"YOLO enrichment failed: {e.stderr}")
        raise Exception(f"YOLO enrichment failed: {e.stderr}")


@job
def telegram_data_pipeline():
    """
    Main pipeline job that orchestrates all data operations.
    
    Execution order:
    1. Scrape Telegram data
    2. Load raw data to PostgreSQL
    3. Run dbt transformations
    4. Run YOLO enrichment (depends on step 1 for images)
    """
    # Step 1: Scrape data
    scrape_result = scrape_telegram_data()
    
    # Step 2: Load to PostgreSQL (depends on scraping)
    load_result = load_raw_to_postgres()
    
    # Step 3: Run dbt transformations (depends on loading)
    dbt_result = run_dbt_transformations()
    
    # Step 4: Run YOLO enrichment (depends on scraping for images)
    yolo_result = run_yolo_enrichment()


@schedule(
    job=telegram_data_pipeline,
    cron_schedule="0 2 * * *",  # Run daily at 2 AM
    default_status=DefaultScheduleStatus.STOPPED,
)
def daily_pipeline_schedule(context: ScheduleEvaluationContext):
    """
    Schedule to run the pipeline daily at 2 AM.
    """
    return {}


if __name__ == "__main__":
    # For local development
    from dagster import DagsterInstance
    from dagster_webserver import DagsterWebserver
    
    instance = DagsterInstance.ephemeral()
    webserver = DagsterWebserver(instance=instance)
    webserver.run()
