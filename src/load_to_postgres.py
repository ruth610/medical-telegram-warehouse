"""
Load raw JSON data from data lake to PostgreSQL

This script reads JSON files from the data lake and loads them
into a raw schema in PostgreSQL.
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/load_postgres_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Data directory
MESSAGES_DIR = Path("data/raw/telegram_messages")


class PostgresLoader:
    """Loader for PostgreSQL database"""
    
    def __init__(self):
        """Initialize database connection"""
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "warehouse_user"),
                password=os.getenv("POSTGRES_PASSWORD", "warehouse_pass"),
                database=os.getenv("POSTGRES_DB", "medical_warehouse")
            )
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
            
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from database")
        
    def create_raw_schema(self):
        """Create raw schema and table if they don't exist"""
        try:
            # Create raw schema
            self.cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            
            # Create telegram_messages table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id SERIAL PRIMARY KEY,
                message_id BIGINT NOT NULL,
                channel_name VARCHAR(255) NOT NULL,
                message_date TIMESTAMP,
                message_text TEXT,
                has_media BOOLEAN DEFAULT FALSE,
                image_path VARCHAR(500),
                views INTEGER DEFAULT 0,
                forwards INTEGER DEFAULT 0,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(message_id, channel_name)
            );
            """
            
            self.cursor.execute(create_table_query)
            
            # Create index for faster queries
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel 
                ON raw.telegram_messages(channel_name);
            """)
            
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_messages_date 
                ON raw.telegram_messages(message_date);
            """)
            
            self.conn.commit()
            logger.info("Created raw schema and telegram_messages table")
            
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            self.conn.rollback()
            raise
            
    def load_json_files(self):
        """Load all JSON files from data lake"""
        if not MESSAGES_DIR.exists():
            logger.warning(f"Data directory {MESSAGES_DIR} does not exist")
            return
            
        json_files = list(MESSAGES_DIR.rglob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files to load")
        
        total_loaded = 0
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    messages = json.load(f)
                    
                if not messages:
                    continue
                    
                # Prepare data for insertion
                insert_data = []
                for msg in messages:
                    insert_data.append((
                        msg.get("message_id"),
                        msg.get("channel_name"),
                        msg.get("message_date"),
                        msg.get("message_text", ""),
                        msg.get("has_media", False),
                        msg.get("image_path"),
                        msg.get("views", 0),
                        msg.get("forwards", 0),
                    ))
                
                # Insert data (using ON CONFLICT to avoid duplicates)
                insert_query = """
                INSERT INTO raw.telegram_messages 
                (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards)
                VALUES %s
                ON CONFLICT (message_id, channel_name) DO NOTHING;
                """
                
                execute_values(self.cursor, insert_query, insert_data)
                self.conn.commit()
                
                loaded = len(insert_data)
                total_loaded += loaded
                logger.info(f"Loaded {loaded} messages from {json_file}")
                
            except Exception as e:
                logger.error(f"Error loading {json_file}: {e}")
                self.conn.rollback()
                
        logger.info(f"Total messages loaded: {total_loaded}")
        
    def run(self):
        """Run the loader"""
        self.connect()
        try:
            self.create_raw_schema()
            self.load_json_files()
        finally:
            self.disconnect()


def main():
    """Main function"""
    loader = PostgresLoader()
    loader.run()


if __name__ == "__main__":
    main()
