"""
Telegram Data Scraper

This script scrapes messages and images from public Telegram channels
and stores them in a raw data lake structure.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import asyncio
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f"scraper_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Telegram channels to scrape
TELEGRAM_CHANNELS = [
    "chemed",  # CheMed Telegram Channel
    "lobelia4cosmetics",  # Lobelia Cosmetics
    "tikvahpharma",  # Tikvah Pharma
    # Add more channels from et.tgstat.com/medicine
]

# Data directories
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
MESSAGES_DIR = RAW_DIR / "telegram_messages"
IMAGES_DIR = RAW_DIR / "images"

# Create directories
MESSAGES_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)


class TelegramScraper:
    """Scraper for Telegram channels"""
    
    def __init__(self, api_id: str, api_hash: str, session_name: str = "scraper_session"):
        """
        Initialize the Telegram scraper
        
        Args:
            api_id: Telegram API ID
            api_hash: Telegram API hash
            session_name: Name for the session file
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.scraped_channels = set()
        self.scraped_dates = set()
        
    async def connect(self):
        """Connect to Telegram"""
        await self.client.start()
        logger.info("Connected to Telegram")
        
    async def disconnect(self):
        """Disconnect from Telegram"""
        await self.client.disconnect()
        logger.info("Disconnected from Telegram")
        
    async def download_image(self, message, channel_name: str) -> Optional[str]:
        """
        Download image from a message if present
        
        Args:
            message: Telegram message object
            channel_name: Name of the channel
            
        Returns:
            Path to downloaded image or None
        """
        if not message.media:
            return None
            
        try:
            # Check if it's a photo
            if isinstance(message.media, MessageMediaPhoto):
                image_path = IMAGES_DIR / channel_name / f"{message.id}.jpg"
                image_path.parent.mkdir(parents=True, exist_ok=True)
                
                await self.client.download_media(message.media, file=str(image_path))
                logger.info(f"Downloaded image: {image_path}")
                return str(image_path.relative_to(DATA_DIR))
                
            # Check if it's a document that might be an image
            elif isinstance(message.media, MessageMediaDocument):
                if message.media.document.mime_type and message.media.document.mime_type.startswith('image/'):
                    image_path = IMAGES_DIR / channel_name / f"{message.id}.jpg"
                    image_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    await self.client.download_media(message.media, file=str(image_path))
                    logger.info(f"Downloaded image: {image_path}")
                    return str(image_path.relative_to(DATA_DIR))
                    
        except Exception as e:
            logger.error(f"Error downloading image for message {message.id}: {e}")
            
        return None
        
    async def scrape_channel(
        self, 
        channel_name: str, 
        limit: int = 100,
        days_back: int = 30
    ) -> List[Dict]:
        """
        Scrape messages from a Telegram channel
        
        Args:
            channel_name: Name of the Telegram channel
            limit: Maximum number of messages to scrape
            days_back: Number of days to look back
            
        Returns:
            List of message dictionaries
        """
        messages = []
        start_date = datetime.now() - timedelta(days=days_back)
        
        try:
            logger.info(f"Scraping channel: {channel_name}")
            entity = await self.client.get_entity(channel_name)
            
            async for message in self.client.iter_messages(
                entity, 
                limit=limit,
                offset_date=start_date
            ):
                if not message.text and not message.media:
                    continue
                    
                # Download image if present
                image_path = await self.download_image(message, channel_name)
                
                message_data = {
                    "message_id": message.id,
                    "channel_name": channel_name,
                    "message_date": message.date.isoformat() if message.date else None,
                    "message_text": message.text or "",
                    "has_media": message.media is not None,
                    "image_path": image_path,
                    "views": message.views or 0,
                    "forwards": message.forwards or 0,
                }
                
                messages.append(message_data)
                
            logger.info(f"Scraped {len(messages)} messages from {channel_name}")
            self.scraped_channels.add(channel_name)
            
        except Exception as e:
            logger.error(f"Error scraping channel {channel_name}: {e}")
            
        return messages
        
    def save_messages(self, messages: List[Dict], channel_name: str):
        """
        Save messages to JSON files organized by date
        
        Args:
            messages: List of message dictionaries
            channel_name: Name of the channel
        """
        if not messages:
            return
            
        # Group messages by date
        messages_by_date = {}
        for msg in messages:
            if msg.get("message_date"):
                date_str = datetime.fromisoformat(msg["message_date"]).strftime("%Y-%m-%d")
                if date_str not in messages_by_date:
                    messages_by_date[date_str] = []
                messages_by_date[date_str].append(msg)
                self.scraped_dates.add(date_str)
        
        # Save to partitioned directories
        for date_str, date_messages in messages_by_date.items():
            date_dir = MESSAGES_DIR / date_str
            date_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = date_dir / f"{channel_name}.json"
            
            # Load existing data if file exists
            existing_data = []
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            
            # Merge with existing data (avoid duplicates)
            existing_ids = {msg["message_id"] for msg in existing_data}
            new_messages = [msg for msg in date_messages if msg["message_id"] not in existing_ids]
            existing_data.extend(new_messages)
            
            # Save updated data
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved {len(new_messages)} new messages to {file_path}")
            
    async def scrape_all_channels(self, limit: int = 100, days_back: int = 30):
        """
        Scrape all configured channels
        
        Args:
            limit: Maximum number of messages per channel
            days_back: Number of days to look back
        """
        await self.connect()
        
        try:
            for channel in TELEGRAM_CHANNELS:
                messages = await self.scrape_channel(channel, limit=limit, days_back=days_back)
                self.save_messages(messages, channel)
                
                # Rate limiting - wait between channels
                await asyncio.sleep(2)
                
        finally:
            await self.disconnect()
            
        logger.info(f"Scraping complete. Channels: {len(self.scraped_channels)}, Dates: {len(self.scraped_dates)}")


async def main():
    """Main function to run the scraper"""
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    
    if not api_id or not api_hash:
        logger.error("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env file")
        return
        
    scraper = TelegramScraper(api_id, api_hash)
    await scraper.scrape_all_channels(limit=100, days_back=30)


if __name__ == "__main__":
    asyncio.run(main())
