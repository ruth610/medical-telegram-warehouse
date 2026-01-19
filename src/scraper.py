import os
import json
import logging
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_ID = os.getenv('TG_API_ID')
API_HASH = os.getenv('TG_API_HASH')
PHONE = os.getenv('TG_PHONE')

# Configuration
CHANNELS = [
    'https://t.me/lobelia4cosmetics',
    'https://t.me/tikvahpharma',
    # Add more channels here
    # 'CheMed123',
]

# Path configuration
# Get the directory where the script is located (src/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root (parent of src/)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')

# Setup logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'scraper.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def scrap_channel(client, channel_url):
    logger.info(f"Scraping channel: {channel_url}")
    try:
        entity = await client.get_entity(channel_url)
        channel_name = entity.username or entity.id

        # Create directory for images
        images_dir = os.path.join(DATA_DIR, 'images', str(channel_name))
        os.makedirs(images_dir, exist_ok=True)

        today_str = datetime.now().strftime('%Y-%m-%d')
        messages_data = []

        async for message in client.iter_messages(entity, limit=100): # Limit for demo

            msg_data = {
                'message_id': message.id,
                'channel_name': channel_name,
                'date': message.date.isoformat(),
                'message_text': message.text,
                'views': message.views,
                'forwards': message.forwards,
                'has_media': False,
                'image_path': None
            }

            if message.media and isinstance(message.media, MessageMediaPhoto):
                msg_data['has_media'] = True
                photo_path = os.path.join(images_dir, f"{message.id}.jpg")
                await client.download_media(message, photo_path)
                msg_data['image_path'] = photo_path

            messages_data.append(msg_data)

        # Save to JSON
        json_dir = os.path.join(DATA_DIR, 'telegram_messages', today_str)
        os.makedirs(json_dir, exist_ok=True)
        json_path = os.path.join(json_dir, f"{channel_name}.json")

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(messages_data, f, ensure_ascii=False, indent=4)

        logger.info(f"Successfully scraped {len(messages_data)} messages from {channel_name}")

    except Exception as e:
        logger.error(f"Error scraping {channel_url}: {e}")

async def main():
    if not API_ID or not API_HASH:
        print("Error: TG_API_ID and TG_API_HASH environment variables are missing.")
        print("Please create a .env file with your Telegram credentials.")
        return

    # Ensure API_ID is an integer
    try:
        api_id_int = int(API_ID)
    except ValueError:
        print("Error: TG_API_ID must be an integer.")
        return

    # Use a session file path relative to project root
    session_path = os.path.join(PROJECT_ROOT, 'anon')

    async with TelegramClient(session_path, api_id_int, API_HASH) as client:
        for channel in CHANNELS:
            await scrap_channel(client, channel)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())