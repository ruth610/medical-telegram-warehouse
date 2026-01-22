#!/bin/bash
# Setup script for the project

set -e

echo "Setting up Telegram Health Data Platform..."

# Create necessary directories
echo "Creating directories..."
mkdir -p data/raw/telegram_messages
mkdir -p data/raw/images
mkdir -p data/processed
mkdir -p logs

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cat > .env << EOF
# Telegram API Credentials
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here

# PostgreSQL Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=warehouse_user
POSTGRES_PASSWORD=warehouse_pass
POSTGRES_DB=medical_warehouse
EOF
    echo "Please edit .env file with your credentials"
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Initialize dbt
echo "Initializing dbt..."
cd medical_warehouse
dbt deps || echo "dbt deps failed, but continuing..."
cd ..

echo "Setup complete!"
echo "Next steps:"
echo "1. Edit .env file with your Telegram API credentials"
echo "2. Start PostgreSQL: docker-compose up -d postgres"
echo "3. Run the scraper: python src/scraper.py"
