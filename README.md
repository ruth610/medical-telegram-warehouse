# Medical Telegram Data Warehouse

End-to-end data pipeline for Telegram, leveraging dbt for transformation, Dagster for orchestration, and YOLOv8 for data enrichment.

## Project Structure

```
medical-telegram-warehouse/
├── .vscode/
├── .env               # Secrets (create from .env.example)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── data/              # Raw data storage
├── medical_warehouse/ # dbt project
├── src/               # Scraper and utility scripts
├── api/               # FastAPI application
├── notebooks/         # Analysis notebooks
└── scripts/           # Helper scripts
```

## Setup

1.  **Environment Setup**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configuration**:
    Copy `.env.example` to `.env` and fill in your Telegram API credentials and Database config.

3.  **Database**:
    Start the PostgreSQL database:
    ```bash
    docker-compose up -d db
    ```

4.  **Data Scraping**:
    Run the scraper to populate `data/raw`:
    ```bash
    python src/scraper.py
    ```

5.  **DBT Setup**:
    Initialize the dbt project (if not already done):
    ```bash
    dbt init medical_warehouse
    ```
    Configure `profiles.yml` to point to your local Postgres database.

6.  **Pipeline**:
    (Instructions for Dagster and API to be added)

## Deliverables Status

- [x] Project Structure Created
- [x] Task 1: Scraper Script (`src/scraper.py`)
- [ ] Task 2: dbt Project & Models
- [ ] Task 3: YOLO Object Detection
- [ ] Task 4: FastAPI
- [ ] Task 5: Dagster Pipeline