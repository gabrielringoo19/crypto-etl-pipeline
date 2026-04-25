# Crypto ETL Pipeline

A lightweight ETL pipeline that extracts real-time cryptocurrency market data from the [CoinGecko API](https://www.coingecko.com/en/api), transforms it into a clean and analysis-ready format, and loads it into a CSV file.

## Features

- Extracts top N coins by market cap via CoinGecko public API
- Transforms raw data: type casting, null handling, and column cleanup
- Loads output to a structured CSV file
- Fully configurable via `.env` file
- Clean logging across all ETL stages

## Tech Stack

- **Python 3.11**
- **Pandas** — data transformation
- **Requests** — API calls
- **python-dotenv** — environment config

## Project Structure

etl_project/
├── main.py          # ETL pipeline logic
├── .env             # Environment variables (not tracked)
├── .env.example     # Example env config
├── requirements.txt
└── README.md

## Getting Started

**1. Clone the repository**
git clone https://github.com/your-username/etl_project.git
cd etl_project


**2. Install dependencies**
pip install -r requirements.txt

**3. Set up environment variables**
cp .env.example .env

Edit `.env` with your config:
BASE_URL=https://api.coingecko.com/api/v3
CURRENCY=usd
TOP_N_COINS=10
OUTPUT_FILE=crypto_data.csv

**4. Run the pipeline**

python main.py

## Output

A clean CSV file containing the following columns:

| Category | Columns |
|---|---|
| Identity | `id`, `symbol`, `name` |
| Price | `current_price`, `high_24h`, `low_24h`, `price_change_percentage_24h` |
| Market | `market_cap`, `market_cap_rank`, `market_cap_change_percentage_24h`, `total_volume` |
| Supply | `circulating_supply`, `total_supply`, `max_supply` |
| All-time | `ath`, `ath_date`, `atl`, `atl_date` |
| Others | `roi`, `last_updated` |


## License
MIT