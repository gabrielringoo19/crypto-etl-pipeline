import pandas as pd
import requests
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, base_url, currency, top_n_coins, output_file):
        self.base_url = base_url
        self.currency = currency
        self.top_n_coins = top_n_coins
        self.output_file = output_file
        self.df = None

    def extract_data(self):
        try:
            logger.info("Starting extraction...")

            url = f"{self.base_url}/coins/markets"
            params = {
                "vs_currency": self.currency,
                "per_page": self.top_n_coins,
                "page": 1
            }

            req = requests.get(url, params=params, timeout=5)
            req.raise_for_status()

            self.df = pd.DataFrame(req.json())
            logger.info(f"Extracted {len(self.df)} rows successfully!")

        except requests.exceptions.RequestException as e:
            logger.error(f"Extraction failed: {e}")
            raise

    def transform_data(self):
        try:
            logger.info("Starting transformation...")

            
            cols_to_drop = [
                'image',
                'fully_diluted_valuation',
                'price_change_24h',
                'market_cap_change_24h',
                'ath_change_percentage',
                'atl_change_percentage',
            ]
            self.df.drop(columns=cols_to_drop, inplace=True)

            
            date_cols = ['ath_date', 'atl_date', 'last_updated']
            for col in date_cols:
                self.df[col] = pd.to_datetime(self.df[col], utc=True)

            
            self.df['max_supply'].fillna(0, inplace=True)
            self.df['total_supply'].fillna(0, inplace=True)  
            self.df['roi'] = self.df['roi'].apply(lambda x: x.get('percentage') if isinstance(x, dict) else None)     

            
            self.df.reset_index(drop=True, inplace=True)

            logger.info(f"Transformation done! Shape: {self.df.shape}")
            logger.info(f"Columns: {list(self.df.columns)}")

        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise

    def load_data(self):
        try:
            logger.info("Starting load...")

            self.df.to_csv(self.output_file, index=False)
            logger.info(f"Data loaded to '{self.output_file}' ({len(self.df)} rows)")

        except Exception as e:
            logger.error(f"Load failed: {e}")
            raise

    def run(self):
        try:
            self.extract_data()
            self.transform_data()
            self.load_data()
            logger.info("ETL pipeline completed successfully!")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    load_dotenv()

    pipeline = ETLPipeline(
        base_url=os.getenv("BASE_URL", "https://api.coingecko.com/api/v3"),
        currency=os.getenv("CURRENCY", "usd"),
        top_n_coins=int(os.getenv("TOP_N_COINS", 10)),
        output_file=os.getenv("OUTPUT_FILE", "crypto_data.csv")
    )
    pipeline.run()