import requests
import pandas as pd
import time
from ingestion.base_ingestor import BaseIngestor


class SensorIngestion(BaseIngestor):

    def __init__(self):
        super().__init__("SensorIngestion")

    def extract(self):
        """
        Simulates pulling sensor data from an external API
        """
        self.logger.info("Extracting sensor data from API")

        url = "https://dummyjson.com/products"
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"API call failed: {response.status_code}")

        return response.json()

    def transform(self, data):
        """
        Convert raw API data to structured dataframe
        """
        self.logger.info("Transforming sensor data")

        records = []

        for item in data.get("products", []):
            records.append({
                "sensor_id": item["id"],
                "temperature": float(item["price"]),
                "pressure": float(item["rating"]),
                "flow_rate": float(item["stock"]),
                "timestamp": int(time.time())
            })

        df = pd.DataFrame(records)

        # Add partition columns
        df["year"] = pd.to_datetime(df["timestamp"], unit="s").dt.year
        df["month"] = pd.to_datetime(df["timestamp"], unit="s").dt.month

        return df

    def load(self, df):
        """
        Load into Bronze layer
        """
        output_path = "data/bronze/sensor/"

        self.logger.info(f"Writing sensor data to {output_path}")

        df.to_parquet(output_path, index=False)

        self.logger.info(f"Loaded {len(df)} sensor records")


if __name__ == "__main__":
    SensorIngestion().run()