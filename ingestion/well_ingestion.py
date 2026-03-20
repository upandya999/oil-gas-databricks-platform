import pandas as pd
import random
from ingestion.base_ingestor import BaseIngestor


class WellIngestion(BaseIngestor):

    def __init__(self):
        super().__init__("WellIngestion")

    def extract(self):
        """
        Simulates well metadata extraction
        """
        self.logger.info("Extracting well metadata")

        wells = []

        for i in range(1000):
            wells.append({
                "well_id": i,
                "status": random.choice(["active", "inactive", "maintenance"]),
                "location": random.choice(["onshore", "offshore"]),
                "operator": random.choice(["Shell", "BP", "Chevron"])
            })

        return wells

    def transform(self, data):
        """
        Convert to DataFrame
        """
        self.logger.info("Transforming well data")

        df = pd.DataFrame(data)

        df["ingestion_date"] = pd.Timestamp.now()

        return df

    def load(self, df):
        """
        Load into Bronze layer
        """
        output_path = "data/bronze/well/"

        self.logger.info(f"Writing well data to {output_path}")

        df.to_parquet(output_path, index=False)

        self.logger.info(f"Loaded {len(df)} well records")


if __name__ == "__main__":
    WellIngestion().run()