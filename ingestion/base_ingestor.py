from abc import ABC, abstractmethod
from utils.logger import get_logger

class BaseIngestor(ABC):

    def __init__(self, name):
        self.logger = get_logger(name)

    @abstractmethod
    def extract(self): pass

    @abstractmethod
    def transform(self, data): pass

    @abstractmethod
    def load(self, df): pass

    def run(self):
        self.logger.info("Start ingestion")
        data = self.extract()
        df = self.transform(data)
        self.load(df)
        self.logger.info("End ingestion")