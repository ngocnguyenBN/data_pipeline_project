from abc import ABC, abstractmethod

class Adapter(ABC):
    """
    Abstract Base Class for Adapters.
    Adapters act as intermediaries between crawlers and data modules.
    """

    @abstractmethod
    def __init__(self, crawler):
        """
        Initialize the adapter with a specific crawler instance.
        """
        self.crawler = crawler

    @abstractmethod
    def fetch_and_clean(self):
        """
        Fetch raw data from the crawler and clean it.
        """
        pass

    @abstractmethod
    def transform(self, cleaned_data):
        """
        Transform cleaned data into the format required by the data module.
        """
        pass

    def process(self):
        """
        High-level process:
        1. Fetch and clean data from the crawler.
        2. Transform the cleaned data for the data module.
        """
        cleaned_data = self.fetch_and_clean()
        transformed_data = self.transform(cleaned_data)
        return transformed_data
