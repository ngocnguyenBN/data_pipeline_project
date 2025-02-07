from .base import Adapter
from crawlers import MT5DataSource


class MT5Adapter(Adapter):
    """
    Adapter for the MT5 crawler.
    """

    def __init__(self, crawler=MT5DataSource()):
        super().__init__(crawler)

    def fetch_and_clean(self):
        """
        Fetch raw data using the MT5 crawler and clean it.
        """
        print("Fetching data from MT5 crawler...")
        raw_data = self.crawler.pull_data()

        print("Cleaning MT5 data...")
        cleaned_data = self.crawler.clean_data(raw_data)

        return cleaned_data

    def transform(self, cleaned_data):
        """
        Transform the cleaned data into a format suitable for the data module.
        """
        print("Transforming MT5 data...")
        # Example transformation: Convert to a dictionary format
        transformed_data = {
            "symbol": cleaned_data["symbol"],
            "price": float(cleaned_data["price"]),
            "timestamp": cleaned_data["timestamp"],
        }
        return transformed_data
