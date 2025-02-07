from abc import ABC, abstractmethod

class DataSource(ABC):
    """Abstract Base Class for all data sources."""

    @abstractmethod
    def connect(self):
        """Kết nối tới nguồn dữ liệu."""
        pass

    @abstractmethod
    def pull_data(self):
        """
        Lấy dữ liệu thô từ nguồn.
        Phương thức này phải được implement bởi các lớp con.
        """
        pass

    @abstractmethod
    def clean_data(self, raw_data):
        """
        Làm sạch dữ liệu thô.
        Nhận dữ liệu thô từ `pull_data` và trả về dữ liệu đã được xử lý.
        """
        pass

    @abstractmethod
    def close_connection(self):
        """Đóng kết nối tới nguồn dữ liệu."""
        pass

    def process_data(self):
        """
        Quy trình chung để lấy và làm sạch dữ liệu.
        Các lớp con có thể sử dụng quy trình này để chuẩn hóa.
        """
        print("Starting data processing...")
        self.connect()
        raw_data = self.pull_data()
        clean_data = self.clean_data(raw_data)
        self.close_connection()
        return clean_data
