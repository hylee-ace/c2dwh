from dataclasses import dataclass
from datetime import datetime


@dataclass
class Product:
    product_id: int
    name: str
    onsale_price: int = None
    brand: str = None
    category: str = None
    os: str = None
    cpu: str = None
    gpu: str = None
    ram: str = None
    storage: str = None
    rating: float = None
    reviews_count: int = None
    is_new: bool = True
    url: str = None
    retailer: str = None
    release_date: datetime = None
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def info(self):
        return {
            "id": self.product_id,
            "name": self.name,
            "onsale_price": self.onsale_price,
            "brand": self.brand,
            "category": self.category,
            "os": self.os,
            "cpu/chipset": self.cpu,
            "gpu": self.gpu,
            "ram": self.ram,
            "storage": self.storage,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "is_new": self.is_new,
            "url": self.url,
            "retailer": self.retailer,
            "created_at": self.release_date,
            "updated_at": self.updated_at,
        }
