from dataclasses import dataclass
from datetime import datetime


@dataclass
class Product:
    product_id: int
    name: str
    price: int | float = None
    onsale_price: int | float = None
    brand: str = None
    category: str = None
    os: str = None
    cpu: str = None
    gpu: str = None
    stock: int = None
    rating: float = None
    reviews_count: int = None
    is_new: bool = True
    is_selling: bool = True
    url: str = None
    retailer: str = None
    created_at: datetime = None
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def info(self):
        if not self.price and not self.stock:
            self.is_selling = False

        return {
            "id": self.product_id,
            "name": self.name,
            "price": self.price,
            "onsale_price": self.onsale_price,
            "brand": self.brand,
            "category": self.category,
            "os": self.os,
            "cpu": self.cpu,
            "gpu": self.gpu,
            "stock": self.stock,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "is_new": self.is_new,
            "is_selling": self.is_selling,
            "url": self.url,
            "retailer": self.retailer,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
