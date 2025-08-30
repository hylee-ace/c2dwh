from dataclasses import dataclass
from datetime import datetime


@dataclass
class Product:
    product_id: int
    name: str
    onsale_price: int | None = None
    brand: str | None = None
    category: str | None = None
    rating: float | None = None
    reviews_count: int | None = None
    url: str | None = None
    release_date: datetime | None = None
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
            "url": self.url,
            "created_at": self.release_date,
            "updated_at": self.updated_at,
        }


"""
product: name, brand, price, category, rating, reviews count, url, release date, updated at
phone: ram, os, chipset, storage, screen size, battery, screen tech, screen frequency
laptop: ram, os, cpu, gpu, storage, screen size, battery, screen tech, screen frequency
tablet: ram, os, chipset, storage, screen size, battery
smartwatch: screen tech, screen size, connection, cpu, os, storage, battery
twsearphones: control, connectivity, battery
screen: screen size, screen tech, frequency, power consumption, connection ports
"""
