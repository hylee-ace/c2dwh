from dataclasses import dataclass
from datetime import datetime


@dataclass
class Product:
    product_id: int
    name: str
    price: int | None = None
    brand: str | None = None
    category: str | None = None
    rating: float | None = None
    reviews_count: int | None = None
    url: str | None = None
    release_date: datetime | None = None


@dataclass
class Phone(Product):
    chipset: str | None = None
    ram: str | None = None
    storage: str | None = None
    scr_size: str | None = None
    scr_tech: str | None = None
    scr_res: str | None = None
    scr_freq: str | None = None
    os: str | None = None
    battery: str | None = None

    def info(self):
        return {
            "id": self.product_id,
            "name": self.name,
            "price": self.price,
            "brand": self.brand,
            "category": self.category,
            "chipset": self.chipset,
            "ram": self.ram,
            "storage": self.storage,
            "screen_size": self.scr_size,
            "screen_tech": self.scr_tech,
            "screen_resolution": self.scr_res,
            "screen_frequency": self.scr_freq,
            "operating_system": self.os,
            "battery": self.battery,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "url": self.url,
            "created_at": self.release_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


@dataclass
class Laptop(Product):
    cpu: str | None = None
    cpu_speed: str | None = None
    gpu: str | None = None
    ram: str | None = None
    ram_type: str | None = None
    storage: str | None = None
    scr_size: str | None = None
    scr_tech: str | None = None
    scr_res: str | None = None
    scr_freq: str | None = None
    os: str | None = None
    battery: str | None = None

    def info(self):
        return {
            "id": self.product_id,
            "name": self.name,
            "price": self.price,
            "brand": self.brand,
            "category": self.category,
            "cpu": self.cpu,
            "cpu_speed": self.cpu_speed,
            "gpu": self.gpu,
            "ram": self.ram,
            "ram_type": self.ram_type,
            "storage": self.storage,
            "screen_size": self.scr_size,
            "screen_tech": self.scr_tech,
            "screen_resolution": self.scr_res,
            "screen_frequency": self.scr_freq,
            "operating_system": self.os,
            "battery": self.battery,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "url": self.url,
            "created_at": self.release_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


@dataclass
class Tablet(Product):
    chipset: str | None = None
    chipset_speed: str | None = None
    gpu: str | None = None
    ram: str | None = None
    storage: str | None = None
    scr_size: str | None = None
    scr_tech: str | None = None
    scr_res: str | None = None
    scr_freq: str | None = None
    os: str | None = None
    battery: str | None = None

    def info(self):
        return {
            "id": self.product_id,
            "name": self.name,
            "price": self.price,
            "brand": self.brand,
            "category": self.category,
            "chipset": self.chipset,
            "chipset_speed": self.chipset_speed,
            "gpu": self.gpu,
            "ram": self.ram,
            "storage": self.storage,
            "screen_size": self.scr_size,
            "screen_tech": self.scr_tech,
            "screen_resolution": self.scr_res,
            "screen_frequency": self.scr_freq,
            "operating_system": self.os,
            "battery": self.battery,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "url": self.url,
            "created_at": self.release_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


@dataclass
class Watch(Product):
    chipset: str | None = None
    storage: str | None = None
    scr_size: str | None = None
    scr_tech: str | None = None
    os: str | None = None
    connectivity: str | None = None
    battery: str | None = None

    def info(self):
        return {
            "id": self.product_id,
            "name": self.name,
            "price": self.price,
            "brand": self.brand,
            "category": self.category,
            "chipset": self.chipset,
            "storage": self.storage,
            "screen_size": self.scr_size,
            "screen_tech": self.scr_tech,
            "operating_system": self.os,
            "connectivity": self.connectivity,
            "battery": self.battery,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "url": self.url,
            "created_at": self.release_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


@dataclass
class Earphone(Product):
    control: str | None = None
    connectivity: str | None = None
    battery: str | None = None
    case_battery: str | None = None

    def info(self):
        return {
            "id": self.product_id,
            "name": self.name,
            "price": self.price,
            "brand": self.brand,
            "category": self.category,
            "control": self.control,
            "connectivity": self.connectivity,
            "battery": self.battery,
            "case_battery": self.case_battery,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "url": self.url,
            "created_at": self.release_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


@dataclass
class Screen(Product):
    scr_size: str | None = None
    scr_tech: str | None = None
    scr_res: str | None = None
    scr_freq: str | None = None
    power_csp: str | None = None
    ports: str | None = None

    def info(self):
        return {
            "id": self.product_id,
            "name": self.name,
            "price": self.price,
            "brand": self.brand,
            "category": self.category,
            "screen_size": self.scr_size,
            "screen_tech": self.scr_tech,
            "screen_resolution": self.scr_res,
            "screen_frequency": self.scr_freq,
            "power_consumption": self.power_csp,
            "ports": self.ports,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "url": self.url,
            "created_at": self.release_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
