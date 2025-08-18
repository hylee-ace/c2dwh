from datetime import datetime


class Device:
    def __init__(
        self,
        product_id: int,
        name: str,
        *,
        sku: str = None,
        price: int | float = None,
        saleoff_price: int | float = None,
        brand: str,
        stock: int = None,
        rating: float = None,
        reviews_count: int = None,
        url: str,
        created_at: datetime = None,
        updated_at: datetime = None
    ):
        self.__prd_id = product_id
        self.__name = name
        self.__sku = sku
        self.__price = price
        self.__sale = saleoff_price
        self.__brand = brand
        self.__stock = stock
        self.__rating = rating
        self.__reviews = reviews_count
        self.__url = url
        self.__created_at = created_at
        self.__updated = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not updated_at
            else updated_at
        )


class Phone(Device):
    def __init__(
        self,
        product_id,
        name,
        *,
        sku=None,
        price=None,
        saleoff_price=None,
        brand,
        stock=None,
        rating=None,
        reviews_count=None,
        url,
        created_at=None,
        updated_at=None,
        os: str = None
    ):
        super().__init__(
            product_id,
            name,
            sku=sku,
            price=price,
            saleoff_price=saleoff_price,
            brand=brand,
            stock=stock,
            rating=rating,
            reviews_count=reviews_count,
            url=url,
            created_at=created_at,
            updated_at=updated_at,
        )
        self.__os = os

    def info(self):
        return {
            "id": self.__prd_id,
            "name": self.__name,
            "sku": self.__sku,
            "price": self.__price,
            "saleoff_price": self.__sale,
            "brand": self.__brand,
            "os": self.__os,
            "stock": self.__stock,
            "rating": self.__rating,
            "reviews_count": self.__reviews,
            "categories": "Smartphone",
            "url": self.__url,
            "created_at": self.__created_at,
            "updated_at": self.__updated,
        }


class Laptop(Device):
    def __init__(
        self,
        product_id,
        name,
        *,
        sku=None,
        price=None,
        saleoff_price=None,
        brand,
        stock=None,
        rating=None,
        reviews_count=None,
        url,
        created_at=None,
        updated_at=None,
        cpu: str = None
    ):
        super().__init__(
            product_id,
            name,
            sku=sku,
            price=price,
            saleoff_price=saleoff_price,
            brand=brand,
            stock=stock,
            rating=rating,
            reviews_count=reviews_count,
            url=url,
            created_at=created_at,
            updated_at=updated_at,
        )
        self.__cpu = cpu

    def info(self):
        return {
            "id": self.__prd_id,
            "name": self.__name,
            "sku": self.__sku,
            "price": self.__price,
            "saleoff_price": self.__sale,
            "brand": self.__brand,
            "cpu": self.__cpu,
            "stock": self.__stock,
            "rating": self.__rating,
            "reviews_count": self.__reviews,
            "categories": "Laptop",
            "url": self.__url,
            "created_at": self.__created_at,
            "updated_at": self.__updated,
        }
