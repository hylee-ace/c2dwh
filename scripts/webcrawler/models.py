from dataclasses import dataclass
from datetime import datetime


@dataclass
class ProductInfo:
    sku: int
    name: str
    price: int | None = None
    brand: str | None = None
    category: str | None = None
    rating: float | None = None
    reviews_count: int | None = None
    url: str | None = None
    release_date: datetime | None = None


@dataclass
class Phone(ProductInfo):
    cpu: str | None = None
    cpu_speed: str | None = None
    gpu: str | None = None
    ram: str | None = None
    storage: str | None = None
    rearcam_specs: str | None = None
    frontcam_specs: str | None = None
    screen_type: str | None = None  # gorillas or regular
    screen_size: str | None = None
    screen_panel: str | None = None  # oled or fhd
    screen_res: str | None = None
    screen_rate: str | None = None
    screen_nits: str | None = None
    os: str | None = None
    water_resistant: str | None = None  # ip68
    battery: str | None = None
    charger: str | None = None
    weight: str | None = None
    material: str | None = None
    connectivity: str | None = None  # wifi or bluetooth
    network: str | None = None  # 4g or 5g
    ports: str | None = None  # typeC or jack 3.5mm
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Tablet(ProductInfo):
    cpu: str | None = None
    cpu_speed: str | None = None
    gpu: str | None = None
    ram: str | None = None
    storage: str | None = None
    rearcam_specs: str | None = None
    frontcam_specs: str | None = None
    screen_size: str | None = None
    screen_panel: str | None = None  # oled or fhd
    screen_res: str | None = None
    screen_rate: str | None = None
    os: str | None = None
    water_resistant: str | None = None  # ip68
    battery: str | None = None
    charger: str | None = None
    weight: str | None = None
    material: str | None = None
    connectivity: str | None = None  # wifi or bluetooth
    network: str | None = None  # 4g or 5g
    ports: str | None = None  # typeC or jack 3.5mm
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Laptop(ProductInfo):
    cpu: str | None = None
    cpu_cores: int | None = None
    cpu_threads: int | None = None
    cpu_speed: str | None = None
    gpu: str | None = None
    ram: str | None = None
    max_ram: str | None = None
    ram_type: str | None = None
    ram_bus: str | None = None
    storage: str | None = None
    webcam: str | None = None
    screen_panel: str | None = None  # ips or oled
    screen_size: str | None = None
    screen_tech: str | None = None  # antiflare
    screen_res: str | None = None
    screen_rate: str | None = None
    screen_nits: str | None = None
    os: str | None = None
    battery: str | None = None
    weight: str | None = None
    material: str | None = None
    connectivity: str | None = None  # wifi or bluetooth
    ports: str | None = None  # thunderbolt or hdmi
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Watch(ProductInfo):
    cpu: str | None = None
    storage: str | None = None
    screen_type: str | None = None  # gorillas or regular
    screen_size: str | None = None
    screen_panel: str | None = None  # oled or tft
    os: str | None = None
    water_resistant: str | None = None  # ip68
    connectivity: str | None = None  # wifi or bluetooth
    battery: str | None = None
    weight: str | None = None
    material: str | None = None
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Earphone(ProductInfo):
    sound_tech: str | None = None
    speaker_driver: str | None = None  # 11mm
    compatible: str | None = None  # os or android
    control: str | None = None  # touch or button
    connectivity: str | None = None  # bluetooth
    water_resistant: str | None = None  # ip68
    ports: str | None = None  # typeC or jack 3.5
    battery: str | None = None
    case_battery: str | None = None
    weight: str | None = None
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Screen(ProductInfo):
    screen_type: str | None = None  # curved or flat
    screen_panel: str | None = None  # ips or oled
    screen_size: str | None = None
    screen_tech: str | None = None  # antiflare
    screen_res: str | None = None
    screen_rate: str | None = None
    power_consumption: str | None = None
    ports: str | None = None
    weight: str | None = None
    updated_at: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
