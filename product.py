from pyarrow.lib import ArrowInvalid
from dataclasses import dataclass
import pyarrow.parquet as pq
from decimal import Decimal
import json


@dataclass
class Product:
    sku: int
    name: str
    price: Decimal
    category: str
    popularity: float

    def __post_init__(self):
        # Mandatory fields
        assert self.sku is not None
        assert self.name is not None
        assert self.price is not None
        assert self.category is not None
        assert self.popularity is not None

        # Enforce type
        self.sku = int(self.sku)
        self.price = Decimal(self.price)

        # Other constraints
        assert self.price >= 0, "Price must be positive."
        assert self.popularity > 0, "Popularity must be greater than 0."

    @classmethod
    def from_string(cls, product_string: str):
        product_json = json.loads(product_string)

        sku = product_json.get("sku")
        if sku == 62291 or sku == "62291":
            print("here")
        name = product_json.get("name")
        price = product_json.get("price")
        category = product_json.get("category")
        popularity = product_json.get("popularity")

        product = cls(sku=sku, name=name, price=price, category=category, popularity=popularity)
        return product


def read_products(filepath: str):
    try:
        file = pq.ParquetFile(filepath)
        empty_file = False
    except ArrowInvalid:
        empty_file = True
    if not empty_file:
        for record_batch in file.iter_batches(batch_size=1000):
            record_list = zip(*[column.to_pylist() for column in record_batch.columns])
            for record in record_list:
                # Construct dictionary of product parameters
                product_dict = {column_name: value for (column_name, value) in zip(record_batch.column_names, record)}
                # Pass dictionary as keyword arguments
                product = Product(**product_dict)
                yield product


def read_all_products(filepath: str):
    return list(read_products(filepath))
