from dataclasses import dataclass, field, InitVar
from pyarrow import ArrowInvalid
from datetime import datetime
import pyarrow.parquet as pq
from decimal import Decimal
from typing import Optional
import pyarrow as pa
import json


class ProductPurchase:
    def __init__(self, product_purchase_json: dict):
        self.sku = int(product_purchase_json["sku"])
        if "quantity" in product_purchase_json:
            self.quantity = product_purchase_json["quantity"]
        else:
            self.quantity = product_purchase_json["quanitity"]
        self.price = Decimal(product_purchase_json["price"])
        self.total = Decimal(product_purchase_json["total"])


class Purchases:
    def __init__(self, purchase_json: dict):
        self.products = [ProductPurchase(product_purchase_dict) for product_purchase_dict in purchase_json["products"]]
        self.total_cost = Decimal(purchase_json["total_cost"])
        cost_sum = sum([product.total for product in self.products])
        assert self.total_cost == cost_sum, "`total_cost` should match the total amount that all products purchased total up to."

    # Returns this object as a parquet data object ready to be saved to file.
    def to_parquet_data(self):
        purchase_dict = vars(self)
        product_dicts = []
        for product_object in purchase_dict['products']:
            product_dicts.append(vars(product_object))
        purchase_dict['products'] = product_dicts
        return purchase_dict

    @staticmethod
    # Returns the parquet structure to save this object to file.
    def parquet_struct():
        product_purchase_schema = pa.struct(
            [
                pa.field('sku', type=pa.int64()),
                pa.field('quantity', type=pa.int32()),
                pa.field('price', type=pa.decimal128(12, 2)),
                pa.field('total', type=pa.decimal128(12, 2)),
            ]
        )
        purchases_schema = pa.struct(
            [
                pa.field('total_cost', pa.decimal128(12, 2)),
                pa.field('products', type=pa.list_(product_purchase_schema))
            ]
        )
        return purchases_schema


@dataclass
class Transaction:
    transaction_id: str
    customer_id: int
    transaction_time: datetime = field(init=False)
    delivery_address: str
    purchases: Optional[Purchases] = field(default=None, init=False)

    purchases_json: InitVar[str] = None
    transaction_time_string: InitVar[str] = None
    transaction_time_datetime: InitVar[datetime] = None

    def __post_init__(self, purchases_json: str, transaction_time_string: str, transaction_time_datetime: datetime):
        # Mandatory fields
        assert self.transaction_id is not None
        assert self.customer_id is not None

        # Enforce type
        self.customer_id = int(self.customer_id)

        # Parse nontrivial inputs
        assert transaction_time_string is None or transaction_time_datetime is None, "Can only set transaction time via one method at a time."
        if transaction_time_string is not None:
            self.transaction_time = datetime.fromisoformat(transaction_time_string)
        elif transaction_time_datetime is not None:
            self.transaction_time = transaction_time_datetime
        else:
            raise Exception("transaction_time not set.")

        if purchases_json is not None:
            self.purchases = Purchases(purchases_json)

    @classmethod
    def from_string(cls, transaction_string: str):
        transaction_json = json.loads(transaction_string)

        transaction_id = transaction_json.get("transaction_id")
        customer_id = transaction_json.get("customer_id")
        transaction_time_string = transaction_json.get("transaction_time")
        delivery_address = transaction_json.get("delivery_address")

        purchases_json = transaction_json.get("purchases")

        transaction = cls(transaction_id=transaction_id, customer_id=customer_id, transaction_time_string=transaction_time_string,
                          delivery_address=delivery_address, purchases_json=purchases_json)
        return transaction

    @staticmethod
    # Returns the parquet structure to save this object to file.
    def parquet_struct():
        # TODO is the explicit schema definition necessary? Can it be trimmed away?
        delivery_address_schema = pa.struct(
            [
                pa.field('address', type=pa.string()),
                pa.field('city', type=pa.string()),
                pa.field('country', type=pa.string()),
                pa.field('postcode', type=pa.string())
            ]
        )
        transactions_struct = pa.struct(
            [
                pa.field('transaction_id', pa.string()),
                pa.field('customer_id', pa.int64()),
                pa.field('delivery_address', delivery_address_schema),
                pa.field('transaction_time', pa.timestamp('us')),
                pa.field('purchases', Purchases.parquet_struct())
            ]
        )
        return transactions_struct


def read_transactions(filepath: str):
    try:
        file = pq.ParquetFile(filepath)
        empty_file = False
    except ArrowInvalid:
        empty_file = True
    if not empty_file:
        for record_batch in file.iter_batches(batch_size=1000):
            record_list = zip(*[column.to_pylist() for column in record_batch.columns])
            for record in record_list:
                # Construct dictionary of transaction parameters
                transaction_dict = {column_name: value for (column_name, value) in zip(record_batch.column_names, record)}
                transaction_dict["transaction_time_datetime"] = transaction_dict.pop("transaction_time")
                transaction_dict["purchases_json"] = transaction_dict.pop("purchases")
                # Pass dictionary as keyword arguments
                transaction = Transaction(**transaction_dict)
                yield transaction


def read_all_transactions(filepath: str):
    return list(read_transactions(filepath))
