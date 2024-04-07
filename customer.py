from dataclasses import dataclass
from pyarrow import ArrowInvalid
import pyarrow.parquet as pq
import hashlib
import json


@dataclass
class Customer:
    id: int
    first_name: str
    last_name: str
    email: str
    date_of_birth: str = None
    phone_number: str = None
    address: str = None
    city: str = None
    country: str = None
    postcode: str = None
    last_change: str = None
    segment: str = None

    def __post_init__(self):
        # Mandatory fields
        assert self.id is not None
        assert self.first_name is not None
        assert self.last_name is not None
        assert self.email is not None

        # Enforce type
        self.id = int(self.id)

    @classmethod
    def from_string(cls, customer_string: str):
        customer_json = json.loads(customer_string, strict=False)

        id = customer_json.get("id")
        first_name = customer_json.get("first_name")
        last_name = customer_json.get("last_name")
        email = customer_json.get("email")
        date_of_birth = customer_json.get("date_of_birth")
        phone_number = customer_json.get("phone_number")
        address = customer_json.get("address")
        city = customer_json.get("city")
        country = customer_json.get("country")
        postcode = customer_json.get("postcode")
        last_change = customer_json.get("last_change")
        segment = customer_json.get("segment")

        customer = cls(id, first_name=first_name, last_name=last_name, email=email, date_of_birth=date_of_birth,
                       phone_number=phone_number, address=address, city=city, country=country, postcode=postcode,
                       last_change=last_change, segment=segment)
        return customer

    # Hash the personal identifying information of the customer
    # This is not fully anonymous- if we want that, we should erase / replace the personal info entirely.
    def anonymize(self):
        self.first_name = hashlib.md5(self.first_name.encode('utf-8')).hexdigest()
        self.last_name = hashlib.md5(self.last_name.encode('utf-8')).hexdigest()
        self.email = hashlib.md5(self.email.encode('utf-8')).hexdigest()
        if self.date_of_birth is not None:
            self.date_of_birth = hashlib.md5(self.date_of_birth.encode('utf-8')).hexdigest()
        if self.phone_number is not None:
            self.phone_number = hashlib.md5(self.phone_number.encode('utf-8')).hexdigest()
        if self.address is not None:
            self.address = hashlib.md5(self.address.encode('utf-8')).hexdigest()
        if self.postcode is not None:
            self.postcode = hashlib.md5(self.postcode.encode('utf-8')).hexdigest()


def read_customers(filepath: str):
    try:
        file = pq.ParquetFile(filepath)
        empty_file = False
    except ArrowInvalid:
        empty_file = True
    if not empty_file:
        for record_batch in file.iter_batches(batch_size=1000):
            record_list = zip(*[column.to_pylist() for column in record_batch.columns])
            for record in record_list:
                # Construct dictionary of customer parameters
                customer_dict = {column_name: value for (column_name, value) in zip(record_batch.column_names, record)}
                # Pass dictionary as keyword arguments
                customer = Customer(**customer_dict)
                yield customer


def read_all_customers(filepath: str):
    return list(read_customers(filepath))
