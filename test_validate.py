from erasure_request import ErasureRequest
from transaction import Transaction
from datetime import datetime
from customer import Customer
from product import Product
from decimal import Decimal
from etl import Etl
import pytest
import os


############# Test parsing valid inputs #############
def test_parse_customer():
    # PREPARE
    customer_json = """
    {
      "id": "347984",
      "first_name": "Georgia",
      "last_name": "Lewis",
      "date_of_birth": "2009-09-27",
      "email": "hollymillar@example.org",
      "phone_number": "01632 960 972",
      "address": "Studio 99\nMorley tunnel",
      "city": "Alana Ville",
      "country": "United Kingdom",
      "postcode": "E09 9TW",
      "last_change": "2020-03-12",
      "segment": "sports"
    }
    """

    # ACT
    customer = Customer.from_string(customer_json)

    # ASSERT
    customer.id == 347984
    customer.first_name == "Georgia"
    customer.last_name == "Lewis"
    customer.date_of_birth == datetime(year=2009, month=9, day=27)
    customer.email == "hollymillar@example.org"
    customer.phone_number == "01632 960 972"
    customer.address == "Studio 99\nMorley tunnel"
    customer.city == "Alana Ville"
    customer.country == "United Kingdom"
    customer.postcode == "E09 9TW"
    customer.last_change == "2020-03-12"
    customer.segment == "sports"


def test_parse_product():
    # PREPARE
    product_json = """
    {
      "sku": 23822,
      "name": "PHidyNvZH",
      "price": "25.00",
      "category": "vitamin",
      "popularity": 0.746141024720593
    }
    """

    # ACT
    product = Product.from_string(product_json)

    # ASSERT
    product.sku == 23822
    product.name == "PHidyNvZH"
    product.price == Decimal("25.00")
    product.category == "vitamin"
    product.popularity == 0.746141024720593


def test_parse_transaction():
    # PREPARE
    transaction_json = """
    {
      "transaction_id": "6a8bb2c0-02f5-467a-8c83-6bb9a8b192b1",
      "transaction_time": "2022-07-01T16:05:08.618160",
      "customer_id": "325795",
      "delivery_address": {
        "address": "275 Nicole fall",
        "postcode": "E90 2FT",
        "city": "Maria Ville",
        "country": "United Kingdom"
      },
      "purchases": {
        "products": [
          {
            "sku": 71227,
            "quanitity": 1,
            "price": "30.98",
            "total": "30.98"
          }
        ],
        "total_cost": "30.98"
      }
    }
    """

    # ACT
    transaction = Transaction.from_string(transaction_json)

    # PREPARE
    assert transaction.transaction_id == "6a8bb2c0-02f5-467a-8c83-6bb9a8b192b1"
    assert transaction.transaction_time == datetime(year=2022, month=7, day=1, hour=16, minute=5, second=8, microsecond=618160)
    assert transaction.customer_id == 325795
    assert transaction.delivery_address["address"] == "275 Nicole fall"
    assert transaction.delivery_address["postcode"] == "E90 2FT"
    assert transaction.delivery_address["city"] == "Maria Ville"
    assert transaction.delivery_address["country"] == "United Kingdom"
    assert len(transaction.purchases.products) == 1
    assert transaction.purchases.products[0].sku == 71227
    assert transaction.purchases.products[0].quantity == 1
    assert transaction.purchases.products[0].price == Decimal("30.98")
    assert transaction.purchases.products[0].total == Decimal("30.98")
    assert transaction.purchases.total_cost == Decimal("30.98")


def test_parse_erasure_request():
    erasure_request_json = """
    {
      "customer-id": "325795",
      "email": "jasonmueller@example.com"
    }
    """
    ErasureRequest.from_string(erasure_request_json)


############# Test parsing invalid json #############
def test_customer_bad_json():
    customer_json = """
    """

    with pytest.raises(ValueError):
        Customer.from_string(customer_json)


def test_product_bad_json():
    product_json = """
    """

    with pytest.raises(ValueError):
        Product.from_string(product_json)


def test_transaction_bad_json():
    transaction_json = """
    """

    with (pytest.raises(ValueError)):
        Transaction.from_string(transaction_json)


def test_erasure_request_bad_json():
    erasure_request_json = """
    """

    with pytest.raises(ValueError):
        ErasureRequest.from_string(erasure_request_json)


############# Test enforce required fields #############
def test_customer_requires_id():
    customer_json = """
    {
      "first_name": "Georgia",
      "last_name": "Lewis",
      "email": "hollymillar@example.org"
    }
    """
    with pytest.raises(Exception):
        Customer.from_string(customer_json)


def test_customer_requires_first_name():
    customer_json = """
    {
      "id": "347984",
      "last_name": "Lewis",
      "email": "hollymillar@example.org"
    }
    """
    with pytest.raises(Exception):
        Customer.from_string(customer_json)


def test_customer_requires_last_name():
    customer_json = """
    {
      "id": "347984",
      "first_name": "Georgia",
      "email": "hollymillar@example.org"
    }
    """
    with pytest.raises(Exception):
        Customer.from_string(customer_json)


def test_customer_requires_email():
    customer_json = """
    {
      "id": "347984",
      "first_name": "Georgia",
      "last_name": "Lewis"
    }
    """
    with pytest.raises(Exception):
        Customer.from_string(customer_json)


def test_product_requires_sku():
    product_json = """
    {
      "name": "PHidyNvZH",
      "price": "25.00",
      "category": "vitamin",
      "popularity": 0.746141024720593
    }
    """
    with pytest.raises(Exception):
        Product.from_string(product_json)


def test_product_requires_name():
    product_json = """
    {
      "sku": 23822,
      "price": "25.00",
      "category": "vitamin",
      "popularity": 0.746141024720593
    }
    """
    with pytest.raises(Exception):
        Product.from_string(product_json)


def test_product_requires_price():
    product_json = """
    {
      "sku": 23822,
      "name": "PHidyNvZH",
      "category": "vitamin",
      "popularity": 0.746141024720593
    }
    """
    with pytest.raises(Exception):
        Product.from_string(product_json)


def test_product_requires_category():
    product_json = """
    {
      "sku": 23822,
      "name": "PHidyNvZH",
      "price": "25.00",
      "popularity": 0.746141024720593
    }
    """
    with pytest.raises(Exception):
        Product.from_string(product_json)


def test_product_requires_popularity():
    product_json = """
    {
      "sku": 23822,
      "name": "PHidyNvZH",
      "price": "25.00",
      "category": "vitamin"
    }
    """
    with pytest.raises(Exception):
        Product.from_string(product_json)


def test_transaction_requires_transaction_id():
    transaction_json = """
    {
      "transaction_time": "2022-07-01T16:05:08.618160",
      "customer_id": "325795",
      "delivery_address": {
        "address": "275 Nicole fall",
        "postcode": "E90 2FT",
        "city": "Maria Ville",
        "country": "United Kingdom"
      },
      "purchases": {
        "products": [
          {
            "sku": 71227,
            "quanitity": 1,
            "price": "30.98",
            "total": "30.98"
          }
        ],
        "total_cost": "30.98"
      }
    }
    """
    with pytest.raises(Exception):
        Transaction.from_string(transaction_json)


def test_transaction_requires_customer_id():
    transaction_json = """
    {
      "transaction_id": "6a8bb2c0-02f5-467a-8c83-6bb9a8b192b1",
      "transaction_time": "2022-07-01T16:05:08.618160",
      "delivery_address": {
        "address": "275 Nicole fall",
        "postcode": "E90 2FT",
        "city": "Maria Ville",
        "country": "United Kingdom"
      },
      "purchases": {
        "products": [
          {
            "sku": 71227,
            "quanitity": 1,
            "price": "30.98",
            "total": "30.98"
          }
        ],
        "total_cost": "30.98"
      }
    }
    """
    with pytest.raises(Exception):
        Transaction.from_string(transaction_json)


def test_transaction_requires_skus():
    transaction_json = """
    {
      "transaction_id": "6a8bb2c0-02f5-467a-8c83-6bb9a8b192b1",
      "transaction_time": "2022-07-01T16:05:08.618160",
      "customer_id": "325795",
      "delivery_address": {
        "address": "275 Nicole fall",
        "postcode": "E90 2FT",
        "city": "Maria Ville",
        "country": "United Kingdom"
      },
      "purchases": {
        "products": [
          {
            "quanitity": 1,
            "price": "30.98",
            "total": "30.98"
          }
        ],
        "total_cost": "30.98"
      }
    }
    """
    with pytest.raises(Exception):
        Transaction.from_string(transaction_json)


def test_transaction_requires_total_cost():
    transaction_json = """
    {
      "transaction_id": "6a8bb2c0-02f5-467a-8c83-6bb9a8b192b1",
      "transaction_time": "2022-07-01T16:05:08.618160",
      "customer_id": "325795",
      "delivery_address": {
        "address": "275 Nicole fall",
        "postcode": "E90 2FT",
        "city": "Maria Ville",
        "country": "United Kingdom"
      },
      "purchases": {
        "products": [
          {
            "sku": 71227,
            "quanitity": 1,
            "price": "30.98",
            "total": "30.98"
          }
        ]
      }
    }
    """
    with pytest.raises(Exception):
        Transaction.from_string(transaction_json)


############# Test for non-unique primary keys #############
def test_customer_id_uniqueness():
    # PREPARE
    test_data_path = "test-data/date=2020-01-01/hour=00/customers.json.gz"
    etl = Etl()
    etl.load_from_file(test_data_path)
    previous_accepted_customer_count = etl.customer_count()
    previous_rejected_customer_count = etl.rejected_input_count()

    # ACT
    etl.load_from_file(test_data_path)

    # ASSERT
    accepted_customer_count = etl.customer_count()
    rejected_customer_count = etl.rejected_input_count()
    assert accepted_customer_count == previous_accepted_customer_count, "No new customers have been added, because we've tried to re-add existing customers, and their ids are not unique."
    assert rejected_customer_count == previous_accepted_customer_count + 2 * previous_rejected_customer_count, "All the new duplicate customers have been rejected."


def test_product_sku_uniqueness():
    # PREPARE
    test_data_path = "test-data/date=2020-01-01/hour=00/products.json.gz"
    etl = Etl()
    etl.load_from_file(test_data_path)
    previous_accepted_product_count = etl.product_count()
    previous_rejected_product_count = etl.rejected_input_count()

    # ACT
    etl.load_from_file(test_data_path)

    # ASSERT
    accepted_product_count = etl.product_count()
    rejected_product_count = etl.rejected_input_count()
    assert accepted_product_count == previous_accepted_product_count, "No new products have been added, because we've tried to re-add existing products, and their skus are not unique."
    assert rejected_product_count == previous_accepted_product_count + 2 * previous_rejected_product_count, "All the new duplicate products have been rejected."


def test_transaction_id_uniqueness():
    # PREPARE
    test_data_path = "test-data/date=2020-01-01/hour=00"
    etl = Etl()
    etl.load_from_file(test_data_path)
    previous_accepted_transaction_count = etl.transaction_count()
    previous_rejected_count = etl.rejected_input_count()
    previous_rejected_transaction_count = 0  # In the "test-data/date=2020-01-01/hour=00/transactions.json.gz", there were no rejected transactions.

    # ACT
    duplicate_transactions_file = "test-data/date=2020-01-01/hour=00/transactions.json.gz"
    etl.load_from_file(duplicate_transactions_file)

    # ASSERT
    accepted_transaction_count = etl.transaction_count()
    rejected_transaction_count = etl.rejected_input_count()
    assert accepted_transaction_count == previous_accepted_transaction_count, "No new transactions have been added, because we've tried to re-add existing transactions, and their transaction ids are not unique."
    assert rejected_transaction_count == previous_accepted_transaction_count + previous_rejected_count + previous_rejected_transaction_count, "All the new duplicate transactions have been rejected."


############# Test for invalid foreign keys #############

# WHEN: Process transactions for which the referenced customers do not exist.
#       (A later set of customers has been added instead, to provide some non-matching customers.)
# RESULT: The transactions are rejected.
def test_transaction_references_existing_customer():
    # PREPARE
    etl = Etl()
    etl.load_from_file("test-data/date=2020-01-01/hour=01/customers.json.gz")
    etl.load_from_file("test-data/date=2020-01-01/hour=00/products.json.gz")
    rejected_nontransaction_count = etl.rejected_input_count()

    # ACT
    etl.load_from_file("test-data/date=2020-01-01/hour=00/transactions.json.gz")

    # ASSERT
    rejected_transaction_count = etl.rejected_input_count() - rejected_nontransaction_count
    assert etl.transaction_count() == 0, "All the transactions were rejected, because they have no customers to reference."
    assert rejected_transaction_count == 12, "All 12 of the transactions have been rejected."


# WHEN: Process transactions for which the referenced products do not exist.
#       (A later set of products has been added instead, to provide some non-matching products.)
# RESULT: The transactions are rejected.
def test_transaction_references_existing_product():
    # PREPARE
    etl = Etl()
    etl.load_from_file("test-data/date=2020-01-01/hour=00/customers.json.gz")
    etl.load_from_file("test-data/date=2020-01-01/hour=01/products.json.gz")
    rejected_nontransaction_count = etl.rejected_input_count()

    # ACT
    etl.load_from_file("test-data/date=2020-01-01/hour=00/transactions.json.gz")

    # ASSERT
    rejected_transaction_count = etl.rejected_input_count() - rejected_nontransaction_count
    assert etl.transaction_count() == 0, "All the transactions were rejected, because they have no products to reference."
    assert rejected_transaction_count == 12, "All 12 of the transactions have been rejected."


############# Test enforce field constraints #############
def test_product_price_must_be_positive():
    product_json = """
    {
      "sku": 23822,
      "name": "PHidyNvZH",
      "price": "-25.00",
      "category": "vitamin",
      "popularity": 0.746141024720593
    }
    """
    with pytest.raises(Exception):
        Product.from_string(product_json)


def test_product_popularity_must_be_greater_than_zero():
    product_json = """
    {
      "sku": 23822,
      "name": "PHidyNvZH",
      "price": "25.00",
      "category": "vitamin",
      "popularity": 0
    }
    """
    with pytest.raises(Exception):
        Product.from_string(product_json)


# Reject any transaction for which the totals do not sum to the total cost.
def test_transaction_totals_must_sum_to_total_cost():
    transaction_json = """
    {
      "transaction_id": "6a8bb2c0-02f5-467a-8c83-6bb9a8b192b1",
      "transaction_time": "2022-07-01T16:05:08.618160",
      "customer_id": "325795",
      "delivery_address": {
        "address": "275 Nicole fall",
        "postcode": "E90 2FT",
        "city": "Maria Ville",
        "country": "United Kingdom"
      },
      "purchases": {
        "products": [
          {
            "sku": 71227,
            "quanitity": 1,
            "price": "30.98",
            "total": "30.98"
          },
          {
            "sku": 71228,
            "quanitity": 2,
            "price": "1.23",
            "total": "2.46"
          }
        ],
        "total_cost": "30.98"
      }
    }
    """
    with pytest.raises(Exception):
        Transaction.from_string(transaction_json)
