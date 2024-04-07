from transaction import read_all_transactions
from customer import read_all_customers
from product import read_all_products
from datetime import datetime
from decimal import Decimal
from etl import Etl
import os


# WHEN: The etl process loads an empty dataset.
# RESULT: An empty output file is created.
def test_etl_empty(request):
    # PREPARE
    test_output_folder = os.path.join("test-output", request.node.name)

    # ACT
    etl = Etl()
    etl.save(test_output_folder)

    # ASSERT
    assert os.path.getsize(os.path.join(test_output_folder, "customers.parquet")) == 0
    assert os.path.getsize(os.path.join(test_output_folder, "products.parquet")) == 0
    assert os.path.getsize(os.path.join(test_output_folder, "transactions.parquet")) == 0


# WHEN: The etl process loads a dataset containing some customers.
# RESULT: An output file is created, holding the customers from the input data files.
def test_etl_customers(request):
    # PREPARE
    test_output_folder = os.path.join("test-output", request.node.name)
    etl = Etl()

    # ACT
    etl.load_customers_from_gzip_file("test-data/date=2020-01-01/hour=00/customers.json.gz")
    etl.save(test_output_folder)

    # ASSERT
    customers = read_all_customers(os.path.join(test_output_folder, "customers.parquet"))
    assert len(customers) == 12

    assert customers[0].id == 808920
    assert customers[0].first_name == "Julie-Anne"
    assert customers[0].last_name == "Lyons"
    assert customers[0].date_of_birth is None
    assert customers[0].email == "simmonsadrian@example.com"
    assert customers[0].phone_number == "+44(0)808 157 0808"
    assert customers[0].address == "81 Marilyn crossing"
    assert customers[0].city == "Emer Ville"
    assert customers[0].country == "United Kingdom"
    assert customers[0].postcode == "E7 1NS"
    assert customers[0].last_change == "2020-01-03"
    assert customers[0].segment == "health"

    assert customers[-1].id == 566096
    assert customers[-1].first_name == "Michael"
    assert customers[-1].last_name == "Powell"
    assert customers[-1].date_of_birth is None
    assert customers[-1].email == "swelch@example.com"
    assert customers[-1].phone_number == "+44(0)1174960658"
    assert customers[-1].address == "566 Edwards skyway"
    assert customers[-1].city == "Emilie Ville"
    assert customers[-1].country == "United Kingdom"
    assert customers[-1].postcode == "WV1H 0AH"
    assert customers[-1].last_change == "2020-01-24"
    assert customers[-1].segment == "sports"

    assert os.path.getsize(os.path.join(test_output_folder, "products.parquet")) == 0
    assert os.path.getsize(os.path.join(test_output_folder, "transactions.parquet")) == 0


# WHEN: The etl process loads a dataset containing a product.
# RESULT: An output file is created, holding the product from the input data files.
def test_etl_product(request):
    # PREPARE
    test_output_folder = os.path.join("test-output", request.node.name)
    etl = Etl()

    # ACT
    etl.load_products_from_gzip_file("test-data/date=2020-01-01/hour=00/products.json.gz")
    etl.save(test_output_folder)

    # ASSERT
    products = read_all_products(os.path.join(test_output_folder, "products.parquet"))
    assert len(products) == 770

    assert products[0].sku == 53248
    assert products[0].name == "whpVcnUvCL"
    assert products[0].price == Decimal("16.99")
    assert products[0].category == "misc"
    assert products[0].popularity == 0.8180098598621565

    assert products[-1].sku == 77823
    assert products[-1].name == "LrpsPpcDMKKG"
    assert products[-1].price == Decimal("5.10")
    assert products[-1].category == "vitamin"
    assert products[-1].popularity == 0.601400961207106

    assert os.path.getsize(os.path.join(test_output_folder, "customers.parquet")) == 0
    assert os.path.getsize(os.path.join(test_output_folder, "transactions.parquet")) == 0


# WHEN: The etl process loads a dataset containing a customer, product and transaction.
# RESULT: An output file is created, holding the customer, product and transaction from the input data files.
def test_etl_customer_product_and_transaction(request):
    # PREPARE
    test_output_folder = os.path.join("test-output", request.node.name)
    etl = Etl()

    # ACT
    etl.load_customers_from_gzip_file("test-data/date=2020-01-01/hour=00/customers.json.gz")
    etl.load_products_from_gzip_file("test-data/date=2020-01-01/hour=00/products.json.gz")
    etl.load_transactions_from_gzip_file("test-data/date=2020-01-01/hour=00/transactions.json.gz")
    etl.save(test_output_folder)

    # ASSERT
    customers = read_all_customers(os.path.join(test_output_folder, "customers.parquet"))
    assert len(customers) == 12

    assert customers[0].id == 808920
    assert customers[0].first_name == "Julie-Anne"
    assert customers[0].last_name == "Lyons"
    assert customers[0].date_of_birth is None
    assert customers[0].email == "simmonsadrian@example.com"
    assert customers[0].phone_number == "+44(0)808 157 0808"
    assert customers[0].address == "81 Marilyn crossing"
    assert customers[0].city == "Emer Ville"
    assert customers[0].country == "United Kingdom"
    assert customers[0].postcode == "E7 1NS"
    assert customers[0].last_change == "2020-01-03"
    assert customers[0].segment == "health"

    assert customers[-1].id == 566096
    assert customers[-1].first_name == "Michael"
    assert customers[-1].last_name == "Powell"
    assert customers[-1].date_of_birth is None
    assert customers[-1].email == "swelch@example.com"
    assert customers[-1].phone_number == "+44(0)1174960658"
    assert customers[-1].address == "566 Edwards skyway"
    assert customers[-1].city == "Emilie Ville"
    assert customers[-1].country == "United Kingdom"
    assert customers[-1].postcode == "WV1H 0AH"
    assert customers[-1].last_change == "2020-01-24"
    assert customers[-1].segment == "sports"

    products = read_all_products(os.path.join(test_output_folder, "products.parquet"))
    assert len(products) == 770

    assert products[0].sku == 53248
    assert products[0].name == "whpVcnUvCL"
    assert products[0].price == Decimal("16.99")
    assert products[0].category == "misc"
    assert products[0].popularity == 0.8180098598621565

    assert products[-1].sku == 77823
    assert products[-1].name == "LrpsPpcDMKKG"
    assert products[-1].price == Decimal("5.10")
    assert products[-1].category == "vitamin"
    assert products[-1].popularity == 0.601400961207106

    transactions = read_all_transactions(os.path.join(test_output_folder, "transactions.parquet"))
    assert len(transactions) == 12

    assert transactions[0].transaction_id == "fd386f34-86e2-469a-90d1-2cf3f6c75a96"
    assert transactions[0].customer_id == 826423
    assert transactions[0].transaction_time == datetime(year=2020, month=1, day=1, hour=0, minute=42, second=43, microsecond=585342)
    assert transactions[0].delivery_address == {"address": "17 Heffron Street", "postcode": "D06 EHHA", "city": "Mariastad", "country": "Republic of Ireland"}
    assert len(transactions[0].purchases.products) == 1
    assert transactions[0].purchases.products[0].sku == 99096
    assert transactions[0].purchases.products[0].quantity == 1
    assert transactions[0].purchases.products[0].price == Decimal("21.00")
    assert transactions[0].purchases.products[0].total == Decimal("21.00")
    assert transactions[0].purchases.total_cost == Decimal("21.00")

    assert transactions[-1].transaction_id == "7163ea16-785a-436f-9541-3c8b91928a67"
    assert transactions[-1].customer_id == 551580
    assert transactions[-1].transaction_time == datetime(year=2020, month=1, day=1, hour=0, minute=15, second=14, microsecond=822580)
    assert transactions[-1].delivery_address == {"address": "Flat 62T\nMann course", "postcode": "F34 13H6", "city": "Louismouth", "country": "United Kingdom"}
    assert len(transactions[-1].purchases.products) == 10
    assert transactions[-1].purchases.products[0].sku == 84590
    assert transactions[-1].purchases.products[0].quantity == 1
    assert transactions[-1].purchases.products[0].price == Decimal("13.98")
    assert transactions[-1].purchases.products[0].total == Decimal("13.98")
    assert transactions[-1].purchases.products[-1].sku == 68924
    assert transactions[-1].purchases.products[-1].quantity == 1
    assert transactions[-1].purchases.products[-1].price == Decimal("25.98")
    assert transactions[-1].purchases.products[-1].total == Decimal("25.98")
    assert transactions[-1].purchases.total_cost == Decimal("240.75")


def test_etl_excludes_invalid_data(request):
    # PREPARE
    test_data_path = "test-data/date=2020-01-07/hour=00/products.json.gz"
    test_output_folder = os.path.join("test-output", request.node.name)
    etl = Etl()

    # ACT
    etl.load_from_file(test_data_path)
    etl.save(test_output_folder)

    # ASSERT
    # Assert that '{"sku": 92796, "name": null, "price": "27.00", "category": "sports", "popularity": 0.36453071600830766}'
    # has been placed in the reject file due to the invalid field name == null, and is not in the accepted products.
    # Assert that '{"sku": 41885, "name": "HRLtXXDT", "price": "47.50", "category": "vitamin", "popularity": 0.0}'
    # has been placed in the reject file due to the invalid field popularity == 0, and is not in the accepted products.
    for product in read_all_products(os.path.join(test_output_folder, etl.products_output_filename)):
        assert product.sku != 92796, "This product is invalid and should not be included in the processed products file."
        assert product.sku != 41885, "This product is invalid and should not be included in the processed products file."
    with open(os.path.join(test_output_folder, etl.rejected_input_output_filename), mode='rb') as reject_file:
        rejected_inputs = reject_file.readlines()
        assert '"sku": 92796,' in str(rejected_inputs[0]), "The invalid product sku==92796 should be in the reject file."
        assert '"sku": 41885,' in str(rejected_inputs[1]), "The invalid product sku==41885 should be in the reject file."


# WHEN: The etl process loads a large and varied dataset.
# RESULT: An output file is created, holding the customers, products and transactions from the input data files.
def test_etl_large_dataset(request):
    # PREPARE
    test_data_path = "test-data"
    test_output_folder = os.path.join("test-output", request.node.name)
    etl = Etl()

    # ACT
    etl.load_from_file(test_data_path)
    etl.save(test_output_folder)

    # ASSERT
    customers = read_all_customers(os.path.join(test_output_folder, etl.customers_output_filename))
    products = read_all_products(os.path.join(test_output_folder, etl.products_output_filename))
    transactions = read_all_transactions(os.path.join(test_output_folder, etl.transactions_output_filename))
    with open(os.path.join(test_output_folder, etl.rejected_input_output_filename), mode='rb') as reject_file:
        rejected_input_lines = reject_file.readlines()

    # Assert output files contain numerous customers, products and transactions.
    # Compare against regression values.
    assert len(customers) == 762, "Number of customers should match across previous runs."
    assert len(products) == 2470, "Number of products should match across previous runs."
    assert len(transactions) == 9701, "Number of transactions should match across previous runs."
    assert len(rejected_input_lines) == 360, "Number of rejected inputs should match across previous runs."

    # Check logged stats
    assert etl.customer_count() == len(customers)
    assert etl.product_count() == len(products)
    assert etl.transaction_count() == len(transactions)
    assert etl.rejected_input_count() == len(rejected_input_lines)
