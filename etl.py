from transaction import Transaction, Purchases
from erasure_request import ErasureRequest
from customer import Customer
from product import Product
import pyarrow.parquet as pq
from pathlib import Path
import pyarrow as pa
import gzip
import os


class Etl:
    customers_output_filename = "customers.parquet"
    products_output_filename = "products.parquet"
    transactions_output_filename = "transactions.parquet"
    rejected_input_output_filename = "rejected_input.txt"

    def __init__(self):
        self.customers = []
        self.products = []
        self.transactions = []
        self.erasure_requests = []  # Store erasure requests till time to write file, so we can anonymize any that arrive after current time?

        self.rejected_input = []

        # Mappings from primary key to array row.
        # Used to efficiently enforce primary key and foreign key constraints.
        self.customer_id_to_row = {}
        self.product_sku_to_row = {}
        self.transaction_id_to_row = {}

    def load_customer_from_string(self, customer_json: str):
        # Enforce uniqueness of customer id, using a dictionary for efficiency.
        customer = Customer.from_string(customer_json)
        if customer.id in self.customer_id_to_row:
            raise Exception(f"Customer id {customer.id} is already present in the customer data, duplicate rejected.")
        self.customer_id_to_row[customer.id] = len(self.customers)
        self.customers.append(customer)

    def load_customers_from_gzip_file(self, zipped_input_filepath: str):
        with gzip.open(zipped_input_filepath) as input_file:
            for line in input_file.readlines():
                try:
                    self.load_customer_from_string(line)
                except Exception as e:
                    print(e)
                    self.rejected_input.append(bytes(zipped_input_filepath+": ", 'utf-8')+line)

    def load_product_from_string(self, product_json: str):
        # Enforce uniqueness of product sku, using a dictionary for efficiency.
        product = Product.from_string(product_json)
        if product.sku in self.product_sku_to_row:
            raise Exception(f"Product SKU {product.sku} is already present in the product data, duplicate rejected.")
        self.product_sku_to_row[product.sku] = len(self.products)
        self.products.append(product)

    def load_products_from_gzip_file(self, zipped_input_filepath: str):
        with gzip.open(zipped_input_filepath) as input_file:
            for line in input_file.readlines():
                try:
                    self.load_product_from_string(line)
                except Exception as e:
                    print(e)
                    self.rejected_input.append(bytes(zipped_input_filepath+": ", 'utf-8')+line)

    def load_transaction_from_string(self, transaction_json: str):
        # Enforce uniqueness of transaction id, using a dictionary for efficiency.
        transaction = Transaction.from_string(transaction_json)
        if transaction.transaction_id in self.transaction_id_to_row:
            raise Exception(f"Transaction id {transaction.transaction_id} is already present in the transaction data, duplicate rejected.")

        # Enforce customer id must refer to a valid customer we have already processed.
        if transaction.customer_id not in self.customer_id_to_row:
            raise Exception(f"Transaction customer_id {transaction.customer_id} is not present in the customer data, transaction rejected (transaction id {transaction.transaction_id}).")

        # Enforce product sku must refer to a valid product we have already processed.
        for product_purchase in transaction.purchases.products:
            if product_purchase.sku not in self.product_sku_to_row:
                raise Exception(f"Transaction product SKU {product_purchase.sku} is not present in the product data, transaction rejected (transaction id {transaction.transaction_id}).")

        self.transaction_id_to_row[transaction.transaction_id] = len(self.transactions)
        self.transactions.append(transaction)

    def load_transactions_from_gzip_file(self, zipped_input_filepath: str):
        with gzip.open(zipped_input_filepath) as input_file:
            for line in input_file.readlines():
                try:
                    self.load_transaction_from_string(line)
                except Exception as e:
                    print(e)
                    self.rejected_input.append(bytes(zipped_input_filepath+": ", 'utf-8')+line)

    def load_erasure_request_from_string(self, erasure_request_json: str):
        self.erasure_requests.append(ErasureRequest.from_string(erasure_request_json))
        self.implement_erasure_request(self.erasure_requests[-1])

    def load_erasure_request_from_gzip_file(self, zipped_input_filepath: str):
        with gzip.open(zipped_input_filepath) as input_file:
            for line in input_file.readlines():
                try:
                    self.load_erasure_request_from_string(line)
                except Exception as e:
                    print(e)
                    self.rejected_input.append(bytes(zipped_input_filepath+": ", 'utf-8')+line)

    def load_from_file(self, input_path:str):
        if os.path.isfile(input_path):
            input_filepaths = [Path(input_path)]
        else:
            # Select all files in input folder
            unsorted_input_files = list(Path(input_path).glob('**/*.json.gz'))

            # Sort input files by time, with a processing order of customers, products, transactions, and then erasure requests.
            process_priority = {"customers.json.gz": "priority-1", "products.json.gz": "priority-2", "transactions.json.gz": "priority-3", "erasure-requests.json.gz": "priority-4"}
            input_filepaths = sorted(unsorted_input_files, key=lambda filepath: '/'.join(filepath.parts[0:-1]) + '/' + process_priority[filepath.parts[-1]])

        # Load each file
        for zipped_input_filepath in input_filepaths:
            zipped_input_file = zipped_input_filepath.name
            if zipped_input_file == "customers.json.gz":
                self.load_customers_from_gzip_file(str(zipped_input_filepath))
            elif zipped_input_file == "products.json.gz":
                self.load_products_from_gzip_file(str(zipped_input_filepath))
            elif zipped_input_file == "transactions.json.gz":
                self.load_transactions_from_gzip_file(str(zipped_input_filepath))
            elif zipped_input_file == "erasure-requests.json.gz":
                self.load_erasure_request_from_gzip_file(str(zipped_input_filepath))

    def implement_erasure_request(self, erasure_request: ErasureRequest):
        for customer in self.customers:
            if (
                (erasure_request.customer_id is not None and customer.id == erasure_request.customer_id) or
                (erasure_request.email is not None and customer.email == erasure_request.email)
            ):
                customer.anonymize()

    def save_customers(self, customers_filepath: str):
        if len(self.customers) > 0:
            customer_headers = list(vars(self.customers[0]).keys())
            customer_columns = [[vars(customer)[customer_key] for customer in self.customers] for customer_key in customer_headers]
            customer_table = pa.Table.from_arrays(customer_columns, names=customer_headers)
            pq.write_table(customer_table, customers_filepath, row_group_size=10000)
        else:
            with open(customers_filepath, mode='w'):
                pass

    def save_products(self, products_filepath: str):
        if len(self.products) > 0:
            product_headers = list(vars(self.products[0]).keys())
            product_columns = [[vars(product)[product_key] for product in self.products] for product_key in product_headers]
            product_table = pa.Table.from_arrays(product_columns, names=product_headers)
            pq.write_table(product_table, products_filepath, row_group_size=10000)
        else:
            with open(products_filepath, mode='w'):
                pass

    def save_transactions(self, transactions_filepath: str):
        if len(self.transactions) > 0:
            transaction_id = [transaction.transaction_id for transaction in self.transactions]
            customer_id = [transaction.customer_id for transaction in self.transactions]
            transaction_time = [transaction.transaction_time for transaction in self.transactions]
            delivery_address = [transaction.delivery_address for transaction in self.transactions]
            purchases = [transaction.purchases.to_parquet_data() for transaction in self.transactions]

            transactions_schema = pa.schema(Transaction.parquet_struct())

            data = [
                pa.array(transaction_id),
                pa.array(customer_id),
                pa.array(delivery_address),
                pa.array(transaction_time),
                pa.array(purchases, type=Purchases.parquet_struct())
            ]
            transaction_headers = list(vars(self.transactions[0]).keys())
            batch = pa.RecordBatch.from_arrays(data, transaction_headers)
            transaction_table = pa.Table.from_batches([batch], transactions_schema)
            pq.write_table(transaction_table, transactions_filepath, row_group_size=10000)
        else:
            with open(transactions_filepath, mode='w'):
                pass

    def save_rejected_input(self, rejected_input_filepath: str):
        with open(rejected_input_filepath, mode='wb') as output_file:
            output_file.writelines(self.rejected_input)

    def save(self, output_folder):
        os.makedirs(output_folder, exist_ok=True)

        customers_filepath = os.path.join(output_folder, self.customers_output_filename)
        products_filepath = os.path.join(output_folder, self.products_output_filename)
        transactions_filepath = os.path.join(output_folder, self.transactions_output_filename)
        rejected_input_filepath = os.path.join(output_folder, self.rejected_input_output_filename)

        self.save_customers(customers_filepath)
        self.save_products(products_filepath)
        self.save_transactions(transactions_filepath)
        self.save_rejected_input(rejected_input_filepath)

    def customer_count(self):
        return len(self.customers)

    def product_count(self):
        return len(self.products)

    def transaction_count(self):
        return len(self.transactions)

    def erasure_request_count(self):
        return len(self.erasure_requests)

    def rejected_input_count(self):
        return len(self.rejected_input)
