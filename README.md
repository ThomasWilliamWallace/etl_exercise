# ETL exercise

### Setup

First unzip the `test-data.zip` file locally, with
`unzip test-data.zip`

### Usage

The customer / product / transaction / erasure-request data is processed via the `Etl` class. \
The Etl class reads in jsons, and produces customer, product and transaction parquet files in it's output folder. \
There will also be a `rejected_input.txt` file, containing all jsons which were rejected during processing.

#### Process a folder
It can process the test data folder, used like:
```python
from etl import Etl
etl = Etl()
etl.load_from_file("test-data")
etl.save("output-folder")
```
See `test_etl_large_dataset` for an example of this.

#### Process a file
It can process a single json gzip, used like:
```python
from etl import Etl
etl = Etl()
etl.load_from_file("test-data/date=2020-01-01/hour=00/customers.json.gz")
etl.save("output-folder")
```
See `test_etl_customers` for an example of this.

#### Process a json string
It can process a single json, used like:
```python
product_json = """
{
  "sku": 23822,
  "name": "PHidyNvZH",
  "price": "25.00",
  "category": "vitamin",
  "popularity": 0.746141024720593
}
"""

from etl import Etl
etl = Etl()
etl.load_product_from_string(product_json)
etl.save("output-folder")
```

#### Process data as it arrives
It supports the processing of json strings as they arrive. \
For an example using Flask webservice, see `flask_webservice.py`.

## Notes
* This ETL method only data that can fit in-memory. To handle out-of-memory datasets, we could save partial results to disk, eg we could save a new transaction parquet file for every X rows, allowing us to clear those rows from memory.
* This ETL program hashes personal identifying info in response to erasure requests. The anonymization offered by hashing is limited- so it may be better to simply substitute or erase that data entirely.
