from customer import read_customers
from etl import Etl
import pytest
import os


@pytest.mark.parametrize("erasure_request, test_name", [
    ('{"customer-id": "347984"}', "test_erasure_request_with_customer_id"),
    ('{"email": "hollymillar@example.org"}', "test_erasure_request_with_email")
])
# WHEN: An erasure request is received.
# RESULT: The corresponding customer info should be hashed.
def test_erasure_request(erasure_request, test_name):
    # PREPARE
    etl = Etl()
    # Load customer dataset
    customer_text = """
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
    }"""
    etl.load_customer_from_string(customer_text)

    # ACT
    etl.load_erasure_request_from_string(erasure_request)
    test_output_folder = os.path.join("test-output", test_name)
    etl.save(test_output_folder)

    # ASSERT
    # Check that customer personally identifiable fields have all been changed to different values of the same format.
    erasure_request_customer_id = 347984
    requested_customer_has_been_erased = False
    for customer in read_customers(os.path.join(test_output_folder, "customers.parquet")):
        if customer.id == erasure_request_customer_id:
            # These fields are personally identifiable information, and should be anonymized.
            assert customer.first_name != "Georgia"
            assert customer.last_name != "Lewis"
            assert customer.date_of_birth != "2009-09-27"
            assert customer.email != "hollymillar@example.org"
            assert customer.phone_number != "01632 960 972"
            assert customer.address != "Studio 99\nMorley tunnel"
            assert customer.postcode != "E09 9TW"

            # These fields are not directly personally identifiable information, in this exercise I've chosen not anonymize them
            assert customer.city == "Alana Ville"
            assert customer.country == "United Kingdom"
            assert customer.last_change == "2020-03-12"
            assert customer.segment == "sports"

            requested_customer_has_been_erased = True
            break

    assert requested_customer_has_been_erased
