from etl import Etl


def test_count_rows_empty():
    # ACT
    etl = Etl()

    # ASSERT
    assert etl.customer_count() == 0, "No data was loaded."
    assert etl.product_count() == 0, "No data was loaded."
    assert etl.transaction_count() == 0, "No data was loaded."
    assert etl.erasure_request_count() == 0, "No data was loaded."
    assert etl.rejected_input_count() == 0, "No data was loaded."


def test_count_rows(request):
    # PREPARE
    etl = Etl()

    # ACT
    etl.load_from_file("test-data/date=2020-01-01/hour=00")

    # ASSERT
    assert etl.customer_count() == 12
    assert etl.product_count() == 770
    assert etl.transaction_count() == 12
    assert etl.erasure_request_count() == 0
    assert etl.rejected_input_count() == 2
