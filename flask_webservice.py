"""
Launch with:
FLASK_APP=flask_webservice.py flask run

Send a test post request with:
curl --header "Content-Type: application/json" \
    --request POST \
    --data '{"id": "347984", "first_name": "Georgia", "last_name": "Lewis", "date_of_birth": "2009-09-27", "email": "hollymillar@example.org", "phone_number": "01632 960 972", "address": "Studio 99\nMorley tunnel", "city": "Alana Ville", "country": "United Kingdom", "postcode": "E09 9TW", "last_change": "2020-03-12", "segment": "sports"}' \
     http://localhost:5000/customer
"""
from flask import Flask, request
from json import dumps
from etl import Etl

etl = Etl()
app = Flask(__name__)

@app.route("/customer", methods=['POST'])
def etl_customer():
    customer_json = request.json
    customer_json_string = dumps(customer_json)
    try:
        etl.load_customer_from_string(customer_json_string)
        return "", 200
    except Exception as e:
        return str(e), 400

@app.route("/product", methods=['POST'])
def etl_product():
    product_json = request.json
    product_json_string = dumps(product_json)
    try:
        etl.load_product_from_string(product_json_string)
        return "", 200
    except Exception as e:
        return str(e), 400

@app.route("/transaction", methods=['POST'])
def etl_transaction():
    transaction_json = request.json
    transaction_json_string = dumps(transaction_json)
    try:
        etl.load_transaction_from_string(transaction_json_string)
        return "", 200
    except Exception as e:
        return str(e), 400

@app.route("/erasure-request", methods=['POST'])
def etl_erasure_request():
    erasure_request_json = request.json
    erasure_request_json_string = dumps(erasure_request_json)
    try:
        etl.load_erasure_request_from_string(erasure_request_json_string)
        return "", 200
    except Exception as e:
        return str(e), 400
