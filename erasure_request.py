from dataclasses import dataclass
import json


@dataclass
class ErasureRequest:
    customer_id: int = None
    email: str = None

    def __post_init__(self):
        # Enforce type
        if self.customer_id is not None:
            self.customer_id = int(self.customer_id)

        # Mandatory fields
        assert self.customer_id is not None or \
            self.email is not None, \
            "customer-id or email must be set for an erasure request."

    @classmethod
    def from_string(cls, erasure_request_string: str):
        erasure_request_json = json.loads(erasure_request_string)

        customer_id = erasure_request_json.get("customer-id")
        email = erasure_request_json.get("email")

        erasure_request = cls(customer_id=customer_id, email=email)
        return erasure_request
