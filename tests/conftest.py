import pytest
from . import run_server


class Data:
    def __init__(self):
        self.valid_ids = ['123456']
        self.locations_in = [{"lat": 37.7739, "long": -122.4312}]
        self.locations_out = [{"lat": 99.9999, "long": -99.9999}]
        self.locations_box = {"min_lat": 37.70, 'max_lat': 37.80, 'min_long': -122.44, 'max_long': -122.43}
        self.init_req = {
            "application_name": "testing",
            "application_version": 1.0,
            "phone_type": "laptop",
            "region": "us/ca/sanfrancisco",
            "health_provider": "kaiser",
            "up_time_percentage": 80,
            "language": "en_US"
        }
        return


@pytest.fixture(scope="session")
def data():
    return Data()


@pytest.fixture(scope="session")
def server():
    yield from run_server()
