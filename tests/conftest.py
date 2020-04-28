import pytest
from . import run_server
class Data:
    def __init__(self):
        self.valid_ids = ['123456']
        self.locations_in =  [{ "lat": 37.7739, "long": -122.4312 }]
        self.locations_out = [{ "lat": 99.9999, "long": -99.9999 }]
        self.locations_box = { "minLat": 37.70, 'maxLat': 37.80, 'minLong': -122.44, 'maxLong': -122.43}
        return

@pytest.fixture(scope = "session")
def data():
    return Data()

@pytest.fixture(scope = "session")
def server():
    yield from run_server()



