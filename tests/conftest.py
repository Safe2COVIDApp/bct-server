import pytest
from . import run_server
class Data:
    def __init__(self):
        self.valid_ids = ['123456']
        self.locations_in =  [{ "lat": 37.773972, "long": -122.431297 }]
        self.locations_out = [{ "lat": 99.9999, "long": -99.999 }]
        self.locations_box = { "minLat": 37, 'maxLat': 39, 'minLong': -123, 'maxLong': -122}
        return

@pytest.fixture(scope = "session")
def data():
    return Data()

@pytest.fixture(scope = "session")
def server():
    yield from run_server()



