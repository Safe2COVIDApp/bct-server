from lib import random_ascii
import random
import logging
from lib import new_nonce

logger = logging.getLogger(__name__)

# Note - 1 meter at the equator (and we start at 0,0) is approx 0.00001 degree of long
class Client:

    def __init__(self, server = None, data = None, **kwargs):
        self.prefix_length = 4
        self.id_length = 10
        self.server = server
        self.data = data;
        self.ids_used = []
        self.new_id()
        self.current_location = { 'lat': 0, 'long': 0}
        self.locations = [self.current_location]
        self.observed_ids = []
        self.status = 4 # Healthy
        self.nonce = None  # Nonce on any status that might need updating
        self.location_alerts = []
        self.id_alerts = []

    def new_id(self):
        # TODO-50 move to getid method
        self.current_id = "%X" % random.randrange(0, 2**128)
        self.ids_used.append(self.current_id)

    def new_location(self, loc):
        self.locations.append(loc)
        self.current_location = loc

    def box(self):
        return {
            'minLat': min([loc['lat'] for loc in self.locations]),
            'minLong': min([loc['long'] for loc in self.locations]),
            'maxLat': max([loc['lat'] for loc in self.locations]),
            'maxLong': max([loc['long'] for loc in self.locations])
        }

    def _prefixes(self):
        return [ i[:self.prefix_length] for i in self.ids_used ]

    def poll(self):
        json_data = self.server.scan_status_json(contact_prefixes=self._prefixes(), locations=[self.box()])
        self.id_alerts.extend([i for i in json_data['ids'] if (i.get('id') in self.ids_used)])
        # TODO-50 replace with a test for within xx meters, and test against all my locations
        #self.location_alerts.extend([i for i in json_data['locations']
        #                                if  (i.get('lat') == data.locations_in[0].get('lat')) and
        #                                   (i.get('long') == data.locations_in[0].get('long'))])

    def broadcast(self):
        return self.current_id

    def listen(self, id):
        self.observed_ids.append({'id': id})

    def send_status(self):
        self.nonce = new_nonce()
        self.server.send_status_json(contacts=self.observed_ids, locations=self.locations, status=self.status, updatetoken=self.nonce)

    def infected(self):
        self.send_status()

    def cron15(self):
        self.new_id()

    def cron60(self):
        self.poll()

    def randwalk(self):
        scale1meter = 100000 # Approx this many meters in lat or long
        self.new_location({
            'lat': self.current_location.get('lat') + random.randrange(-scale1meter, scale1meter) / scale1meter,
            'long': self.current_location.get('long') + random.randrange(-scale1meter, scale1meter) / scale1meter
        })

    # Simulate what happens when this client "observes" another, i.e. here's its bluetooth
    def observes(self, other):
        self.listen(other.broadcast())


def test_pseudoclient_basic(server, data):
    pass

#def test_pseudoclient_twopeople(server, data):
def test_pseudoclient_work(server, data):
    logging.info('Started test_pseudoclient_twopeople')
    alice = Client(server = server, data = data)
    bob = Client(server = server, data = data)
    bob.randwalk()
    alice.observes(bob)
    bob.observes(alice)
    alice.infected()
    bob.cron60()  # Bob polls and should see alice
    assert len(bob.id_alerts) == 1
    logging.info('Completed test_pseudoclient_twopeople')
