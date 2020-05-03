import random
import logging
import time
import copy
import math
from lib import new_nonce, update_token, replacement_token

logger = logging.getLogger(__name__)

scale1meter = 100000  # Approx this many meters in lat or long
STATUS_INFECTED = 1
STATUS_PUI = 2
STATUS_UNKNOWN = 3
STATUS_HEALTHY = 4


# Note - 1 meter at the equator (and we start at 0,0) is approx 0.00001 degree of long
class Client:

    def __init__(self, server=None, data=None, name="Unnamed", **kwargs):
        # TODO-MITRA - check prefix length - when is it bits and when characters
        self.prefix_length = 8  # How many characters to use in prefix - see issue#34 for changing this prefix length automatically
        self.id_length = 32  # How many characters in hex id. Normally would be 128 bits = 32 chars
        self.safe_distance = 2  # Meters
        self.location_resolution = 4  # lat/long decimal points
        self.bounding_box_minimum_dp = 2  # Updated after init
        self.bounding_box_maximum_dp = 3  # Do not let the server require resolution requests > ~100

        # Access generic test functions and data
        self.server = server
        self.data = data
        self.name = name

        # Initialize arrays where we remember things
        self.ids_used = []
        self.observed_ids = []
        self.location_alerts = []
        self.id_alerts = []
        self.locations = []

        # Setup initial status
        self.new_id()
        self.move_to({'lat': 0, 'long': 0})
        self.length = 0
        self.status = STATUS_HEALTHY  # Healthy
        self.nonce = None  # Nonce on any status that might need updating
        self.since = None

    def new_id(self):
        self.current_id = "%X" % random.randrange(0, 2 ** 128)
        self.ids_used.append(self.current_id)

    def move_to(self, loc):
        self.locations.append(loc)
        self.current_location = loc

    def _min(self, a):
        return math.floor(min(a) * 10**self.bounding_box_minimum_dp) * 10**(-self.bounding_box_minimum_dp)

    def _max(self, a):
        return math.ceil(max(a) * 10**self.bounding_box_minimum_dp) * 10**(-self.bounding_box_minimum_dp)

    def _box(self):
        return {
            'min_lat': self._min(loc['lat'] for loc in self.locations),
            'min_long': self._min(loc['long'] for loc in self.locations),
            'max_lat': self._max(loc['lat'] for loc in self.locations),
            'max_long': self._max(loc['long'] for loc in self.locations),
        }

    def _prefixes(self):
        return [i[:self.prefix_length] for i in self.ids_used]

    # This is a very crude "_close_to" function, could obviously be much better.
    def _close_to(self, otherloc, loc):
        return (abs(otherloc['lat'] - loc['lat']) + abs(otherloc['long'] - loc['long'])) * scale1meter <= self.safe_distance

    # Received location matches if its close to any of the locations I have been to
    def _location_match(self, loc):
        return any(self._close_to(pastloc, loc) for pastloc in self.locations)

    def poll(self):
        json_data = self.server.scan_status_json(contact_prefixes=self._prefixes(), locations=[self._box()],
                                                 since=self.since)
        logging.info("poll result: %s" % (str(json_data)))
        self.since = json_data.get('until')

        self.id_alerts.extend([i for i in json_data['contact_ids'] if (i.get('id') in self.ids_used)])

        # Filter incoming location updates for those close to where we have been,
        # but exclude any of our own (based on matching update_token
        existing_location_updatetokens = [loc.get('update_token') for loc in self.locations]
        self.location_alerts.extend(
            filter(
                lambda loc: self._location_match(loc) and not loc.get('update_token') in existing_location_updatetokens,
                json_data['locations']))

        # Find the replaces tokens for both ids and locations - these are the locations this data point replaces
        id_replaces = [i.get('replaces') for i in self.id_alerts if i.get('replaces')]
        location_replaces = [loc.get('replaces') for loc in self.location_alerts if loc.get('replaces')]

        # Find update_tokens that have been replaced
        id_updatetokens = [update_token(rt) for rt in id_replaces]
        location_updatetokens = [update_token(rt) for rt in location_replaces]

        # Mark any ids or locations that have been replaced
        for i in self.id_alerts:
            if i.get('update_token') in id_updatetokens:
                i['replaced'] = True
        for loc in self.location_alerts:
            if loc.get('update_token') in location_updatetokens:
                loc['replaced'] = True

        # New status is minimum of any statuses that haven't been replaced +1
        # (e.g. if user is Infected (1) we will be PUI (2)
        new_status = min([i['status'] + 1 for i in self.id_alerts if not i.get('replaced')]
                         + [loc['status'] + 1 for loc in self.location_alerts if not loc.get('replaced')]
                         + [STATUS_HEALTHY])
        self.update_status(new_status)  # Correctly handles case of no change

    # Simulate broadcasting an id
    def broadcast(self):
        return self.current_id

    # Simulate hearing an id
    def listen(self, contact_id):
        self.observed_ids.append({'id': contact_id, 'duration': 15})

    def _preprocessed_locations(self, location):
        loc = copy.deepcopy(location)
        for k in ['lat', 'long']:
            loc[k] = round(location[k], self.location_resolution)
        return loc

    def next_updatetoken(self):
        if not self.nonce:
            self.nonce = new_nonce()
            self.length = 0
        ut = update_token(replacement_token(self.nonce, self.length))
        self.length += 1
        return ut

    # Send current status to server (on change of status)
    def update_status(self, new_status):
        if new_status != self.status:  # Its changed
            self.status = new_status
            replaces = self.nonce  # Will be None the first time
            self.nonce = new_nonce()
            if replaces:
                length = len(self.locations) + len(self.observed_ids)
                json_data = self.server.status_update_json(status=self.status, nonce=self.nonce, replaces=replaces, length=length)
                logging.info("status/update result: %s" % (str(json_data)))
            else:
                # Store the update_tokens on these ids, it will allow us to deduplicate echos
                # TODO depending on tests, might want to only update and send ones not already sent
                for o in self.observed_ids:
                    o['update_token'] = self.next_updatetoken()
                for loc in self.locations:
                    loc['update_token'] = self.next_updatetoken()
                json_data = self.server.send_status_json(
                    contacts=copy.deepcopy(self.observed_ids),
                    locations=[self._preprocessed_locations(loc) for loc in self.locations],
                    status=self.status, nonce=self.nonce, replaces=replaces)
                logging.info("status/send result: %s" % (str(json_data)))

    # Action taken every 15 seconds
    def cron15(self):
        self.new_id()  # Rotate id

    # Action taken every hour - check for updates
    def cron_hourly(self):
        self.poll()

    # Randomly move up to distance meters in any direction
    def random_walk(self, distance):
        self.move_to({
            'lat': self.current_location.get('lat') + random.randrange(-distance, distance) / (scale1meter),
            'long': self.current_location.get('long') + random.randrange(-distance, distance) / (scale1meter)
        })

    # Simulate what happens when this client "observes" another, i.e. here's its bluetooth
    def observes(self, other):
        self.listen(other.broadcast())

    def init(self, json_data):
        self.init_resp = self.server.init(json_data)
        self.bounding_box_minimum_dp = min(self.bounding_box_maximum_dp,
                                           self.init_resp.get('bounding_box_minimum_dp', self.bounding_box_minimum_dp))
        self.location_resolution = self.init_resp.get('location_resolution', self.location_resolution)
        self.prefix_length = self.init_resp.get('prefix_length', self.prefix_length)


def test_pseudoclient_2people(server, data):
    server.reset()
    logging.info('Started test_pseudoclient_work')
    alice = Client(server=server, data=data, name="Alice")
    alice.init(data.init_req)
    bob = Client(server=server, data=data, name="Bob")
    bob.init(data.init_req)
    bob.random_walk(10)  # Bob has been in two locations now
    alice.observes(bob)
    bob.observes(alice)
    alice.update_status(STATUS_INFECTED)
    time.sleep(1)
    bob.cron_hourly()  # Bob polls and should see alice
    assert len(bob.id_alerts) == 1
    assert len(bob.location_alerts) == 1
    assert bob.status == STATUS_PUI
    time.sleep(1)  # Make sure its a new time slot
    bob.cron_hourly()  # Bob polls and should get its own report back
    # time.sleep(1.5) # Make sure its a new time slot
    alice.update_status(STATUS_HEALTHY)
    time.sleep(1)
    bob.cron_hourly()  # Bob polls and should get the update from alice
    assert len(bob.id_alerts) == 2
    assert len(bob.location_alerts) == 2
    assert bob.status == STATUS_HEALTHY
    logging.info('Completed test_pseudoclient_work')

def test_pseudoclient_work(server, data):
    numberOfClients = 3
    chanceOfWalking = 1
    chanceOfInfection = 4
    steps = 10
    clients = []
    logging.info("Creating %s clients" % numberOfClients)
    for i in range(numberOfClients):
        c = Client(server=server, data=data, name = str(i))
        c.init(data.init_req)
        clients.append(c)
    for steps in range(steps):
        logging.info("===STEP %s ====", steps)
        for c in clients:
            if not random.randrange(0, chanceOfWalking):
                c.random_walk(10)
                logging.info("%s: walked to %.5fN,%.5fW" % (c.name, c.current_location['lat'], c.current_location['long']))
            for o in clients:
                #time.sleep(1)
                if o != c: # Skip self
                    if o._close_to(o.current_location, c.current_location):
                        c.observes(o)
                        logging.info("%s: observed %s" % (c.name, o.name))
            if not random.randrange(0, chanceOfInfection):
                c.update_status(STATUS_PUI)
                logging.info("%s: is PUI" % (c.name))
            c.cron_hourly()


