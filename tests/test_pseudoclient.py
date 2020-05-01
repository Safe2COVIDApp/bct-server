import random
import logging
import time
import copy
from lib import new_nonce, update_token, replacement_token

logger = logging.getLogger(__name__)

scale1meter = 100000 # Approx this many meters in lat or long
STATUS_INFECTED = 1
STATUS_PUI = 2
STATUS_UNKNOWN = 3
STATUS_HEALTHY = 4

# Note - 1 meter at the equator (and we start at 0,0) is approx 0.00001 degree of long
class Client:

    def __init__(self, server = None, data = None, **kwargs):
        self.prefix_length = 8  # How many characters to use in prefix - see issue#34 for changing this prefix length automatically
        self.id_length = 32 # How many characters in hex id. Normally would be 128 bits = 32 chars
        self.safe_distance = 2 # Meters
        self.location_resolution = 4 # lat/long decimal points
        self.bounding_box_minimum_dp = 2 # Updated after init
        self.bounding_box_maximum_dp = 3 # Dont let the server require resolution requests > ~100

        # Access generic test functions and data
        self.server = server
        self.data = data

        # Initialize arrays where we remember things
        self.ids_used = []
        self.observed_ids = []
        self.location_alerts = []
        self.id_alerts = []
        self.locations = []

        # Setup initial status
        self.new_id()
        self.move_to({ 'lat': 0, 'long': 0})
        self.length = 0
        self.status = STATUS_HEALTHY # Healthy
        self.nonce = None  # Nonce on any status that might need updating
        self.since = None

    def new_id(self):
        self.current_id = "%X" % random.randrange(0, 2**128)
        self.ids_used.append(self.current_id)

    def move_to(self, loc):
        self.locations.append(loc)
        self.current_location = loc

    def _box(self):
        return {
            'minLat': round(min([loc['lat'] for loc in self.locations]), self.bounding_box_minimum_dp),
            'minLong': round(min([loc['long'] for loc in self.locations]), self.bounding_box_minimum_dp),
            'maxLat': round(max([loc['lat'] for loc in self.locations]), self.bounding_box_minimum_dp),
            'maxLong': round(max([loc['long'] for loc in self.locations]), self.bounding_box_minimum_dp),
        }

    def _prefixes(self):
        return [ i[:self.prefix_length] for i in self.ids_used ]

    # This is a very crude "_close_to" function, could obviously be much better.
    def _close_to(self, l, loc):
        return abs(l['lat']-loc['lat'])+abs(l['long']-loc['long']) * scale1meter <= self.safe_distance

    # Received location matches if its close to any of the locations I have been to
    def _location_match(self,loc):
        return any(self._close_to(l, loc) for l in self.locations)

    def poll(self):
        json_data = self.server.scan_status_json(contact_prefixes=self._prefixes(), locations=[self._box()], since = self.since)
        self.since = json_data.get('now')

        self.id_alerts.extend([i for i in json_data['ids'] if (i.get('id') in self.ids_used)])

        # Filter incoming location updates for those close to where we have been, but exclude any of our own (based on matching updatetoken
        existing_location_updatetokens = [l.get('updatetoken') for l in self.locations]
        self.location_alerts.extend(
            filter(lambda loc: self._location_match(loc) and not loc.get('updatetoken') in existing_location_updatetokens,
                   json_data['locations']))

        # Find the replaces tokens for both ids and locations - these are the locations this data point replaces
        id_replaces = [ i.get('replaces') for i in self.id_alerts if i.get('replaces')]
        location_replaces = [ loc.get('replaces') for loc in self.location_alerts if loc.get('replaces')]

        # Find updatetokens that have been replaced
        id_updatetokens = [ update_token(rt) for rt in id_replaces ]
        location_updatetokens = [ update_token(rt) for rt in location_replaces ]

        # Mark any ids or locations that have been replaced
        for i in self.id_alerts:
            if i.get('updatetoken') in id_updatetokens:
                i['replaced'] = True
        for l in self.location_alerts:
            if l.get('updatetoken') in location_updatetokens:
                l['replaced'] = True

        # New status is minimum of any statuses that haven't been replaced +1 (e.g. if user is Infected (1) we will be PUI (2)
        new_status = min([i['status'] + 1 for i in self.id_alerts if not i.get('replaced')]
                         + [l['status']+ 1 for l in self.location_alerts if not l.get('replaced')]
                         + [STATUS_HEALTHY])
        self.update_status(new_status) # Correctly handles case of no change

    # Simulate broadcasting an id
    def broadcast(self):
        return self.current_id

    # Simulate hearing an id
    def listen(self, contact_id):
        self.observed_ids.append({'id': contact_id})

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
        if new_status != self.status: # Its changed
            self.status = new_status
            replaces = self.nonce # Will be None the first time
            self.nonce = new_nonce()
            if replaces:
                length = len(self.locations)+len(self.observed_ids)
                self.server.status_update_json(status=self.status, nonce=self.nonce, replaces=replaces, length=length)
            else:
                # Store the updatetokens on these ids, it will allow us to deduplicate echos
                # TODO depending on tests, might want to only update and send ones not already sent
                for o in self.observed_ids:
                    o['updatetoken'] = self.next_updatetoken()
                for l in self.locations:
                    l['updatetoken'] = self.next_updatetoken()
                self.server.send_status_json(
                    contacts=copy.deepcopy(self.observed_ids), locations=[ self._preprocessed_locations(loc) for loc in self.locations],
                    status=self.status, nonce=self.nonce, replaces = replaces)

    # Action taken every 15 seconds
    def cron15(self):
        self.new_id()   # Rotate id


    # Action taken every hour - check for updates
    def cron_hourly(self):
        self.poll()

    # Randomly move up to 10 meters in any direction
    def random_walk(self):
        self.move_to({
            'lat': self.current_location.get('lat') + random.randrange(-10, 10) / (10*scale1meter),
            'long': self.current_location.get('long') + random.randrange(-10, 10) / (10*scale1meter)
        })

    # Simulate what happens when this client "observes" another, i.e. here's its bluetooth
    def observes(self, other):
        self.listen(other.broadcast())

    def init(self, json_data):
        self.init_resp = self.server.init(json_data)
        self.bounding_box_minimum_dp = min(self.bounding_box_maximum_dp, self.init_resp.get('bounding_box_minimum_dp', self.bounding_box_minimum_dp)) # Update if found
        self.location_resolution = self.init_resp.get('location_resolution', self.location_resolution)
        self.prefix_length = self.init_resp.get('prefix_length', self.prefix_length)

def test_pseudoclient_work(server, data):
    server.reset()
    logging.info('Started test_pseudoclient_work')
    alice = Client(server = server, data = data)
    alice.init(data.init_req)
    bob = Client(server = server, data = data)
    bob.init(data.init_req)
    bob.random_walk() # Bob has been in two locations now
    alice.observes(bob)
    bob.observes(alice)
    alice.update_status(STATUS_INFECTED)
    time.sleep(1)
    bob.cron_hourly()  # Bob polls and should see alice
    assert len(bob.id_alerts) == 1
    assert len(bob.location_alerts) == 1
    assert bob.status == STATUS_PUI
    time.sleep(1) # Make sure its a new time slot
    bob.cron_hourly()  # Bob polls and should get its own report back
    #time.sleep(1.5) # Make sure its a new time slot
    alice.update_status(STATUS_HEALTHY)
    time.sleep(1)
    bob.cron_hourly()  # Bob polls and should get the update from alice
    assert len(bob.id_alerts) == 2
    assert len(bob.location_alerts) == 2
    assert bob.status == STATUS_HEALTHY
    logging.info('Completed test_pseudoclient_work')
