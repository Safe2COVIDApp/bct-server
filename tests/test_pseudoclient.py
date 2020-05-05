import random
import logging
import time
import copy
import math
from lib import new_seed, update_token, replacement_token

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
        self.seed = None  # Seed on any status that might need updating
        self.since = None

    def new_id(self):
        self.current_id = "%X" % random.randrange(0, 2 ** 128)
        self.ids_used.append(self.current_id)

    def move_to(self, loc):
        self.locations.append(loc)
        self.current_location = loc

    def _min(self, a):
        return math.floor(min(a) * 10 ** self.bounding_box_minimum_dp) * 10 ** (-self.bounding_box_minimum_dp)

    def _max(self, a):
        return math.ceil(max(a) * 10 ** self.bounding_box_minimum_dp) * 10 ** (-self.bounding_box_minimum_dp)

    def _box(self):
        return {
            'min_lat': self._min(loc['lat'] for loc in self.locations),
            'min_long': self._min(loc['long'] for loc in self.locations),
            'max_lat': self._max(loc['lat'] for loc in self.locations),
            'max_long': self._max(loc['long'] for loc in self.locations),
        }

    def _prefixes(self):
        return [i[:self.prefix_length] for i in self.ids_used]

    # This is a very crude "close_to" function, could obviously be much better.
    def close_to(self, otherloc, loc):
        return (abs(otherloc['lat'] - loc['lat']) + abs(
            otherloc['long'] - loc['long'])) * scale1meter <= self.safe_distance

    # Received location matches if its close to any of the locations I have been to
    def _location_match(self, loc):
        return any(self.close_to(pastloc, loc) for pastloc in self.locations)

    def poll(self):
        json_data = self.server.scan_status_json(contact_prefixes=self._prefixes(), locations=[self._box()],
                                                 since=self.since)
        logging.info("%s: poll result: %s" % (self.name, str(json_data)))
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

    def _prep_contact(self, c):
        c['update_token'] = self._next_updatetoken()
        return copy.deepcopy(c)

    def _prep_location(self, location):
        location['update_token'] = self._next_updatetoken()
        loc = copy.deepcopy(location)
        for k in ['lat', 'long']:
            loc[k] = round(location[k], self.location_resolution)
        return loc

    def _next_updatetoken(self):
        ut = update_token(replacement_token(self.seed, self.length))
        self.length += 1
        return ut

    def _send_to(self, new_status):
        # Store the update_tokens on these ids, it will allow us to deduplicate echos
        # TODO depending on tests, might want to only update and send ones not already sent
        logging.info("%s: status/send: %s" % (self.name, new_status))
        if not self.seed:
            self.seed = new_seed()
            self.length = 0
        json_data = self.server.send_status_json(
            contacts=[self._prep_contact(c) for c in self.observed_ids if not c.get('update_token')],
            locations=[self._prep_location(loc) for loc in self.locations if not loc.get('update_token')],
            status=new_status)
        logging.info("%s: status/send result: %s" % (self.name, str(json_data)))

    def _update_to(self, new_status):
        replaces = self.seed  # Will be None the first time
        self.seed = new_seed()
        length = len(self.locations) + len(self.observed_ids)
        json_data = self.server.status_update_json(status=new_status, seed=self.seed, replaces=replaces, length=length)
        logging.info("status/update result: %s" % (str(json_data)))
        self.seed = None  # Any further status change is a new incident, there is no mechanism to re-update

    # Set status - if its changed, then send current status to server (on change of status)
    # There are only a few valid transitions
    # 1: Alice Healthy or Unknown receives notification (from #2 or #4) becomes PUI - status/send causes Bob #5
    # 2: Alice PUI tests positive (goes to Infected) - status/update pushes Bob from Unknown (because of this client's #1) to PUI (that  #1)
    # 3: Alice PUI tests negative (goes to Healthy or Unknown) - status/update causes Bob #6
    # 4: Alice Healthy or Unknown tests directly positive (goes to Infected) - status/send pushes Bob Healthy or Unknown to PUI (see #1)
    # Or as a result of a poll.
    # 5: Bob receives notification (from Alice status/send or status/update #1) changes Healthy to Unknown - doesn't send anything out
    # 6: Bob receives notification (from Alice status/update #3) changes Unknown to Healthy, nothing sent
    # 8: Alice was Infected but recovers (Healthy, or another Unknown) - nothing sent
    # Infected -> PUI shouldn't happen
    def update_status(self, new_status):
        if self.status == STATUS_PUI and new_status != self.status:  # Cases #2 or #3 above, self.seed should be set
            assert self.seed
            self._update_to(new_status)
        # This covers cases #1 and #4 (H|U -> P|I) plus any newly observed ids or visited locations while P|I
        if new_status in [STATUS_PUI, STATUS_INFECTED] and (any(not c.get('update_token') for c in self.observed_ids) or any(not loc.get('update_token') for loc in self.locations)):
            self._send_to(new_status)
        self.status = new_status

    # Action taken every 15 seconds
    def cron15(self):
        self.new_id()  # Rotate id

    # Action taken every hour - check for updates
    def cron_hourly(self):
        self.poll()

    # Randomly move up to distance meters in any direction
    def random_walk(self, distance):
        self.move_to({
            'lat': self.current_location.get('lat') + random.randrange(-distance, distance) / scale1meter,
            'long': self.current_location.get('long') + random.randrange(-distance, distance) / scale1meter
        })

    # Simulate what happens when this client "observes" another, i.e. here's its bluetooth
    def observes(self, other):
        self.listen(other.broadcast())

    def init(self, init_data):
        json_data = copy.deepcopy(init_data)
        json_data['status'] = self.status
        self.init_resp = self.server.init(json_data)
        self.bounding_box_minimum_dp = min(self.bounding_box_maximum_dp,
                                           self.init_resp.get('bounding_box_minimum_dp', self.bounding_box_minimum_dp))
        self.location_resolution = self.init_resp.get('location_resolution', self.location_resolution)
        self.prefix_length = self.init_resp.get('prefix_length', self.prefix_length)


def test_pseudoclient_2client(server, data):
    server.reset()
    logging.info('Started test_pseudoclient_work')
    alice = Client(server=server, data=data, name="Alice")
    alice.init(data.init_req)
    bob = Client(server=server, data=data, name="Bob")
    bob.init(data.init_req)
    bob.random_walk(10)  # Bob has been in two locations now
    alice.observes(bob)
    bob.observes(alice)
    logging.info("==Alice sends in a status of Infected(4) along with Bob's id and a location")
    alice.update_status(STATUS_PUI)
    logging.info("==Bob polls and should see Alice's notice with both ID and location")
    bob.cron_hourly()  # Bob polls and should see alice
    assert len(bob.id_alerts) == 1
    assert len(bob.location_alerts) == 1
    assert bob.status == STATUS_UNKNOWN
    logging.info("==Alice updates her status=1 (Infected)")
    alice.update_status(STATUS_INFECTED)
    logging.info("==Bob polls and should see the updated ID and location from Alice with status=1(Infected); he'll send in his own status as PUI with his own location and ids")
    bob.cron_hourly()  # Bob polls and should get the update from alice
    assert len(bob.id_alerts) == 2
    assert len(bob.location_alerts) == 2
    assert bob.status == STATUS_PUI
    logging.info("==Bob polls and should see his own status=3(PUI) coming back, but ignore it")
    bob.cron_hourly()  # Bob polls and should get the update from alice
    logging.info('Completed test_pseudoclient_work')


def test_pseudoclient_multiclient(server, data):
    number_of_initial_clients = 3
    add_client_each_step = True
    chance_of_walking = 1
    chance_of_infection = 3  # Chance of having an infection status change event (other than through interaction with another test subject)
    chance_of_test_positive = 2  # Chance that a PUI tests positive is 1:2
    chance_of_recovery = 10  # Average 10 steps before recover
    steps = 5   # Work is proportional to square of this if add_client_each_step
    clients = []

    def _add_client():
        c = Client(server=server, data=data, name=str(len(clients)))
        c.init(data.init_req)
        clients.append(c)

    logging.info("Creating %s clients" % number_of_initial_clients)
    for i in range(number_of_initial_clients):
        _add_client()
    for steps in range(steps):
        logging.info("===STEP %s ====", steps)
        if add_client_each_step:
            _add_client()
        for c in clients:
            if not random.randrange(0, chance_of_walking):
                c.random_walk(10)
                logging.info(
                    "%s: walked to %.5fN,%.5fW" % (c.name, c.current_location['lat'], c.current_location['long']))
            for o in clients:
                if o != c:  # Skip self
                    if o.close_to(o.current_location, c.current_location):
                        c.observes(o)
                        logging.info("%s: observed %s" % (c.name, o.name))
            new_status = c.status
            if c.status == STATUS_PUI:
                if not random.randrange(0, chance_of_test_positive):
                    new_status = STATUS_INFECTED
                else:
                    new_status = STATUS_HEALTHY
            else:
                if c.status in [STATUS_HEALTHY, STATUS_UNKNOWN] and not random.randrange(0, chance_of_infection):
                    new_status = STATUS_PUI
                elif c.status in [STATUS_INFECTED] and not random.randrange(0, chance_of_recovery):
                    new_status = STATUS_HEALTHY
            if new_status != c.status:
                logging.info("%s: status change %s -> %s" % (c.name, c.status, new_status))
                c.update_status(new_status)
            c.cron_hourly()
