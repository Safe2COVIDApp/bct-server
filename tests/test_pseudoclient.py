import random
import logging
import time
import copy
import math
from lib import new_seed, update_token, replacement_token, iso_time_from_seconds_since_epoch, current_time, set_current_time_for_testing, inc_current_time_for_testing
from threading import Thread

logger = logging.getLogger(__name__)

scale1meter = 100000  # Approx this many meters in lat or long
STATUS_INFECTED = 1
STATUS_PUI = 2
STATUS_UNKNOWN = 3
STATUS_HEALTHY = 4

######
# This file is intended to give a roadmap for functionality needed in the real (app) client.

set_current_time_for_testing(100000)

class Client:

    def __init__(self, server=None, data=None, name="Unnamed", **kwargs):
        """
        Initialize the Client

        Notes:
             some variables will be overwritten with values returned in the init call.
             1 meter at the equator (and we start at 0,0) is approx 0.00001 degree of long
        """
        self.prefix_bits = 64  # How many bits to use in prefix - see issue#34 for changing this prefix length automatically
        self.id_length = 32  # How many characters in hex id. Normally would be 128 bits = 32 chars
        # Distance apart in meters of a GPS report to be considered valid
        # note because rounding to 10meters in location_resolution this needs to be 20 to catch within 10 meters distance
        self.safe_distance = 20
        self.location_resolution = 4  # lat/long decimal points - 4 is 10 meters
        self.bounding_box_minimum_dp = 2  # Updated after init - 2 is 1km
        self.bounding_box_maximum_dp = 3  # Do not let the server require resolution requests > ~100
        self.location_time_significant = 1 # Time in seconds we want to consider significant (1 for testing)
        self.expire_locations_seconds = 45 # Would be 45*24*60*60

        # Access generic test functions and data
        self.server = server
        self.data = data
        self.name = name

        # Initialize arrays where we remember things
        self.ids_used = [] # A list of all {id, last_used} used, these never leave the client except via bluetooth beacon
        self.locations = [] # A list of locations this client has been for an epidemiologically significant time
        self.observed_ids = [] # A list of all ids we have seen
        self.location_alerts = [] # A list of alerts sent to us by the server filtered by locations we have been at
        self.id_alerts = [] # A list of {id, update_token} sent to us by the server filtered by ids_used

        # Setup initial status
        self.new_id()
        self.current_location = None
        self.move_to({'lat': 0, 'long': 0}) # In a real client this would be called with GPS results
        # This status changes based on something external to notifications, for example self-reported symptoms or a test result
        self.local_status = STATUS_HEALTHY
        # Status based on local_status but also any alerts from others.
        self.status = STATUS_HEALTHY
        # Seed on any status that might need updating
        self.seed = None
        self.length = 0 # How many records have been reported with this seed
        self.since = None # The time we last did a /status/scan

    def init(self, init_data):
        """
        Perform a /init call to the client, which should be done on startup of the app, or once a day.
        Initial data is sent to the server for statistical purposes, and parameters are read back.
        """
        json_data = copy.deepcopy(init_data)
        json_data['status'] = self.status
        self.init_resp = self.server.init(json_data)
        # Take care the bounding_box_minimum_dp is within allowed bounds, stops a possible malicious server attack
        self.bounding_box_minimum_dp = min(self.bounding_box_maximum_dp,
                                           self.init_resp.get('bounding_box_minimum_dp', self.bounding_box_minimum_dp))
        self.location_resolution = self.init_resp.get('location_resolution', self.location_resolution)
        self.prefix_bits = self.init_resp.get('prefix_bits', self.prefix_bits)

    def new_id(self):
        """
        The client's id is set to a new random value, and a record is kept of what we have used.
        """
        self.current_id = "%X" % random.randrange(0, 2 ** 128)
        self.ids_used.append({"id": self.current_id, "last_used": current_time()})

    def move_to(self, loc):
        """
        Manage a new location -
        A real client needs to expire from locations.append if older than 45 days
        """
        old_location = self.current_location
        if old_location:
            old_location["end_time"] = current_time()
            if (old_location["end_time"] - old_location["start_time"]) >= self.location_time_significant:
                self.locations.append(old_location)
        loc['start_time'] = current_time()
        self.current_location = loc

    def _min(self, a):
        return math.floor(min(a) * 10 ** self.bounding_box_minimum_dp) * 10 ** (-self.bounding_box_minimum_dp)

    def _max(self, a):
        return math.ceil(max(a) * 10 ** self.bounding_box_minimum_dp) * 10 ** (-self.bounding_box_minimum_dp)

    def _box(self):
        """
        Calculate a bounding box around the locations that have been used, rounded to bounding_box_minimum_dp
        """
        return {
            'min_lat': self._min(loc['lat'] for loc in self.locations),
            'min_long': self._min(loc['long'] for loc in self.locations),
            'max_lat': self._max(loc['lat'] for loc in self.locations),
            'max_long': self._max(loc['long'] for loc in self.locations),
        }

    def _prefixes(self):
        """
        Return a list of prefixes that can be used for /status/scan
        """
        prefix_chars = int(self.prefix_bits/8)
        return [i['id'][:prefix_chars] for i in self.ids_used]

    def close_to(self, otherloc, loc, distance):
        """
        Calculate if the two locations are closer than distance
        This is a very crude "close_to" function because of the rounding in positions, could obviously be much better.
        """
        return math.sqrt( ((otherloc['lat'] - loc['lat']) ** 2 +
                           (otherloc['long'] - loc['long']) ** 2 )) * scale1meter <= distance

    # Received location matches if its close to any of the locations I have been to
    def _location_match(self, loc):
        """
        Check if a location received from a /status/scan is actually close enough to any location
        that we have been at.
        Note that since the server does not receive a time from the infected person
        there is no concept of time in this match.
        """
        return any(self.close_to(pastloc, loc, self.safe_distance) for pastloc in self.locations)

    def poll(self):
        """
        Perform a regular poll of the server with /status/scan, and process any results.
        """
        json_data = self.server.scan_status_json(contact_prefixes=self._prefixes(),
                                                 locations=[self._box()] if len(self.locations) else [],
                                                 since=self.since)
        logging.info("%s: poll result: %s" % (self.name, str(json_data)))

        # Record when data is updated till, for our next request
        self.since = json_data.get('until')

        # Record any ids in the poll that match one we have used (id = {id, last_used})
        ids_to_match = [i['id'] for i in self.ids_used]
        self.id_alerts.extend([i for i in json_data['contact_ids'] if (i.get('id') in ids_to_match)])

        # Filter incoming location updates for those close to where we have been,
        # but exclude any of our own (based on matching update_token
        existing_location_updatetokens = [loc.get('update_token') for loc in self.locations]
        self.location_alerts.extend(
            filter(
                lambda loc: self._location_match(loc) and not loc.get('update_token') in existing_location_updatetokens,
                json_data.get('locations', [])))

        # Look for any updated data points
        # Find the replaces tokens for both ids and locations - these are the locations this data point replaces
        # Note that by checking all id_alerts we also handle any received out of order (replace received before original)
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

        # Recalculate our own status based on the current set of location and id alerts and our local_status
        # Note that if it has changed it may trigger a notify_status which can cause a /status/send or /status/update
        self._recalculate_status()

    # Simulate broadcasting an id - on a real client this would be every 15 minutes by bluetooth
    def broadcast(self):
        return self.current_id

    # Simulate hearing an id
    def listen(self, contact_id):
        self.observed_ids.append({'id': contact_id, 'duration': 15})


    ###  The next section relates to /send/status

    def _next_updatetoken(self):
        """
        Find a unique update_token to use, based on the seed and length
        """
        ut = update_token(replacement_token(self.seed, self.length))
        self.length += 1
        return ut

    def _prep_contact(self, c):
        """
        Prepare a contact data point for /send/status,
        adds an updatetoken which is used to detect if its been sent previously
        """
        c['update_token'] = self._next_updatetoken()
        return c

    def _prep_location(self, location):
        """
        Prepare a location data point for /send/status,
        adds an updatetoken which is used to detect if its been sent previously
        a copy is returned with rounded values, but we keep the full resolution result for distance calculations
        """
        location['update_token'] = self._next_updatetoken()
        loc = copy.deepcopy(location)
        for k in ['lat', 'long']:
            loc[k] = round(location[k], self.location_resolution)
        for k in ['start_time', 'end_time']:
            loc[k] = iso_time_from_seconds_since_epoch(location[k])
        return loc

    def _send_to(self, new_status):
        """
        Perform a /send/status, send any unsent ids and locations
        Store the update_tokens on these ids, it will allow us to deduplicate echos
        """
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
        """
        Perform a /send/update

        On a real client there are more heuristics as to when a update should be sent, and when it should leave the old status valid
        For example depending on whether it is self-reported or notified
        """
        replaces = self.seed  # Will be None the first time
        length = self.length
        self.seed = new_seed()
        self.length = 0
        json_data = self.server.status_update_json(status=new_status, seed=self.seed, replaces=replaces, length=length)
        logging.info("status/update result: %s" % (str(json_data)))

    def local_status_event(self, new_status):
        """
        Set status based on a local observation e.g. notification of test or contact, or self-reported symptoms.
        """
        logging.info("%s: local status change %s -> %s" % (self.name, self.status, new_status))
        self.local_status = new_status
        self._recalculate_status() # This may trigger a /send/status or /send/update

    def _recalculate_status(self):
        """
        Work out any change in status, based on the rest of the state (id_alerts, location_alerts and local_status)

        New status is minimum of any statuses that haven't been replaced +1
        (e.g. if user is Infected (1) we will be PUI (2)

        Can trigger a /status/send or /status/update
        """
        new_status = min([i['status'] + 1 for i in self.id_alerts if not i.get('replaced')]
                         + [loc['status'] + 1 for loc in self.location_alerts if not loc.get('replaced')]
                         + [self.local_status])
        if (new_status != self.status):
            self._notify_status(new_status)  # Correctly handles case of no change and can trigger /status/send or /status/update
            self.status = new_status

    def _notify_status(self, new_status):
        """
        Notify status and where required any observered ids and locations to server.

        There are only a few valid transitions
        1: Alice Healthy or Unknown receives notification (from #2 or #4) becomes PUI - status/send causes Bob #5
        2: Alice PUI tests positive (goes to Infected) - status/update pushes Bob from Unknown (because of this client's #1) to PUI (that  #1)
        3: Alice PUI tests negative (goes to Healthy or Unknown) - status/update causes Bob #6
        4: Alice Healthy or Unknown tests directly positive (goes to Infected) - status/send pushes Bob Healthy or Unknown to PUI (see #1)
        Or as a result of a poll.
        5: Bob receives notification (from Alice status/send or status/update #1) changes Healthy to Unknown - doesn't send anything out
        6: Bob receives notification (from Alice status/update #3) changes Unknown to Healthy, nothing sent
        8: Alice was Infected but recovers (Healthy, or another Unknown) - nothing sent
        Infected -> PUI shouldn't happen
        """
        if self.status == STATUS_PUI and new_status != self.status:  # Cases #2 or #3 above, self.seed should be set
            assert self.seed
            self._update_to(new_status)
        # This covers cases #1 and #4 (H|U -> P|I) plus any newly observed ids or visited locations while P|I
        if new_status in [STATUS_PUI, STATUS_INFECTED] and (any(not c.get('update_token') for c in self.observed_ids) or any(not loc.get('update_token') for loc in self.locations)):
            self._send_to(new_status)

    def expire_data(self):
        """
        Expire old location and id data
        """
        expiry_time = current_time()-self.expire_locations_seconds
        if len(self.locations):
            while self.locations[0].get('end_time') < expiry_time:
                self.locations.pop(0)
        if len(self.ids_used):
            while self.ids_used[0].get('last_used') < expiry_time:
                self.ids_used.pop(0)

    def cron15(self):
        """
        Action taken every 15 seconds
        """
        self.new_id()  # Rotate id

    def cron_hourly(self):
        """
        Action taken every hour - check for updates
        """
        self.poll() # Performs a /status/scan
        self.expire_data()

    def simulate_random_walk(self, distance):
        """
        For the simulation this randomly moves up to 'distance' meters in any direction
        """
        self.move_to({
            'lat': self.current_location.get('lat') + random.randrange(-distance, distance) / scale1meter,
            'long': self.current_location.get('long') + random.randrange(-distance, distance) / scale1meter
        })

    def simulate_observes(self, other):
        """
        Simulate what happens when this client "observes" another, i.e. here's its bluetooth
        """
        self.listen(other.broadcast())

    def simulation_step(self, step_parameters, readonly_clients):
        """
        Perform a single step of a simulation - this may change as new features are tested.

        :param simulation_parameters: { steps, chance_of_walking, chance_of_test_positive, chance_of_infection, chance_of_recovery, bluetooth_range }
        :param readonly_clients: [ client ] an array of clients - READONLY to this thread, so that its thread safe.
        :return:
        """
        for step in range(0,step_parameters['steps']):
            inc_current_time_for_testing() # At least one clock tick
            # Possibly move the client
            if not random.randrange(0, step_parameters['chance_of_walking']):
                self.simulate_random_walk(10)
                logging.info(
                    "%s: walked to %.5fN,%.5fW" % (self.name, self.current_location['lat'], self.current_location['long']))

            # In this step we look at a provided read_only array to see of who is close to ourselves,
            for o in readonly_clients:
                if o != self:  # Skip self
                    if o.close_to(o.current_location, self.current_location, step_parameters['bluetooth_range']):
                        self.simulate_observes(o)
                        logging.info("%s: observed %s" % (self.name, o.name))

            if self.status == STATUS_PUI:
                # Simulate receiving a test result
                if not random.randrange(0, step_parameters['chance_of_test_positive']):
                    self.local_status_event(STATUS_INFECTED)
                else:
                    self.local_status_event(STATUS_HEALTHY)
            else:
                # Simulate finding infected by some method
                if self.status in [STATUS_HEALTHY, STATUS_UNKNOWN] and not random.randrange(0, step_parameters['chance_of_infection']):
                    self.local_status_event(STATUS_PUI)
                # Simulate recovering by some other method
                elif self.status in [STATUS_INFECTED] and not random.randrange(0, step_parameters['chance_of_recovery']):
                    self.local_status_event(STATUS_HEALTHY)

            # Simulate an hourly event
            self.cron_hourly()  # Will poll for any data from server


def test_pseudoclient_2client(server, data):
    """
    This test simulates two clients and walks through specific actions, checking the results match expected
    """
    server.reset()
    logging.info('Started test_pseudoclient_work')
    alice = Client(server=server, data=data, name="Alice")
    alice.init(data.init_req)
    bob = Client(server=server, data=data, name="Bob")
    bob.init(data.init_req)
    inc_current_time_for_testing()
    bob.simulate_random_walk(10)  # Bob has been in two locations now
    inc_current_time_for_testing()
    alice.simulate_observes(bob)
    inc_current_time_for_testing()
    bob.simulate_observes(alice)
    inc_current_time_for_testing()
    alice.simulate_random_walk(10)  # Alice needs to move to update her locations
    logging.info("==Alice sends in a status of Infected(4) along with Bob's id and a location")
    inc_current_time_for_testing()
    alice.local_status_event(STATUS_PUI)
    logging.info("==Bob polls and should see Alice's notice with both ID and location")
    inc_current_time_for_testing()
    bob.cron_hourly()  # Bob polls and should see alice
    assert len(bob.id_alerts) == 1
    assert len(bob.location_alerts) == 1
    assert bob.status == STATUS_UNKNOWN
    logging.info("==Alice updates her status=1 (Infected)")
    inc_current_time_for_testing()
    alice.local_status_event(STATUS_INFECTED)
    logging.info("==Bob polls and should see the updated ID and location from Alice with status=1(Infected); he'll send in his own status as PUI with his own location and ids")
    inc_current_time_for_testing()
    bob.cron_hourly()  # Bob polls and should get the update from alice
    assert len(bob.id_alerts) == 2
    assert len(bob.location_alerts) == 2
    assert bob.status == STATUS_PUI
    logging.info("==Bob polls and should see his own status=3(PUI) coming back, but ignore it")
    inc_current_time_for_testing()
    bob.cron_hourly()  # Bob polls and should get the update from alice
    logging.info('Completed test_pseudoclient_work')

def test_pseudoclient_multiclient(server, data):

    """
    This test simulates a growing group of clients, at each step, the number of clients is increased and a simulation_step is taken for each
    results aren't checked,
    instead its an opportunity to check random interactions make sense by observing results.
    In most cases this will be run with larger `number_of_initial_clients` and `steps`
    The url in _init.py set can be set to point at a separate server by uncommenting one line
    """
    simulation_parameters = {
        'number_of_initial_clients': 3,
        'add_client_each_step': True,
        'steps': 5   # Work is proportional to square of this if add_client_each_step
    }
    # Setup some parameters for each simulation step, especially chances of certain events happening.
    step_parameters = {
        'chance_of_walking': 1,
        'chance_of_infection': 3,  # Chance of having an infection status change event (other than through interaction with another test subject)
        'chance_of_test_positive': 2,  # Chance that a PUI tests positive is 1:2
        'chance_of_recovery': 10,  # Average 10 steps before recover
        'bluetooth_range': 2,
        'steps': 1,                 # Steps each client does on own before back to this level
    }
    clients = []

    def _add_client():
        c = Client(server=server, data=data, name=str(len(clients)))
        c.init(data.init_req)
        clients.append(c)

    logging.info("Creating %s clients" % simulation_parameters['number_of_initial_clients'])
    for i in range(simulation_parameters['number_of_initial_clients']):
        _add_client()
    for steps in range(simulation_parameters['steps']):
        logging.info("===STEP %s ====", simulation_parameters['steps'])
        if simulation_parameters['add_client_each_step']:
            _add_client()
        for c in clients:
            c.simulation_step(step_parameters, clients)

def test_spawn_clients_one_test(server, data, n_clients=5, n_steps=5):

    """
    This test simulates a large group of clients in separate threads.
    results aren't checked,
    instead its an opportunity to check random interactions make sense by observing results.
    In most cases this will be run with larger `number_of_initial_clients` and `steps`
    The url in _init.py set can be set to point at a separate server by uncommenting one line
    """
    step_parameters = {
        'chance_of_walking': 1,
        'chance_of_infection': 3,  # Chance of having an infection status change event (other than through interaction with another test subject)
        'chance_of_test_positive': 2,  # Chance that a PUI tests positive is 1:2
        'chance_of_recovery': 10,  # Average 10 steps before recover
        'bluetooth_range': 2,
        'steps': n_steps,                 # Steps each client does on own before back to this level
    }
    clients = []
    for i in range(0, n_clients):
        c = Client(server=server, data=data, name="Client-"+str(i))
        c.init(data.init_req)
        clients.append(c)
    threads = []
    for c in clients:
        # This next line is the one we want to multithread
        this_thread = Thread(target=c.simulation_step, args = (step_parameters, clients,))
        threads.append(this_thread)
        this_thread.start()
    for t in threads:
        t.join()
