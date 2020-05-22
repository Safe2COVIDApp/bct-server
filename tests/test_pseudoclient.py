import random
import logging
import copy
import math
from lib import new_seed, get_update_token, get_replacement_token, iso_time_from_seconds_since_epoch, current_time, \
    set_current_time_for_testing, inc_current_time_for_testing, get_next_id_from_proof, \
    random_ascii, get_next_id, get_provider_daily, get_id_proof
from threading import Thread

logger = logging.getLogger(__name__)

scale1meter = 100000  # Approx this many meters in lat or long
STATUS_INFECTED = 1
STATUS_PUI = 2
STATUS_UNKNOWN = 3
STATUS_HEALTHY = 4

StatusEnglish = [ "Test Positive", "Infected", "Under Investigation", "Unknown", "Healthy"]

MAX_DATA_POINTS_PER_TEST = 256 # Should match in confi

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
        self.location_time_significant = 1  # Time in seconds we want to consider significant (1 for testing)
        self.expire_locations_seconds = 45  # Would be 45*24*60*60

        # Access generic test functions and data
        self.server = server
        self.data = data
        self.name = name

        # Initialize arrays where we remember things
        self.daily_ids_used = []  # A list of all daily ids, {id, last_used} used, these never leave the client and are used to calculate EphIds
        self.locations = []  # A list of locations this client has been for an epidemiologically significant time
        self.observed_ids = []  # A list of all EphIds we have seen via Bluetooth
        self.location_alerts = []  # A list of alerts sent to us by the server filtered by locations we have been at
        self.id_alerts = []  # A list of {id, update_token} sent to us by the server filtered by map_ids_used()

        # Setup initial status
        self.init_resp = None  # Setup in init
        self.daily_id = None  # Setup in new_daily_id
        self.current_id = None
        self.new_daily_id()
        self.new_id()
        self.current_location = None
        self.move_to({'lat': 0, 'long': 0})  # In a real client this would be called with GPS results
        # This status changes based on something external to notifications, for example self-reported symptoms or a test result
        self.local_status = STATUS_HEALTHY
        # Status based on local_status but also any alerts from others.
        self.status = STATUS_HEALTHY
        # Seed on any status that might need updating
        self.seed = None
        self.length = 0  # How many records have been reported with this seed
        self.since = None  # The time we last did a /status/scan
        self.pending_test = None
        self.init(self.data.init_req)

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

    # A list of all {id, last_used} used, these never leave the client except via bluetooth beacon
    def map_ids_used(self):
        for daily_id in self.daily_ids_used+[self.daily_id]:
            daily_id_str = daily_id['id']
            for seq in range(daily_id['len']):
                yield get_next_id(daily_id_str, seq)

    def find_proof_and_seq(self, id):
        """
        This finds proof that this client created this id as part of the support for Contact Tracers
        """
        if id is None:
            # Handle case where there is no id in the data point
            return ""
        else:
            for daily_id in self.daily_ids_used+[self.daily_id]:
                daily_id_str = daily_id['id']
                for seq in range(daily_id['len']):
                    if get_next_id(daily_id_str, seq):
                        return get_id_proof(daily_id_str), seq

    def new_daily_id(self, id=None):
        """
        The client's id is set to a new random value, and a record is kept of what we have used.
        """
        if self.daily_id:
            self.daily_id['last_used'] = current_time()
            self.daily_ids_used.append(self.daily_id)
        self.daily_id = {"id": id or "%X" % random.randrange(0, 2 ** 128), "len": 0}

    def new_id(self):
        self.current_id = get_next_id(self.daily_id['id'],self.daily_id['len'])
        self.daily_id['len'] += 1

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
        return [i[:prefix_chars] for i in self.map_ids_used()]

    @staticmethod
    def close_to(other_loc, loc, distance):
        """
        Calculate if the two locations are closer than distance
        This is a very crude "close_to" function because of the rounding in positions, could obviously be much better.
        TODO-150 see open issue about time
        """
        return math.sqrt(((other_loc['lat'] - loc['lat']) ** 2 +
                          (other_loc['long'] - loc['long']) ** 2)) * scale1meter <= distance

    # Received location matches if its close to any of the locations I have been to
    def _location_match(self, loc):
        """
        Check if a location received from a /status/scan is actually close enough to any location
        that we have been at.
        Note that since the server does not receive a time from the infected person
        there is no concept of time in this match.
        """
        return any(Client.close_to(pastloc, loc, self.safe_distance) for pastloc in self.locations)

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
        # Note that this can include a test result which might be STATUS_HEALTHY
        matched_ids = [i for i in json_data['contact_ids'] if i.get('id') in self.map_ids_used()]
        self.id_alerts.extend(matched_ids)
        if self.pending_test and self.pending_test['id'] in [ i['id'] for i in matched_ids]:
            # TODO-114 check this code gets called
            self.pending_test = None    # Clear pending test

        # Filter incoming location updates for those close to where we have been,
        # but exclude any of our own (based on matching update_token
        existing_location_update_tokens = [loc.get('update_token') for loc in self.locations]
        self.location_alerts.extend(
            filter(
                # TODO-150 see open issue about time of match
                lambda loc: self._location_match(loc) and not loc.get('update_token') in existing_location_update_tokens,
                json_data.get('locations', [])))

        # Look for any updated data points
        # Find the replaces tokens for both ids and locations - these are the locations this data point replaces
        # Note that by checking all id_alerts we also handle any received out of order (replace received before original)
        id_replaces = [i.get('replaces') for i in self.id_alerts if i.get('replaces')]
        location_replaces = [loc.get('replaces') for loc in self.location_alerts if loc.get('replaces')]

        # Find update_tokens that have been replaced
        id_update_tokens = [get_update_token(rt) for rt in id_replaces]
        location_update_tokens = [get_update_token(rt) for rt in location_replaces]

        # Mark any ids or locations that have been replaced
        for i in self.id_alerts:
            if i.get('update_token') in id_update_tokens:
                i['replaced'] = True
        for loc in self.location_alerts:
            if loc.get('update_token') in location_update_tokens:
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

    #  === The next section relates to /send/status

    def _next_update_token(self):
        """
        Find a unique update_token to use, based on the seed and length
        """
        ut = get_update_token(get_replacement_token(self.seed, self.length))
        self.length += 1
        return ut

    def _prep_contact(self, c):
        """
        Prepare a contact data point for /send/status,
        adds an update_token which is used to detect if its been sent previously
        """
        c['update_token'] = self._next_update_token()
        return c

    def _prep_location(self, location):
        """
        Prepare a location data point for /send/status,
        adds an update_token which is used to detect if its been sent previously
        a copy is returned with rounded values, but we keep the full resolution result for distance calculations
        """
        location['update_token'] = self._next_update_token()
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

    def _update_to(self, new_status, seed=None):
        """
        Perform a /send/update

        On a real client there are more heuristics as to when a update should be sent, and when it should leave the old status valid
        For example depending on whether it is self-reported or notified
        """
        replaces = self.seed  # Will be None the first time
        length = self.length
        self.seed = seed or new_seed()
        self.length = 0
        json_data = self.server.status_update_json(status=new_status, seed=self.seed, replaces=replaces, length=length)
        logging.info("status/update result: %s" % (str(json_data)))

    def got_tested(self, provider_id="", test_id="", pin=""):
        logging.info("%s: got tested at %s testid=%s pin=%s" % (self.name, provider_id, test_id, pin))
        self.local_status = STATUS_PUI

        provider_daily = get_provider_daily(provider_id, test_id, pin)
        provider_proof = get_id_proof(provider_daily)
        self._recalculate_status(seed=provider_proof)
        self.new_daily_id(provider_daily)  # Saves as ued with len=0
        self.new_id()  # Uses the 0-th and increments
        # Record the NEW current_id as a pending test, when we see a notification (either way) we'll clear this
        self.pending_test = { "id": self.current_id, "provider_id": provider_id, "test_id": test_id, "pin": pin }
        self.new_daily_id() # Don't use the special daily id again
        self.new_id()  # Uses the 0-th and increments

    def local_status_event(self, new_status):
        """
        Set status based on a local observation e.g. notification of test or contact, or self-reported symptoms.
        """
        logging.info("%s: local status change %s -> %s" % (self.name, self.status, new_status))
        self.local_status = new_status
        self._recalculate_status()  # This may trigger a /send/status or /send/update

    def _recalculate_status(self, seed=None):
        """
        Work out any change in status, based on the rest of the state (id_alerts, location_alerts and local_status)

        New status is minimum of any statuses that haven't been replaced +1
        (e.g. if user is Infected (1) we will be PUI (2)

        Can trigger a /status/send or /status/update
        """
        new_status = min([i['status'] + 1 for i in self.id_alerts if not i.get('replaced')]
                         + [loc['status'] + 1 for loc in self.location_alerts if not loc.get('replaced')]
                         + [self.local_status])
        self._notify_status(new_status, seed)  # Correctly handles case of no change and can trigger /status/send or /status/update
        self.status = new_status

    def _notify_status(self, new_status, seed=None):
        """
        Notify status and where required any observed ids and locations to server.
        seed can override using a random number, and is used when working with a test provider

        There are only a few valid transitions
        1: Alice Healthy or Unknown receives notification (from #2 or #4) becomes PUI - status/send causes Bob #5
        2: Alice PUI tests positive (goes to Infected) - status/update pushes Bob from Unknown (because of this client's #1) to PUI (that  #1)
        3: Alice PUI tests negative (goes to Healthy or Unknown) - status/update causes Bob #6
        4: Alice Healthy or Unknown tests directly positive (goes to Infected) - status/send pushes Bob Healthy or Unknown to PUI (see #1)
        Or as a result of a poll.
        5: Bob receives notification (from Alice status/send or status/update #1) changes Healthy to Unknown - doesn't send anything out
        6: Bob receives notification (from Alice status/update #3) changes Unknown to Healthy, nothing sent
        8: Alice was Infected but recovers (Healthy, or another Unknown) - nothing sent
        9: Alice is attempting to update based on a new seed that the Tester also knows
        Infected -> PUI shouldn't happen
        """
        if self.status == STATUS_PUI and new_status != self.status or seed:  # Cases #2 or #3 or #9 above, self.seed should be set
            assert self.seed
            self._update_to(new_status, seed)
        # This covers cases #1 and #4 (H|U -> P|I) plus any newly observed ids or visited locations while P|I
        if new_status in [STATUS_PUI, STATUS_INFECTED] and (any(not c.get('update_token') for c in self.observed_ids) or any(not loc.get('update_token') for loc in self.locations)):
            self._send_to(new_status)

    def _process_one_message(self, dp):
        """
        Substitute in a message from a datapoint which might be an id or location
        """
        proof_and_seq = self.find_proof_and_seq(dp.get('id',None))
        return dp.get('message')\
            .replace('{id}', dp.get('id',""))\
            .replace('{proof}', proof_and_seq[0] if proof_and_seq else "")

    def get_message_data_points(self):
        return [dp for dp in self.id_alerts + self.location_alerts if
         dp.get('message') and not dp.get('replaced')]

    def show_messages(self):
        """
        If there are alerts then show the user the message.
        """
        messages = [ self._process_one_message(dp) for dp in self.get_message_data_points() ]
        if len(messages):
            logging.info("%s: Should be displayed messages: %s", self.name, ';'.join(messages))

    def expire_data(self):
        """
        Expire old location and id data
        """
        expiry_time = current_time()-self.expire_locations_seconds
        while len(self.locations) and self.locations[0].get('end_time') < expiry_time:
            self.locations.pop(0)
        while len(self.daily_ids_used) and self.daily_ids_used[0].get('last_used') < expiry_time:
            self.daily_ids_used.pop(0)

    def cron15(self):
        """
        Action taken every 15 minutes
        """
        self.new_id()  # Rotate id

    def cron_hourly(self):
        """
        Action taken every hour - check for updates
        """
        self.poll()  # Performs a /status/scan
        self.show_messages() # Display any messges (in real client this could be handled various ways
        self.expire_data()

    def cron_daily(self):
        """
        Action taken if the app has been running for more than an hour
        """
        self.new_daily_id()
        self.init(self.data.init_req)

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

    def simulation_step(self, step_parameters, readonly_clients, tester, tracer):
        """
        Perform a single step of a simulation - this may change as new features are tested.

        :param step_parameters: { steps, chance_of_walking, chance_of_test_positive, chance_of_infection, chance_of_recovery, bluetooth_range }
        :param readonly_clients: [ client ] an array of clients - READONLY to this thread, so that its thread safe.
        :return:
        """
        def _chance(s):
            return not random.randrange(0, step_parameters[s])

        for step in range(0, step_parameters['steps']):
            inc_current_time_for_testing()  # At least one clock tick
            # Possibly move the client
            if _chance('chance_of_walking'):
                self.simulate_random_walk(10)
                logging.info(
                    "%s: walked to %.5fN,%.5fW" % (self.name, self.current_location['lat'], self.current_location['long']))

            # In this step we look at a provided read_only array to see of who is close to ourselves,
            for o in readonly_clients:
                if o != self:  # Skip self
                    if Client.close_to(o.current_location, self.current_location, step_parameters['bluetooth_range']):
                        self.simulate_observes(o)
                        logging.info("%s: observed %s" % (self.name, o.name))

            if self.status == STATUS_PUI and not self.pending_test:
                if any(i.get('message') for i in self.id_alerts):
                    if _chance('chance_of_calling_tracer'):
                        proof, seq = self.find_proof_and_seq(self.get_message_data_points()[0]['id'])
                        tracer.provided_proof(proof)
                elif _chance('chance_of_getting_tested'):
                    (provider_id, test_id, pin) = tester.new_test()
                    self.got_tested(provider_id=provider_id, test_id=test_id, pin=pin)
            elif self.pending_test and _chance('chance_of_getting_result'):
                    # Simulate receiving a test result
                    test_id = self.pending_test['test_id']
                    if _chance('chance_of_test_positive'):
                        tester.result(test_id, STATUS_INFECTED)
                        tracer.receive_test(tester.send_test(test_id))
                    else:
                        tester.result(test_id, STATUS_HEALTHY)
            elif not self.pending_test:
                # Simulate finding infected by some method
                if self.status in [STATUS_HEALTHY, STATUS_UNKNOWN] and _chance('chance_of_infection'):
                    self.local_status_event(STATUS_PUI)
                # Simulate recovering by some other method
                elif self.status in [STATUS_INFECTED] and _chance('chance_of_recovery'):
                    self.local_status_event(STATUS_HEALTHY)

            # Simulate an hourly event
            self.cron_hourly()  # Will poll for any data from server


class xTester:

    def __init__(self, server, provider_id):
        self.server = server
        self.provider_id = provider_id
        self.tests = {}

    def new_test(self, pin=None):
        test_id = random_ascii(8)
        if not pin:
            pin = random_ascii(4)
        self.tests[test_id] = {'pin': pin}
        return (self.provider_id, test_id, pin)

    def result(self, test_id, status):
        """
        Tester got a result, which should be STATUS_INFECTED or STATUS_HEALTHY
        """
        test = self.tests[test_id]
        provider_daily =  get_provider_daily(self.provider_id, test_id, test.get('pin'))
        provider_proof = get_id_proof(provider_daily)  # This is the replaces value Alice will have derived UpdateTokens from
        id_for_provider = get_next_id(provider_daily, 0)  # This is the id that Alice will be watching for
        test['status'] = status
        length = 256 # How many points to account for - adjust this with experience
        test['seed'] = new_seed()
        update_tokens = [get_update_token(get_replacement_token(test['seed'], n)) for n in range(length)]
        json_data = self.server.result(
            replaces = provider_proof,
            status = status,
            update_tokens = update_tokens,
            id = id_for_provider,
            message = "Please call 0412-345-6789 to speak to a contact tracer and quote {proof}"
        )

    def send_test(self, test_id):
        test = self.tests[test_id]
        return { "seed": test['seed'], "test_id": test_id}  # Currently, only thing the Tracer needs is the seed, but test_id helps reference

class Tracer:

    def __init__(self, server):
        self.server = server
        self.traces = {}
        self.id_index = {}

    def receive_test(self, new_trace):
        test_id = new_trace.get('test_id')
        self.traces[test_id] = new_trace
        self.get_data_points(test_id)

    def get_data_points(self, test_id):
        trace = self.traces[test_id]
        resp = self.server.status_data_points(seed=trace['seed'])
        trace['contact_ids'] = resp['contact_ids']
        for contact in resp['contact_ids']:
            self.id_index[contact['id']] = {"contact": contact, "trace": trace}
        trace['locations'] = resp['locations']

    def check_provided_proof(self, proof):
        """
        Find any contact data points that relate to an id generated by the person who provided this proof
        """
        ret = []
        for seq in range(24*15): # proof cant have been used more than once every 15 mins for a day
            id = get_next_id_from_proof(proof, seq)
            if id in self.id_index:
                ret.append(self.id_index[id])
        return ret

    def provided_proof(self, proof):
        for res in  self.check_provided_proof(proof):
          logging.info("Test: %s was %s and they were in contact with id %s for %s minutes" %
                     (res["trace"]["test_id"], StatusEnglish[res["contact"]["status"]], res["contact"]["id"], res["contact"]["duration"]))

def test_pseudoclient_test_and_trace(server, data):
    # Standard setup
    server.reset()
    logging.info('Started test_provider_and_tracer')
    alice = Client(server=server, data=data, name="Alice")
    bob = Client(server=server, data=data, name="Bob")
    alice.simulate_observes(bob)
    bob.simulate_observes(alice)
    alice.local_status_event(STATUS_PUI)
    bob.cron_hourly()  # Bob polls and should see alice
    assert bob.status == STATUS_UNKNOWN
    inc_current_time_for_testing()
    logging.info('Alice gets tested')
    terry = xTester(server, 'Kaiser')
    (provider_id, test_id, pin) = terry.new_test()
    alice.got_tested(provider_id=provider_id, test_id=test_id, pin=pin)
    inc_current_time_for_testing()
    bob.cron_hourly()  # Bob polls and should see update from alice
    assert bob.status == STATUS_UNKNOWN
    inc_current_time_for_testing()
    logging.info('Test result comes in')
    terry.result(test_id, STATUS_INFECTED)
    #TODO-114 think thru side-effect of this as Alice's update doesnt have the message
    bob.cron_hourly()
    assert bob.status == STATUS_PUI
    inc_current_time_for_testing()
    alice.cron_hourly()
    assert alice.status == STATUS_INFECTED
    inc_current_time_for_testing()
    bob.cron_hourly()
    assert bob.status == STATUS_PUI
    logging.info('Tracer gets test from Tester and looks up users')
    tracy = Tracer(server)
    tracy.receive_test(terry.send_test(test_id))
    assert len(tracy.traces[test_id]["contact_ids"]) == 2  # Saw Alice and Bob
    logging.info('Bob gives Tracey a call')
    bob_proof,bob_seq = bob.find_proof_and_seq(bob.get_message_data_points()[0]['id'])
    assert tracy.check_provided_proof(bob_proof)[0]['contact']['id'] in bob.map_ids_used()
    tracy.provided_proof(bob_proof)


def test_pseudoclient_2client(server, data):
    """
    This test simulates two clients and walks through specific actions, checking the results match expected
    """
    server.reset()
    logging.info('Started test_pseudoclient_work')
    alice = Client(server=server, data=data, name="Alice")
    bob = Client(server=server, data=data, name="Bob")
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
        'chance_of_getting_tested': 2,
        'chance_of_getting_result': 3,
        'chance_of_calling_tracer': 2,
        'steps': 1,                 # Steps each client does on own before back to this level
    }
    clients = []

    def _add_client():
        cl = Client(server=server, data=data, name=str(len(clients)))
        cl.init(data.init_req)
        clients.append(cl)
    terry = xTester(server, "Kaiser")
    tracy = Tracer(server)
    logging.info("Creating %s clients" % simulation_parameters['number_of_initial_clients'])
    for i in range(simulation_parameters['number_of_initial_clients']):
        _add_client()
    for steps in range(simulation_parameters['steps']):
        logging.info("===STEP %s ====", simulation_parameters['steps'])
        if simulation_parameters['add_client_each_step']:
            _add_client()
        for c in clients:
            c.simulation_step(step_parameters, clients, terry, tracy)

def test_pseudoclient_work(server, data, n_clients=5, n_steps=20):
    #def test_spawn_clients_one_test(server, data, n_clients=5, n_steps=20):
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
        'chance_of_getting_tested': 2,
        'chance_of_getting_result': 3,
        'chance_of_calling_tracer': 1,
        'steps': n_steps,                 # Steps each client does on own before back to this level
    }
    clients = []
    terry = xTester(server, "Kaiser")
    tracy = Tracer(server)
    for i in range(0, n_clients):
        c = Client(server=server, data=data, name="Client-"+str(i))
        clients.append(c)
    threads = []
    for c in clients:
        # This next line is the one we want to multithread
        this_thread = Thread(target=c.simulation_step, args=(step_parameters, clients, terry, tracy))
        threads.append(this_thread)
        this_thread.start()
    for t in threads:
        t.join()
