# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import calendar
import rtree
from collections import defaultdict
from lib import update_token, replacement_token, random_ascii, current_time, unix_time_from_iso, iso_time_from_seconds_since_epoch
from blist import sortedlist


os.umask(0o007)

logger = logging.getLogger(__name__)

# Return a matching date - see issue#57 for discussion of a valid date
# Essentially is date < now to return all items in anything other than the current second
# that is to make sure that if an event arrives in the same second, we know for sure that it was NOT included, no matter if after or before this sync or scan_status
# And is since <= date so that passing back now will get any events that happened on that second
# All times are floating point seconds since the epoch
def _good_date(date, since = None, now = None):
    return ((not since) or (since <= date)) and ((not now) or (date < now))

init_statistics_fields = ['application_name', 'application_version', 'phone_type', 'region', 'health_provider', 'language']
class FSBackedThreeLevelDict:

    @staticmethod
    def dictionary_factory():
        return defaultdict(FSBackedThreeLevelDict.dictionary_factory)

    def __init__(self, directory):
        self.items = FSBackedThreeLevelDict.dictionary_factory()
        self.item_count = 0
        self.update_index = {}
        self.sorted_list_by_time = sortedlist()
        self.time_to_file_path_map = {}
        self.directory = directory
        self._load()
        return

    def _load(self):
        for root, sub_dirs, files in os.walk(self.directory):
            for file_name in files:
                if file_name.endswith('.data'):
                    simple_file_name = file_name.replace('.data', '')
                    (code, floating_seconds) = simple_file_name.split(':')
                    dirs = root.split('/')[-3:]
                    contact_dates = self.items[dirs[0]][dirs[1]][dirs[2]]
                    floating_seconds = float(floating_seconds)
                    self.sorted_list_by_time.add(floating_seconds)
                    relative_file_path = '/'.join([dirs[0], dirs[1], dirs[2], file_name])
                    self.time_to_file_path_map[floating_seconds] = relative_file_path
                    floating_seconds_list = [floating_seconds]
                    if code in contact_dates:
                        floating_seconds_list = contact_dates[code]
                        floating_seconds_list.append(floating_seconds)
                    self.item_count += 1
                    self.items[dirs[0]][dirs[1]][dirs[2]][code] = floating_seconds_list

                    # Note this is expensive, it has to read each file to find update_tokens - maintaining an index would be better.
                    blob = json.load(open('/'.join([root, file_name])))
                    updatetoken = blob.get('update_token')
                    if updatetoken:
                        self.update_index[updatetoken] = file_name
                    self._load_key(code, blob)
        return
        
    def _load_key(self, key, blob):
        """
        _load_key can be subclassed to associate a key with data stored at that key
        
        Parameters:
        ----------
        key -- String
               Index into the FS and dictionary
        blob -- Dict 
                Data associated with Key
        """
        return
        


    def _make_key(self, key):
        """ 
        _make_key is defined in the subclass and returns a unique string (should be over 6 characters, but long enough to avoid collisions)
        that can be used to index into the three level filesystem

        Parameters:
        ----------
        self -- FSBackedThreeLevelDict
        key  -- the object that needs to be turned into the string
        """
        raise NotImplementedError
    
    def _insert_disk(self, key):
        """
        _insert_disk does subclass dependent insertion into both memory and filesystem
        
        """
        raise NotImplementedError

    def _key_string_from_blob(self, blob):
        raise NotImplementedError

    def get_directory_name_and_chunks(self, key):
        chunks = [key[i:i+2] for i in [0, 2, 4]]
        return chunks, "/".join(chunks)

    def insert(self, key, value, floating_seconds):
        """
        Insert value object at key with date, keep various indexes to it (update_index)

        Parameters:
        -----------
        key -- String || Object Either the contact id, or tuple or lat/long
        value -- Dict object needing storing
        floating_seconds -- unix time
        """
        if str != type(key):
            key = self._make_key(key)
        # we are NOT going to read multiple things from the file system for performance reasons
        #if value in self.map_over_json_blobs(key, None, None):
        #    logger.warning('%s already in data for %s' % (value, key))
        #    return
        if 6 > len(key):
            raise Exception("Key %s must by at least 6 characters long" % key)
        key = key.upper()

        chunks, dir_name = self.get_directory_name_and_chunks(key)
        
        # first put this floating_seconds into the item list
        if list != type(self.items[chunks[0]][chunks[1]][chunks[2]][key]):
            self.items[chunks[0]][chunks[1]][chunks[2]][key] = [floating_seconds]
        else:
            self.items[chunks[0]][chunks[1]][chunks[2]][key].append(floating_seconds)

        os.makedirs(self.directory + '/' + dir_name, 0o770, exist_ok = True)
        file_name = '%s:%f.data'  % (key, floating_seconds)
        file_path = '%s/%s' % (dir_name, file_name)
        logger.info('writing %s to %s' % (value, self.directory + '/' + file_path))
        with open(self.directory + '/' + file_path, 'w') as file:
            json.dump(value, file)
        self.sorted_list_by_time.add(floating_seconds)
        self.time_to_file_path_map[floating_seconds] = file_path
        self._insert_disk(key)
        self.item_count += 1
        ut = value.get('update_token')
        if ut:
            self.update_index[ut] = file_name
        return

    def map_over_matching_data(self, key, since, now):
        raise NotImplementedError

    def __len__(self):
        return self.item_count

    def retrieve_json_from_file_paths(self, file_paths):
        for file_path in file_paths:
            yield json.load(open(self.directory + '/' + file_path))
        return

    
    def get_file_paths_between_times(self, since, now):
        """
        get_file_paths_between_times returns a list of file_paths to retrieve and the last time that an entry was made
        """
        logger.info('gfp %s' % self.sorted_list_by_time)
        times_to_retrieve = list(self.sorted_list_by_time[self.sorted_list_by_time.bisect(since):self.sorted_list_by_time.bisect(now - 1)])
        if 0 != len(times_to_retrieve):
            ret = [self.time_to_file_path_map[floating_time] for floating_time in times_to_retrieve], times_to_retrieve[-1]
            return ret
        else:
            return [], 0
    

    def get_file_path_from_file_name(self, file_name):
        (key_string, date_and_extension) = file_name.split(':')
        chunks, dir_name = self.get_directory_name_and_chunks(key_string)
        return "%s/%s" % (dir_name, file_name)

    def update(self, updating_token, updates, now):
        """
        Look for an entry matching updating_token, add a new one after modifying with updates

        :param updating_token: folded hash 16 character string
        :param updates:      { update_token, replaces, status }
        :param now:          unix time
        :return:             True if succeeded
        """
        file_name = self.update_index.get(updating_token)
        if file_name:
            file_path = self.get_file_path_from_file_name(file_name)
            blob = json.load(open(self.directory + '/' + file_path))
            blob.update(updates)
            key_string = self._key_string_from_blob(blob)
            self.insert(key_string, blob, now)
            return True
        else:
            return False

# TODO-DAN it complains class ContactDict must implement all abstract methods
class ContactDict(FSBackedThreeLevelDict):

    def __init__(self, directory):
        directory = directory + '/contact_dict'
        os.makedirs(directory, 0o770, exist_ok = True)
        super().__init__(directory)

    def _insert_disk(self, key):
        logger.info('ignoring _insert_disk for ContactDict')
        return


    def _map_over_matching_contacts(self, prefix, ids, since, now, start_pos = 0):
        logger.info('_map_over_matching_contacts called with %s, %s' % (prefix, ids.keys()))
        if start_pos < 6:
            this_prefix = prefix[start_pos:]
            if len(this_prefix) >= 2:
                ids = ids.get(this_prefix[0:2])
                if ids:
                    yield from self._map_over_matching_contacts(prefix, ids, since, now, start_pos + 2)
            else:
                if 0 == len(this_prefix):
                    prefixes = [('%02x' % i).upper() for i in range(0,256)]
                else:
                    hex_char = this_prefix[0]
                    prefixes = [('%s%01x' % (hex_char, i)).upper() for i in range(0,16)]
                for this_prefix in prefixes:
                    these_ids = ids.get(this_prefix)
                    if these_ids:
                        yield from self._map_over_matching_contacts(prefix, these_ids, since, now, start_pos + 2)
        else:
            for contact_id in filter(lambda x: x.startswith(prefix), ids.keys()):
                for floating_time in ids[contact_id]:
                    if _good_date(floating_time, since, now):
                        yield self.get_file_path_from_file_name('%s:%f.data' % (contact_id, floating_time))
        return


    def map_over_matching_data(self, key, since, now):
        yield from self._map_over_matching_contacts(key, self.items, since, now)
        return


    def _key_string_from_blob(self, blob):
        return blob.get('id')


class SpatialDict(FSBackedThreeLevelDict):
    def __init__(self, directory):
        directory = directory + '/spatial_dict'
        os.makedirs(directory, 0o770, exist_ok = True)
        super().__init__(directory)
        self.spatial_index = rtree.index.Index()
        self.keys = {}          # Maps key_tuple to key_QQ1
        self.coords = {}        # Maps key_QQ1 to key_tuple
        return

    def _key_tuple_from_blob(self, blob):
        return float(blob['lat']), float(blob['long'])

    def _load_key(self, key_string, blob):
        key_tuple = self._key_tuple_from_blob(blob)
        self.keys[key_tuple] = key_string
        self.coords[key_string] = key_tuple
        return
        
    def _make_key(self, key_tuple):
        """
        Return key string from lat,long

        :param key_tuple: (float lat, float long)
        :return:
        """
        key_string = self.keys.get(key_tuple)
        if not key_string:
            key_string = random_ascii(10).upper()
            self.coords[key_string] = key_tuple
        return key_string

    def _insert_disk(self, key_string):
        (lat, long) = coords = self.coords[key_string]

        # only insert if coords not currently in keys
        if coords not in self.keys:
            # we can always use the 0 for the id, duplicates are allowed
            self.spatial_index.insert(0,  (lat, long, lat, long), obj = key_string)
            self.keys[coords] = key_string
        return

    @property
    def bounds(self):
        return self.spatial_index.bounds

    # key is a bounding box tuple (min_lat, min_long, max_lat, max_long) as floats
    def map_over_matching_data(self, key, since, now):
        for obj in self.spatial_index.intersection(key, objects = True):  # [ object: [ obj, ob], object: [ obj, obj]]
            chunks, dir_name = self.get_directory_name_and_chunks(obj.object)
            for floating_time in self.items[chunks[0]][chunks[1]][chunks[2]][obj.object]:
                if _good_date(floating_time, since, now):
                    yield self.get_file_path_from_file_name('%s:%f.data' % (obj.object, floating_time))
        return

    def _key_string_from_blob(self, blob):
        key_tuple = self._key_tuple_from_blob(blob)
        return self._make_key(key_tuple)


# contains both the code for the in memory and on disk version of the database
# The in memory is a four deep hash table where the leaves of the hash are:
#   list of dates (as integers for since compares) of when contact data# has come in.


# for an id "DEADBEEF", the in memory version is stored in self.ids in the element
# self.ids['DE']['AD']['BE']["DEADBEEF']  for the disk version is is store in a four
# level directory structure rooted at config['directory'] in 'DE/AD/BE/DEADBEEF.[DATE].[PSEUDORANDOM].data'
# [DATE] is the date it gets entered in the system and [PSEUDORANDOM] is used to differential contacts with the same ID that come in at the same time
# (accuracy is to minutes).  The date strings are 'YYYYMMDDHHmm'

registry = {}

def register_method(_func = None, *, route):
    def decorator(func):
        registry[route] = func
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


class Contacts:

    def __init__(self, config):
        self.directory_root = config['directory']
        self.testing = ('True' == config.get('testing', ''))
        self.spatial_dict = SpatialDict(self.directory_root)
        self.contact_dict = ContactDict(self.directory_root)
        self.bb_min_dp = config.getint('bounding_box_minimum_dp', 2)
        self.bb_max_size = config.getfloat('bounding_box_maximum_size', 4)
        self.location_resolution = config.getint('location_resolution', 4)
        self.unused_update_tokens = {}
        self.config = config # used in init
        self.statistics = {}
        for k in init_statistics_fields:
            self.statistics[k] = 0
        return


    def execute_route(self, name, *args):
        return registry[name](self, *args)

    def close(self):
        return


    # send_status POST
    # { locations: [ { min_lat, update_token, ...} ], contacts: [ { id, update_token, ... } ], memo, replaces, status, ... ]
    @register_method(route = '/status/send')
    def send_status(self, data, args):
        logger.info('in send_status')
        floating_time = current_time()

        repeated_fields = {}
        # These are fields allowed in the send_status, and just copied from top level into each data point
        # Note memo is not supported yet and is a placeholder
        for key in ['memo', 'replaces', 'status']:
            val = data.get(key)
            if val:
                repeated_fields[key] = val

        # first process contacts, then process geocode
        for contact in data.get('contacts', []):
            contact.update(repeated_fields)
            contact_id = contact['id']
            self.contact_dict.insert(contact_id, contact, floating_time)
        for location in data.get('locations', []):
            location.update(repeated_fields)
            self.spatial_dict.insert((float(location['lat']), float(location['long'])), location, floating_time)
        return {"status": "ok"}

    def _update(self, updatetoken, updates, floating_time):
        return any(this_dict.update(updatetoken, updates, floating_time) for this_dict in [self.contact_dict, self.spatial_dict])

    # status_update POST
    # { locations: [ { min_lat, update_token, ...} ], contacts: [ { id, update_token, ... } ], memo, replaces, status, ... ]
    @register_method(route = '/status/update')
    def status_update(self, data, args):
        logger.info('in status_update')
        now = current_time()
        length = data.get('length') # This is how many to replace
        if length:
            updatetokens = data.get('update_tokens', [])
            for i in range(length):
                rt = replacement_token(data.get('replaces'), i)
                ut = update_token(rt)
                updates = {
                    'replaces': rt,
                    'status': data.get('status'),
                    'update_token': updatetokens[i]
                }  # SEE-OTHER-ADD-FIELDS
                # If some of the update_tokens are not found, it might be a sync issue, hold the update tokens till sync comes in
                if not self._update(ut, updates, now):
                    self.unused_update_tokens[ut] = updates
                    # TODO-80 process unused_update_tokens later
        return {"status": "ok"}

    # scan_status post
    @register_method(route = '/status/scan')
    def scan_status(self, data, args):
        since = data.get('since')
        now = current_time()
        req_locations = data.get('locations', [])
        if not self.check_bounding_box(req_locations):
            return {
                status: 302,
                error: "bounding boxes should be a maximum of %s sq km and specified to a resolution of %s decimal places" % (self.bb_max_size, self.bb_min_dp)
            }
        ret = {}
        if not since:
            since = "1970-01-01T01:01Z"

        ret['since'] = since
        since = unix_time_from_iso(since)
        prefixes = data.get('contact_prefixes')
        if prefixes:
            contact_file_paths = []
            for prefix in prefixes:
                contact_file_paths += self.contact_dict.map_over_matching_data(prefix, since, now)
            logger.info('contact file_paths = %s' % contact_file_paths)
            def get_contact_id_data():
                return list(self.contact_dict.retrieve_json_from_file_paths(contact_file_paths))
            ret['contact_ids'] = get_contact_id_data

        # Find any reported locations, inside the requests bounding box.
        # { locations: [ { min_lat...} ] }
        req_locations = data.get('locations')
        if req_locations:
            spatial_file_paths = []
            for bounding_box in req_locations:
                spatial_file_paths += self.spatial_dict.map_over_matching_data((bounding_box['min_lat'], bounding_box['min_long'], bounding_box['max_lat'], bounding_box['max_long']), since, now)

            logger.info('spatial file_paths = %s' % spatial_file_paths)
            def get_location_id_data():
                return list(self.spatial_dict.retrieve_json_from_file_paths(spatial_file_paths))
            ret['locations'] = get_location_id_data
        ret['until'] = iso_time_from_seconds_since_epoch(now)
        return ret

    # sync get
    @register_method(route = '/sync')
    def sync(self, data, args):
        # Note that any replaced items will be sent as new items, so there is no need for a separate list of nonces.
        now = current_time()  # Do this at the start of the process, we want to guarantee have all before this time (even if multi-threading)
        since_string = args.get('since')
        if since_string:
            since_string = since_string[0].decode()
        else:
            since_string = "1970-01-01T01:01Z"

        since = unix_time_from_iso(since_string)
        contacts, latest_contact_time = self.contact_dict.get_file_paths_between_times(since, now)
        locations, latest_location_time = self.spatial_dict.get_file_paths_between_times(since, now)
        latest_time = max(latest_contact_time, latest_location_time)

        ret = {'until':iso_time_from_seconds_since_epoch(latest_time),
               'since':since_string}

        if 0 != len(contacts):
            def get_contact_id_data():
                return list(self.contact_dict.retrieve_json_from_file_paths(contacts))
            ret['contacts'] = get_contact_id_data
        if 0 != len(locations):
            def get_location_id_data():
                return list(self.spatial_dict.retrieve_json_from_file_paths(locations))
            ret['contacts'] = get_contact_id_data
            ret['locations'] = get_location_id_data
        return ret

    # admin_config get
    @register_method(route = '/admin/config')
    def admin_config(self, data, args):
        ret = {
            'directory': self.directory_root,
            'testing': self.testing
        }
        return ret

    # admin_status get
    @register_method(route = '/admin/status')
    def admin_status(self, data, args):
        ret = {
            'bounding_box' : self.spatial_dict.bounds,
            'geo_points' : len(self.spatial_dict),
            'contacts_count': len(self.contact_dict)
        }
        return ret

    # init post
    @register_method(route = '/init')
    def init(self, data, args):
        for k in init_statistics_fields:
            self.statistics[k] += 1
        #TODO-83
        app_name = data.get('application_name')
        app_current_version = self.config.getfloat('APP_' + app_name)
        ret = {
            # "messaging_url": "", "messaging_version": 1, # TODO-84 - delayed till clients capable
            "bounding_box_minimum_dp": self.bb_min_dp,
            "bounding_box_maximum_size": self.bb_max_size,
            "location_resolution": self.location_resolution, # ~10 meters at the equator
            "prefix_bits": 20, # TODO-34 will need to calculate this
        }
        if (app_current_version):
            ret["application_current_version"] = app_current_version # TODO-83
        return ret

    # reset should only be called and allowed if testing
    def reset(self):
        if self.testing:
            logger.info('resetting ids')
            self.spatial_dict = SpatialDict(self.directory_root)
            self.contact_dict = ContactDict(self.directory_root)
        return

    def check_bounding_box(self, bb_arr):
        for bb in bb_arr:
            for k in ['max_long', 'min_long', 'max_lat', 'min_lat']:
                v = bb.get(k)
                if (round(v, self.bb_min_dp) != v):
                    return False
            if (abs(bb.get('max_long')-bb.get('min_long')) * abs(bb.get('max_lat')-bb.get('min_lat'))) > self.bb_max_size:
                return False
        return True
