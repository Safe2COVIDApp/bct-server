# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import calendar
import datetime
import rtree
from collections import defaultdict
from lib import update_token, replacement_token, random_ascii

def unix_time(dt):
    return int(dt.timestamp())

os.umask(0o007)



logger = logging.getLogger(__name__)

# Return a matching date - see issue#57 for discussion of a valid date
# Essentially is date < now to return all items in anything other than the current second
# that is to make sure that if an event arrives in the same second, we know for sure that it was NOT included, no matter if after or before this sync or scan_status
# And is since <= date so that passing back now will get any events that happened on that second
def _good_date(date, since = None, now = None):
    return ((not since) or (since <= date)) and ((not now) or (date < calendar.timegm(now)))


class FSBackedThreeLevelDict:

    @staticmethod
    def dictionary_factory():
        return defaultdict(FSBackedThreeLevelDict.dictionary_factory)

    def __init__(self, directory):
        self.items = FSBackedThreeLevelDict.dictionary_factory()
        self.item_count = 0
        self.update_index = {}
        self.file_map = {}
        self.directory = directory
        self._load()
        return

    def _load(self):
        for root, sub_dirs, files in os.walk(self.directory):
            for file_name in files:
                if file_name.endswith('.data'):
                    (code, ignore, date, extension) = file_name.split('.')
                    dirs = root.split('/')[-3:]
                    contact_dates = self.items[dirs[0]][dirs[1]][dirs[2]]
                    date = int(date)
                    dates = [date]
                    if code in contact_dates:
                        dates = contact_dates[code]
                        dates.append(date)
                    self.item_count += 1
                    self.items[dirs[0]][dirs[1]][dirs[2]][code] = dates

                    # Note this is expensive, it has to read each file to find updatetokens - maintaining an index would be better.
                    blob = json.load(open(('%s/%s/%s/%s/%s' % (self.directory, dirs[0], dirs[1], dirs[2], file_name))))
                    updatetoken = blob.get('updatetoken')
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
        dir_name = '%s/%s/%s/%s' % (self.directory, chunks[0], chunks[1], chunks[2])
        return chunks, dir_name

    def insert(self, key, value, date):
        """
        Insert value object at key with date, keep various indexes to it (update_index)

        Parameters:
        -----------
        key -- String || Object Either the contact id, or tuple or lat/long
        value -- Dict object needing storing
        date -- date (unix time)
        """
        if str != type(key):
            key = self._make_key(key)
        if value in self.map_over_json_blobs(key, None, None):
            logger.warning('%s already in data for %s' % (value, key))
            return
        if 6 > len(key):
            raise Exception("Key %s must by at least 6 characters long" % key)
        key = key.upper()

        chunks, dir_name = self.get_directory_name_and_chunks(key)
        
        # first put this date into the item list
        if list != type(self.items[chunks[0]][chunks[1]][chunks[2]][key]):
            self.items[chunks[0]][chunks[1]][chunks[2]][key] = [date]
        else:
            self.items[chunks[0]][chunks[1]][chunks[2]][key].append(date)

        os.makedirs(dir_name, 0o770, exist_ok = True)
        file_name = '%s.%s.%d.data'  % (key, random_ascii(6), date)
        file_path = '%s/%s' % (dir_name, file_name)
        logger.info('writing %s to %s' % (value, file_path))
        with open(file_path, 'w') as file:
            json.dump(value, file)
        
        self._insert_disk(key)
        self.item_count += 1
        ut = value.get('updatetoken')
        if ut:
            self.update_index[ut] = file_name
        return

    def map_over_matching_data(self, key):
        raise NotImplementedError

    def __len__(self):
        return self.item_count

    def map_over_json_blobs(self, key_string, since, now):
        chunks, dir_name = self.get_directory_name_and_chunks(key_string)
        logger.info('looking for %s in %s/%s' % (key_string, chunks, dir_name))
        if os.path.isdir(dir_name):
            for file_name in os.listdir(dir_name):
                if file_name.endswith('data'):
                    (code, ignore, date, extension) = file_name.split('.')
                    if (code == key_string) and _good_date(int(date), since, now):
                        logger.info('matched, returnding %s/%s' % (dir_name, file_name))
                        yield json.load(open(('%s/%s' % (dir_name, file_name))))
                    else:
                        logger.info('did not matched, returnding %s/%s' % (dir_name, file_name))


        return

    def map_over_all_data(self, since = None, now = None):
        for key1, value1 in self.items.items():
            for key2, value2 in self.items[key1].items():
                for key3, value3 in self.items[key1][key2].items():
                    for key in self.items[key1][key2][key3].keys():
                        yield from self.map_over_json_blobs(key, since, now)

    def get_file_path_from_file_name(self, file_name):
        (key_string, ignore, date, extension) = file_name.split('.')
        chunks, dir_name = self.get_directory_name_and_chunks(key_string)
        return "%s/%s" % (dir_name, file_name)

    #TODO-55 on dict
    def update(self, updating_token, updates, now):
        """
        Look for an entry matching updating_token, add a new one after modifying with updates

        :param updating_token: folded hash 16 character string
        :param updates:      { updatetoken, replaces, status }
        :param now:          unix time
        :return:             True if succeeded
        """
        file_name = self.update_index.get(updating_token)
        if file_name:
            file_path = self.get_file_path_from_file_name(file_name)
            blob = json.load(open(file_path))
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


    def _map_over_matching_contacts(self, prefix, ids, start_pos = 0):
        logger.info('_map_over_matching_contacts called with %s, %s' % (prefix, ids.keys()))
        if start_pos < 6:
            this_prefix = prefix[start_pos:]
            if len(this_prefix) >= 2:
                ids = ids.get(this_prefix[0:2])
                if ids:
                    yield from self._map_over_matching_contacts(prefix, ids, start_pos + 2)
            else:
                if 0 == len(this_prefix):
                    prefixes = [('%02x' % i).upper() for i in range(0,256)]
                else:
                    hex_char = this_prefix[0]
                    prefixes = [('%s%01x' % (hex_char, i)).upper() for i in range(0,16)]
                for this_prefix in prefixes:
                    these_ids = ids.get(this_prefix)
                    if these_ids:
                        yield from self._map_over_matching_contacts(prefix, these_ids, start_pos + 2)
        else:
            for contact_id in filter(lambda x: x.startswith(prefix), ids.keys()):
                yield contact_id
        return


    def map_over_matching_data(self, key):
        yield from self._map_over_matching_contacts(key, self.items)
        return


    def _key_string_from_blob(self, blob):
        return blob.get('id')


class SpatialDict(FSBackedThreeLevelDict):
    def __init__(self, directory):
        directory = directory + '/spatial_dict'
        os.makedirs(directory, 0o770, exist_ok = True)
        super().__init__(directory)
        self.spatial_index = rtree.index.Index(directory + '/rtree')
        self.keys = {}          # Maps key_tuple to key_QQ1
        self.coords = {}        # Maps key_QQ1 to key_tuple
        return

    def _key_tuple_from_blob(self, blob):
        return float(blob['lat']), float(blob['long'])

    # TODO-DAN this looks wrong - it refers to original_data not to blob ?
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
            self.keys[key_tuple] = key_string
            self.coords[key_string] = key_tuple
        return key_string

    def _insert_disk(self, key_string):
        (lat, long) = self.coords[key_string]
        # we can always use the 0 for the id, duplicates are allowed
        self.spatial_index.insert(0,  (lat, long, lat, long), obj = key_string)
        return

    @property
    def bounds(self):
        return self.spatial_index.bounds

    # key is a bounding box tuple (minLat, minLong, maxLat, maxLong) as floats
    def map_over_matching_data(self, key):
        for obj in self.spatial_index.intersection(key, objects = True):  # [ object: [ obj, ob], object: [ obj, obj]]
            yield obj.object
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
        self.unused_update_tokens = {}
        return


    def execute_route(self, name, *args):
        return registry[name](self, *args)

    def close(self):
        return


    # send_status POST
    # { locations: [ { minLat, updatetoken, ...} ], contacts: [ { id, updatetoken, ... } ], memo, replaces, status, ... ]
    @register_method(route = '/status/send')
    def send_status(self, data, args):
        logger.info('in send_status')
        now = int(time.time())

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
            self.contact_dict.insert(contact_id, contact, now)
        for location in data.get('locations', []):
            location['date'] = now
            location.update(repeated_fields)
            self.spatial_dict.insert((float(location['lat']), float(location['long'])), location, now)
        return {"status": "ok"}

    def _update(self, updatetoken, updates, now):
        # TODO-55 test if can do this without the "["
        return any(this_dict.update(updatetoken, updates, now) for this_dict in [self.contact_dict, self.spatial_dict])

    # status_update POST
    # { locations: [ { minLat, updatetoken, ...} ], contacts: [ { id, updatetoken, ... } ], memo, replaces, status, ... ]
    @register_method(route = '/status/update')
    def status_update(self, data, args):
        logger.info('in status_update')
        now = int(time.time())
        length = data.get('length') # This is how many to replace
        if length:
            updatetokens = data.get('updatetokens', [])
            for i in range(length):
                rt = replacement_token(data.get('replaces'), i)
                ut = update_token(rt)
                updates = {
                    'replaces': rt,
                    'status': data.get('status'),
                    'updatetoken': updatetokens[i]
                }  # SEE-OTHER-ADD-FIELDS
                # If some of the updatetokens are not found, it might be a sync issue, hold the update tokens till sync comes in
                if not self._update(ut, updates, now):
                    self.unused_update_tokens[ut] = updates
                    # TODO-55 process unused_update_tokens later
        return {"status": "ok"}

    # scan_status post
    @register_method(route = '/status/scan')
    def scan_status(self, data, args):
        since = data.get('since')
        now = time.gmtime()
        ret = {}
        if since:
            ret['since'] = since
            since = int(unix_time(datetime.datetime.fromisoformat(since.replace("Z", "+00:00"))))
        else:
            ret['since'] = "1970-01-01T01:01Z"

        prefixes = data.get('contact_prefixes')
        if prefixes:
            ids = []
            for prefix in prefixes:
                ids += self.contact_dict.map_over_matching_data(prefix)
            ret['ids'] = []
            for contact_id in ids:
                ret['ids'] += self.contact_dict.map_over_json_blobs(contact_id, since, now)

        # Find any reported locations, inside the requests bounding box.
        # { locations: [ { minLat...} ] }
        req_locations = data.get('locations')
        if req_locations:
            locations = []
            ret['locations'] = []
            for bounding_box in req_locations:
                locations += self.spatial_dict.map_over_matching_data((bounding_box['minLat'], bounding_box['minLong'], bounding_box['maxLat'], bounding_box['maxLong']))
            logger.info('locations are: %s' % locations)
            for location_id in locations:
                ret['locations'] += self.spatial_dict.map_over_json_blobs(location_id, since, now)
        ret['now'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', now)
        return ret

    # sync get
    @register_method(route = '/sync')
    def sync(self, data, args):
        # Note that any replaced items will be sent as new items, so there is no need for a separate list of nonces.
        now = time.gmtime()  # Do this at the start of the process, we want to guarantee have all before this time (even if multi-threading)
        since_string = args.get('since')
        if since_string:
            since_string = since_string[0].decode()
        else:
            since_string = "1970-01-01T01:01Z"

        since = int(unix_time(datetime.datetime.fromisoformat(since_string.replace("Z", "+00:00"))))
        contacts = []
        for blob in self.contact_dict.map_over_all_data(since, now):
            contacts.append(blob)
        locations = []
        for blob in self.spatial_dict.map_over_all_data(since, now):
            locations.append(blob)

        ret = {'now':time.strftime('%Y-%m-%dT%H:%M:%SZ', now),
               'since':since_string}

        if 0 != len(contacts):
            ret['contacts'] = contacts
        if 0 != len(locations):
            ret['locations'] = locations
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

    # reset should only be called and allowed if testing
    def reset(self):
        if self.testing:
            logger.info('resetting ids')
            self.spatial_dict = SpatialDict(self.directory_root)
            self.contact_dict = ContactDict(self.directory_root)
            # TODO-DAN - I think if its not in self.testing it should return a 403
        return
