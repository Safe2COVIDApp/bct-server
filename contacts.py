# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import calendar
import datetime
import rtree
import string
import random
import copy
from collections import defaultdict
from lib import hash_nonce, fold_hash, random_ascii

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


class FSBackedThreeLevelDict():

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
                        self.update_index[updatetoken, file_name]
        return
        

    def _make_key(self, key):
        raise NotImplementedError
    
    def _insert_disk(self, original_key, new_key, value, date):
        raise NotImplementedError

    def _get_directory_name_and_chunks(self, key):
        chunks = [key[i:i+2] for i in [0, 2, 4]]
        dir_name = '%s/%s/%s/%s' % (self.directory, chunks[0], chunks[1], chunks[2])
        return (chunks, dir_name)

        
    def insert(self, key, value, date):
        if str != type(key):
            key = self._make_key(key)
        if value in self.map_over_json_blobs(key, None, None):
            logger.warning('%s already in data for %s' % (value, key))
            return
        if 6 > len(key):
            raise Exception("Key %s must by at least 6 characters long" % key)
        key = key.upper()


        chunks, dir_name = self._get_directory_name_and_chunks(key)
        
        # first put this date into the item list
        if list != type(self.items[chunks[0]][chunks[1]][chunks[2]][key]):
            self.items[chunks[0]][chunks[1]][chunks[2]][key] = [date]
        else:
            self.items[chunks[0]][chunks[1]][chunks[2]][key].append(date)

        os.makedirs(dir_name, 0o770, exist_ok = True)
        file_path = '%s/%s.%s.%d.data'  % (dir_name, key, random_ascii(6), date)
        logger.info('writing %s to %s' % (value, file_path))
        with open(file_path, 'w') as file:
            json.dump(value, file)
        
        self._insert_disk(key, value, date)
        self.item_count += 1
        return

    def map_over_matching_data(self, key, since, now, start_pos = 0):
        raise NotImplementedError

    def __len__(self):
        return self.item_count
    
    def map_over_json_blobs(self, key, since, now):
        chunks, dir_name = self._get_directory_name_and_chunks(key)
        if os.path.isdir(dir_name):
            for file_name in os.listdir(dir_name):
                if file_name.endswith('data'):
                    (code, ignore, date, extension) = file_name.split('.')
                    if (code == key) and _good_date(int(date), since, now):
                        yield json.load(open(('%s/%s' % (dir_name, file_name))))

        return

    def map_over_all_data(self, since = None, now = None):
        for key1, value1 in self.items.items():
            for key2, value2 in self.items[key1].items():
                for key3, value3 in self.items[key1][key2].items():
                    for key in self.items[key1][key2][key3].keys():
                        yield from self.map_over_json_blobs(key, since, now)

        

class ContactDict(FSBackedThreeLevelDict):

    def __init__(self, directory):
        directory = directory + '/contact_dict'
        os.makedirs(directory, 0o770, exist_ok = True)
        super().__init__(directory)

    def _insert_disk(self, key, value, date):
        logger.info('ignoring _insert_disk for ContactDict')
        return


    def _map_over_matching_contacts(self, prefix, ids, since, now, start_pos = 0):
        logger.info('_map_over_matching_contacts called with %s, %s, %s, %s' % (prefix, ids.keys(), since, now))
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
                yield from self.map_over_json_blobs(contact_id, since, now)
        return


    def map_over_matching_data(self, key, since, now):
        yield from self._map_over_matching_contacts(key, self.items, since, now)
        return

class SpatialDict(FSBackedThreeLevelDict):
    def __init__(self, directory):
        directory = directory + '/spatial_dict'
        os.makedirs(directory, 0o770, exist_ok = True)
        super().__init__(directory)
        self.spatial_index = rtree.index.Index('%s/rtree' % directory)
        self.keys = {}
        return
        
    def _make_key(self, key):
        (lat, long) = key
        val = int(lat * long)
        ret = self.keys.get(val)
        if not ret:
            ret = random_ascii(10).upper()
            self.keys[val] = ret
            self.keys[ret] = key
        return ret

    def _insert_disk(self, key, value, date):
        original_key = self.keys[key]
        (lat, long) = original_key
        self.spatial_index.insert(int(lat * long),  (lat, long, lat, long), obj = key)
        self.spatial_index.close()
        self.spatial_index = rtree.index.Index('%s/rtree' % self.directory)
        return

    @property
    def bounds(self):
        return self.spatial_index.bounds

    def map_over_matching_data(self, key, since, now):
        for obj in self.spatial_index.intersection(key, objects = True):  # [ object: [ obj, ob], object: [ obj, obj]]
            yield from self.map_over_json_blobs(obj.object, since, now)
        return

    def close(self):
        self.spatial_index.close()
        return

# used for contact ids
class GContactDict:

    @staticmethod
    def dictionary_factory():
        return defaultdict(ContactDict.dictionary_factory)

    def __init__(self):
        self.items = dictionary_factory()

    # return all ids that match the prefix
    def _get_matching_contacts(self, prefix, start_pos = 0):
        matches = []
        if start_pos < 6:
            this_prefix = prefix[start_pos:]
            if len(this_prefix) >= 2:
                next_ids = self.get(this_prefix[0:2])
                if next_ids:
                    matches = next_ids._get_matching_contacts(prefix, start_pos + 2)
            else:
                if 0 == len(this_prefix):
                    prefixes = [('%02x' % i).upper() for i in range(0,256)]
                else:
                    hex_char = this_prefix[0]
                    prefixes = [('%s%01x' % (hex_char, i)).upper() for i in range(0,16)]
                for this_prefix in prefixes:
                    these_ids = self.get(this_prefix)
                    if these_ids:
                        matches = matches + these_ids._get_matching_contacts(prefix, start_pos + 2)
        else:
            matches = list(filter(lambda x: x.startswith(prefix), self.keys()))
        return matches
    


# like a dict but the values can be auto extended, is if you want to set a[B][C] you don't have to initialize B apriori
class fContactDict(dict):

    def __missing__(self, key):
        self[key] = ContactDict()
        return self[key]

    # return all ids that match the prefix
    # TODO-DAN - would this be better as a method on Contact_Dict - YES either of us can change
    def _get_matching_contacts(self, prefix, start_pos = 0):
        matches = []
        if start_pos < 6:
            this_prefix = prefix[start_pos:]
            if len(this_prefix) >= 2:
                next_ids = self.get(this_prefix[0:2])
                if next_ids:
                    matches = next_ids._get_matching_contacts(prefix, start_pos + 2)
            else:
                if 0 == len(this_prefix):
                    prefixes = [('%02x' % i).upper() for i in range(0,256)]
                else:
                    hex_char = this_prefix[0]
                    prefixes = [('%s%01x' % (hex_char, i)).upper() for i in range(0,16)]
                for this_prefix in prefixes:
                    these_ids = self.get(this_prefix)
                    if these_ids:
                        matches = matches + these_ids._get_matching_contacts(prefix, start_pos + 2)
        else:
            matches = list(filter(lambda x: x.startswith(prefix), self.keys()))
        return matches

class UpdateTokenIdIdx(dict):

    def store(self, updatetoken, file_name):
        self[updatetoken] = file_name

class UpdateTokenGeoIdx(dict):

    def store(self, updatetoken, obj):
        self[updatetoken] = obj

# MITRA -- keeping this class so you can refer to it, delete when you are ready to
class SpatialIndex:
    def __init__(self, file_path):
        self.file_path = file_path
        self.index = rtree.index.Index(file_path)
        return


    def get_objects_in_bounding_box(self, min_lat, min_long, max_lat, max_long):
        objectss = self.index.intersection((min_lat, min_long, max_lat, max_long), objects = True)  # [ object: [ obj, ob], object: [ obj, obj]]
        # Flatten array of arrays
        objs = []
        for o in objectss:
            objs.extend(o.object)
        return objs

    def get_objects_at_point(self, lat, long):
        return self.get_objects_in_bounding_box(lat, long, lat, long)

    def append(self, lat, long, obj):
        objects = self.get_objects_at_point(lat, long)
        objects.append(obj)
        self.insert(lat, long, obj = objects)
        return obj

    # Pair of append's return - for now assuming can rely on obj, but that might not be true (requires knowledge of rtree internals)
    def retrieve(self, ptr):
        return ptr

    def insert(self, lat, long, obj):
        self.index.insert(int(lat * long),  (lat, long, lat, long), obj = obj)
        self.flush()
        return
        
    @property
    def bounds(self):
        return self.index.bounds
    
    def flush(self):
        # slow...
        self.index.close()
        self.index = rtree.index.Index(self.file_path)
        return

    def close(self):
        self.index.close()
        return
    
    def __len__(self):
        return self.index.get_size()

    def map_over_objects(self, bounding_box = None):
        if 0 != len(self):
            if not bounding_box:
                bounding_box = self.index.bounds
            for objs in self.index.intersection(bounding_box, objects = True): # [ object: [ obj ] ]
                for obj in objs.object:
                    yield obj
        return

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
        return


    def execute_route(self, name, *args):
        return registry[name](self, *args)

    def close(self):
        logging.info('closing spatial index file')
        self.spatial_dict.close()
        return


    # send_status POST
    # { locations: [ { minLat, updatetoken, ...} ], contacts: [ { id, updatetoken, ... } ], memo, replaces, status, ... ]
    @register_method(route = '/status/send')
    def send_status(self, data, args):
        logger.info('in send_status')
        now = int(time.time())

        repeated_fields = {}
        # These are fields allowed in the send_status, and just copied from top level into each data point
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

    # status_update POST
    # { locations: [ { minLat, updatetoken, ...} ], contacts: [ { id, updatetoken, ... } ], memo, replaces, status, ... ]
    @register_method(route = '/status/update')
    def status_update(self, data, args):
        logger.info('in status_update')
        now = int(time.time())

        nextkey = data.get('replaces') # This is a nonce, that is one before the first key
        length = data.get('length') # This is how many to replace
        if length:
            for i in range(length):
                updatetokens = data.get('updatetokens',[])
                nextkey = hash_nonce(nextkey)
                #TODO-55 Storing this replaces doesn't prove anything - since just folded to make updatetoken
                updates = {'replaces': nextkey, 'status': data.get('status'), 'updatetoken': updatetokens.pop()}  # SEE-OTHER-ADD-FIELDS
                file_name = self.updatetoken_id_idx.get(fold_hash(nextkey))
                if file_name:
                    (contact_id, ignore, date, extension) = file_name.split('.')
                    dir_name = self._return_dir_name(contact_id)
                    json_data = json.load(open(('%s/%s' % (dir_name, file_name))))
                    json_data.update(updates)
                    # Store in the structure with the new info
                    self._store_id(contact_id, json_data, now)
                else:  # Cannot be filename and geo_obj for same updatetoken
                    geo_obj = self.updatetoken_geo_idx.get(fold_hash(nextkey))
                    if geo_obj:
                        location = copy.deepcopy(geo_obj)
                        location['date'] = now
                        location.update(updates)
                        self._store_geo(location)
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
            ret['ids'] = []
            for prefix in prefixes:
                for blob in self.contact_dict.map_over_matching_data(prefix, since, now):
                    ret['ids'].append(blob)

        # Find any reported locations, inside the requests bounding box.
        # { locations: [ { minLat...} ] }
        req_locations = data.get('locations')
        locations = []
        if req_locations:
            ret['locations'] = []
            for bounding_box in req_locations:
                for blob in self.spatial_dict.map_over_matching_data((bounding_box['minLat'], bounding_box['minLong'], bounding_box['maxLat'], bounding_box['maxLong']),
                                                                     since, now):
                    ret['locations'].append(blob)
        ret['now'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', now)
        return ret

    # sync get
    @register_method(route = '/sync')
    def sync(self, data, args):
        # Note that any replaced items will be sent as new items, so there is no need for a separate list of nonces.
        now = time.gmtime()  # Do this at the start of the process, we want to guarrantee have all before this time (even if multithreading)
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
        return
