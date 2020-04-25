# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import datetime
import rtree
import string
import random
import copy
from lib import hash_nonce, fold_hash

def unix_time(dt):
    return int(dt.timestamp())

os.umask(0o007)


logger = logging.getLogger(__name__)


# like a dict but the values can be auto extended, is if you want to set a[B][C] you don't have to initialize B apriori
class ContactDict(dict):

    def __missing__(self, key):
        self[key] = ContactDict()
        return self[key]

class UpdateTokenIdIdx(dict):

    def store(self, updatetoken, file_name):
        self[updatetoken] = file_name

class UpdateTokenGeoIdx(dict):

    def store(self, updatetoken, obj):
        self[updatetoken] = obj

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
        self.spatial_index = SpatialIndex('%s/rtree' % self.directory_root)
        self.testing = ('True' == config.get('testing', ''))
        self.ids = ContactDict()
        self.updatetoken_id_idx = UpdateTokenIdIdx()
        self.updatetoken_geo_idx = UpdateTokenGeoIdx()
        self.id_count = 0
        self._load_ids_from_filesystem()
        self._load_updatetoken_geo_idx()
        return


    def execute_route(self, name, *args):
        return registry[name](self, *args)

    def _load_updatetoken_geo_idx(self):
        for obj in self.spatial_index.map_over_objects():
            if obj.updatetoken:
                self.updatetoken_geo_idx.store(obj.updatetoken, obj)

    def _load_ids_from_filesystem(self):
        for root, sub_dirs, files in os.walk(self.directory_root):
            for file_name in files:
                if file_name.endswith('.data'):
                    (code, ignore, date, extension) = file_name.split('.')
                    dirs = root.split('/')[-3:]
                    contact_dates = self.ids[dirs[0]][dirs[1]][dirs[2]]
                    date = int(date)
                    dates = [date]
                    if code in contact_dates:
                        dates = contact_dates[code]
                        dates.append(date)
                    self.id_count += 1
                    self.ids[dirs[0]][dirs[1]][dirs[2]][code] = dates

                    # Note this is expensive, it has to read each file to find updatetokens - maintaining an index would be better.
                    blob = json.load(open(('%s/%s/%s/%s/%s' % (self.directory_root, dirs[0], dirs[1], dirs[2], file_name))))
                    updatetoken = blob.get('updatetoken', None)
                    if updatetoken:
                        self.updatetoken_id_idx.store(updatetoken, file_name)
        return
    
    def close(self):
        logging.info('closing spatial index file')
        self.spatial_index.close()
        return

    # used to start JSON_DATA at NOW for CONTACT_ID, if CONTACT_ID has other unique JSON_DATA then a new one will be stored
    def _store_id(self, contact_id, json_data, now):
        first_level, second_level, third_level = self._return_contact_keys(contact_id)
        dir_name = "%s/%s/%s/%s" % (self.directory_root, first_level, second_level, third_level)
        os.makedirs(dir_name, 0o770, exist_ok = True)

        # we add some randomness to the name so we deal with the case of the same contact_id coming in within a minute (which is
        # the resolution of now
        
        random_string = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(8)])
        file_name = '%s.%s.%d.data' % (contact_id, random_string, now)
        file_path = '%s/%s' % (dir_name, file_name)
        logger.info('writing %s to %s' % (json_data, file_path))
        with open(file_path, 'w') as file:
            json.dump(json_data, file)
        try:
            dates = self.ids[first_level][second_level][third_level].get(contact_id, [])
        except KeyError:
            dates = []
        dates.append(now)
        self.ids[first_level][second_level][third_level][contact_id] = dates
        self.id_count += 1
        updatetoken = json_data.get('updatetoken')
        if updatetoken:
            self.updatetoken_id_idx.store(updatetoken, file_name)
        return

    def _store_geo(self, location):
        lat = float(location['lat'])
        long = float(location['long'])
        # make a unique id
        logger.info('inserting %s at lat: %f, long: %f' % (location, lat, long))
        self.spatial_index.append(lat, long, location)
        updatetoken = location.get('updatetoken')
        if updatetoken:
            self.updatetoken_geo_idx.store(updatetoken, location)

    # get the three levels for both the memory and directory structure
    def _return_contact_keys(self, contact_id):
        return contact_id[0:2].upper(), contact_id[2:4].upper(), contact_id[4:6].upper()

    # Get the directory name
    def _return_dir_name(self, contact_id):
        first_level, second_level, third_level = self._return_contact_keys(contact_id)
        return "%s/%s/%s/%s" % (self.directory_root, first_level, second_level, third_level)

    # return all contact json contents since SINCE for CONTACT_ID
    def _get_json_blobs(self, contact_id, since = None):
        dir_name = self._return_dir_name(contact_id)
        blobs = []
        if os.path.isdir(dir_name):
            for file_name in os.listdir(dir_name):
                if file_name.endswith('data'):
                    (code, ignore, date, extension) = file_name.split('.')
                    if code == contact_id:
                        if (not since) or (since <= int(date)):
                            blobs.append(json.load(open(('%s/%s' % (dir_name, file_name)))))
        return blobs


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
            if contact in self._get_json_blobs(contact_id):
                logger.info('contact for id: %s already found, not saving' % contact_id)
            else:
                self._store_id(contact_id, contact, now)
        for location in data.get('locations', []):
            location['date'] = now
            location.update(repeated_fields)
            self._store_geo(location)
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
                #TODO-33 Storing this replaces doesn't prove anything - since just folded to make updatetoken
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

    # return all ids that match the prefix
    # TODO-DAN - would this be better as a method on Contact_Dict - YES either of us can change
    def _get_matching_contacts(self, prefix, ids, start_pos = 0):
        matches = []
        if start_pos < 6:
            this_prefix = prefix[start_pos:]
            if len(this_prefix) >= 2:
                ids = ids.get(this_prefix[0:2])
                if ids:
                    matches = self._get_matching_contacts(prefix, ids, start_pos + 2)
            else:
                if 0 == len(this_prefix):
                    prefixes = [('%02x' % i).upper() for i in range(0,256)]
                else:
                    hex_char = this_prefix[0]
                    prefixes = [('%s%01x' % (hex_char, i)).upper() for i in range(0,16)]
                for this_prefix in prefixes:
                    these_ids = ids.get(this_prefix)
                    if these_ids:
                        matches = matches + self._get_matching_contacts(prefix, these_ids, start_pos + 2)
        else:
            matches = list(filter(lambda x: x.startswith(prefix), ids.keys()))
        return matches

    # scan_status post
    @register_method(route = '/status/scan')
    def scan_status(self, data, args):
        since = data.get('since')
        ret = {}
        if since:
            ret['since'] = since
            since = int(unix_time(datetime.datetime.fromisoformat(since.replace("Z", "+00:00"))))
        else:
            ret['since'] = "1970-01-01T01:01Z"

        prefixes = data.get('contact_prefixes')
        if prefixes:
            matched_ids = []
            for prefix in prefixes:
                for contact in self._get_matching_contacts(prefix, self.ids):
                    matched_ids = matched_ids + self._get_json_blobs(contact, since)
            ret['ids'] = matched_ids

        # Find any reported locations, inside the requests bounding box.
        # { locations: [ { minLat...} ] }
        req_locations = data.get('locations')
        locations = []
        if req_locations:
            for bounding_box in req_locations:
                for location in self.spatial_index.get_objects_in_bounding_box(bounding_box['minLat'], bounding_box['minLong'], bounding_box['maxLat'], bounding_box['maxLong']):
                    if (not since) or (since <= location['date']):
                        locations.append(location)
            ret['locations'] = locations
        ret['now'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        return ret

    # sync get
    @register_method(route = '/sync')
    def sync(self, data, args):
        # Note that any replaced items will be sent as new items, so there is no need for a separate list of nonces.

        since_string = args.get('since')
        if since_string:
            since_string = since_string[0].decode()
        else:
            since_string = "1970-01-01T01:01Z"

        since = int(unix_time(datetime.datetime.fromisoformat(since_string.replace("Z", "+00:00"))))
        contacts = []
        for key1, value1 in self.ids.items():
            for key2, value2 in self.ids[key1].items():
                for key3, value3 in self.ids[key1][key2].items():
                    for contact_id in self.ids[key1][key2][key3].keys():
                        contacts = contacts + self._get_json_blobs(contact_id, since)

        locations = []
        for obj in self.spatial_index.map_over_objects():
            if (not since) or (since <= obj['date']):
                locations.append(obj)
        
        ret = {'now':time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
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
            'bounding_box' : self.spatial_index.bounds,
            'geo_points' : len(self.spatial_index),
            'contacts_count': self.id_count
        }
        return ret

    # reset should only be called and allowed if testing
    def reset(self):
        if self.testing:
            logger.info('resetting ids')
            self.ids = ContactDict()
            self.updatetoken_id_idx = UpdateTokenIdIdx()
            self.spatial_index = SpatialIndex('%s/rtree' % self.directory_root)
        return
