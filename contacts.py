# the module contains the client and server process to manage ids

from twisted.logger import Logger
import os
import json
import rtree
import copy
from collections import defaultdict
from lib import get_update_token, replacement_token, random_ascii, current_time, unix_time_from_iso, \
    iso_time_from_seconds_since_epoch
from blist import sortedlist

os.umask(0o007)

logger = Logger()


# Return a matching date - see issue#57 for discussion of a valid date
# Essentially is date < now to return all items in anything other than the current second
# that is to make sure that if an event arrives in the same second, we know for sure that it was NOT included,
# no matter if after or before this sync or scan_status
# And is since <= date so that passing back now will get any events that happened on that second
# All times are floating point seconds since the epoch
def _good_date(date, since=None, now=None):
    return ((not since) or (since <= date)) and ((not now) or (date < now))


# For now, all we do is capture these as statistics, later we could capture in a table and analyse
init_statistics_fields = ['application_name', 'application_version', 'phone_type', 'region', 'health_provider',
                          'language', 'status']

# == Some names
# chunks = [ 'AB', 'CD', 'EF' ]
# dir_name = 'AB/CD/EF'
# file_name = 'ABCDEF123.data'
# file_path = dir_name/file_name = 'AB/CD/EF/ABCDEF123.data'
# blob = {...} the data object stored at each point
# key_tuple = (float lat, float long)
# key_string = Random string

# == And Some Short cuts ....
# FSBackedThreeLevelDict.get_directory_name_and_chunks(key) -> chunks, dir_name
# FSBackedThreeLevelDict.get_file_path_from_file_name(file_name) -> file_path
# FSBackedThreeLevelDict._get_parts_from_file_name(file_name) -> key, floating_seconds, serial_number
# DICT.get_bottom_level_from_key(key) -> { key: [(floating_seconds, serial)]}
# DICT.retrieve_json_from_file_path(file_path) -> blob
# ContactDict._key_string_from_blob(blob) -> blob['id']
# SpatialDict._key_string_from_blob(blob) -> key_string
# SpatialDict._key_tuple_from_blob(blob) -> key_tuple
# SpatialDict._make_key(key_tuple) -> key_string
# key in UpdatesDict -> bool
# UpdatesDict[key] -> [blob]

# TODO pass through and add here

class FSBackedThreeLevelDict:

    @staticmethod
    def dictionary_factory():
        return defaultdict(FSBackedThreeLevelDict.dictionary_factory)

    def __init__(self, directory):
        # { AA: { BB: { CC: AABBCCDEF123: [(floatingseconds, serialnumber)] } } }
        self.items = FSBackedThreeLevelDict.dictionary_factory()
        self.item_count = 0
        self.update_index = {}
        # [ (floating_seconds, serial_number)* ] used to order data by time
        self.sorted_list_by_time_and_serial_number = sortedlist(key=lambda key: key[0])
        # { (floating_seconds, serial_number): relative_file_path }
        self.time_and_serial_number_to_file_path_map = {}
        self.directory = directory
        os.makedirs(directory, 0o770, exist_ok=True)
        self._load()
        # file paths that are pending deletion
        self.file_paths_to_delete = []
        return

    @staticmethod
    def _get_parts_from_file_name(file_name):
        """
        Pull apart a file name
        file_name: key:floating_seconds:serial_number.data
        returns (key, floating_seconds, serial_number)
        """
        simple_file_name = file_name.replace('.data', '')
        parts = simple_file_name.split(':')
        key = parts[0]
        floating_seconds = float(parts[1])
        serial_number = int(parts[2])
        return key, floating_seconds, serial_number

    def _add_to_items(self, key, floating_seconds_and_serial_number):
        bottom_level = self.get_bottom_level_from_key(key)  # { key: [(floating_seconds, serial)]
        if key in bottom_level:  # Already at least one item for this key
            bottom_level[key].append(floating_seconds_and_serial_number)
        else:
            bottom_level[key] = [floating_seconds_and_serial_number]
        self.item_count += 1

    def _add_to_items_and_indexes(self, key, floating_seconds, serial_number, file_name, relative_file_path, update_token):
        floating_seconds_and_serial_number = (floating_seconds, serial_number)
        self.time_and_serial_number_to_file_path_map[floating_seconds_and_serial_number] = relative_file_path
        self._add_to_items(key, floating_seconds_and_serial_number)
        self.sorted_list_by_time_and_serial_number.add(floating_seconds_and_serial_number)
        if update_token:
            self.update_index[update_token] = file_name

    def _load(self):
        """
        This creates the data structures that correspond to what is on disk
        """
        for root, sub_dirs, files in os.walk(self.directory):
            for file_name in files:
                if file_name.endswith('.data'):
                    (key, floating_seconds, serial_number) = FSBackedThreeLevelDict._get_parts_from_file_name(file_name)
                    relative_file_path = FSBackedThreeLevelDict.get_file_path_from_file_name(file_name)
                    # Note this is expensive, it has to read each file to find update_tokens
                    # - maintaining an index would be better.
                    blob = json.load(open('/'.join([root, file_name])))
                    update_token = blob.get('update_token')
                    self._add_to_items_and_indexes(key, floating_seconds, serial_number, file_name, relative_file_path, update_token)
                    self._load_key(key, blob)
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

    def _remove_key(self, key, blob):
        """
        _remove_key can be subclassed to remove association of a key with data stored at that key

        Parameters:
        ----------
        key -- String
               Index into the FS and dictionary
        blob -- Dict
                Data associated with Key
        """
        return

    def _insert_disk(self, key):
        """
        _insert_disk does subclass dependent insertion into both memory and filesystem
        
        """
        raise NotImplementedError

    def _key_string_from_blob(self, blob):
        raise NotImplementedError

    @staticmethod
    def get_directory_name_and_chunks(key):
        chunks = [key[i:i + 2] for i in [0, 2, 4]]
        return chunks, "/".join(chunks)

    def insert(self, key, value, floating_seconds, serial_number):
        """
        Insert value object at key with date, keep various indexes to it (update_index)

        Parameters:
        -----------
        key -- String || Object Either the contact id, or tuple or lat/long - if None, will be got from the blob
        value -- Dict object needing storing
        floating_seconds -- unix time
        serial_number -- int 
        """
        if key is None:
            key = self._key_string_from_blob(value) # On Spatial dict Implicitly does a make_key allowing _insert_disk to work below
        # we are NOT going to read multiple things from the file system for performance reasons
        # if value in self.map_over_json_blobs(key, None, None):
        #    logger.warning('%s already in data for %s' % (value, key))
        #    return
        update_token = value.get('update_token')
        if update_token in self.update_index:
            logger.info("Silently ignoring duplicate of update token: %s" % update_token)
        else:
            if 6 > len(key):
                raise Exception("Key %s must by at least 6 characters long" % key)
            key = key.upper()
            chunks, dir_name = FSBackedThreeLevelDict.get_directory_name_and_chunks(key)
            file_name = '%s:%f:%s.data' % (key, floating_seconds, serial_number)
            relative_file_path = '%s/%s' % (dir_name, file_name)
            # Put in the in-memory data structures
            self._add_to_items_and_indexes(key, floating_seconds, serial_number, file_name, relative_file_path, update_token)
            # Now put in the file system
            os.makedirs(self.directory + '/' + dir_name, 0o770, exist_ok=True)
            logger.info('writing {value} to {directory}', value=value, directory=self.directory + '/' + relative_file_path)
            with open(self.directory + '/' + relative_file_path, 'w') as file:
                json.dump(value, file)
            self._insert_disk(key)   # Depends on _key_string_from_blob above
        return

    def map_over_matching_data(self, key, since, now):
        """
        Sublass dependent fetch,
        key may vary between classes - for Contacts its a prefix, for Spatial dict its a bounding_box
        returns list of relative file_paths [ 'AA/BB/CC/AABBCC1234.data' ]
        """
        raise NotImplementedError

    def __len__(self):
        return self.item_count

    def retrieve_json_from_file_path(self, file_path):
        return json.load(open(self.directory + '/' + file_path))

    def retrieve_json_from_file_name(self, file_name):
        return self.retrieve_json_from_file_path(FSBackedThreeLevelDict.get_file_path_from_file_name(file_name))

    def retrieve_json_from_file_paths(self, file_paths):
        for file_path in file_paths:
            yield self.retrieve_json_from_file_path(file_path)
        return

    def _delete(self, file_path):
        logger.info("deleting {file_path}", file_path=file_path)
        blob = self.retrieve_json_from_file_path(file_path)
        update_token = blob.get('update_token')
        if update_token:
            del self.update_index[update_token]
        os.remove(self.directory + "/" + file_path)
        return

    def delete_from_deletion_list(self):
        logger.info('there are {count} items to delete', count=len(self.file_paths_to_delete))
        while 0 != len(self.file_paths_to_delete):
            file_path = self.file_paths_to_delete.pop()
            self._delete(file_path)
        return

    def get_bottom_level_from_key(self, key):
        chunks, dir_name = FSBackedThreeLevelDict.get_directory_name_and_chunks(key)
        return self.items[chunks[0]][chunks[1]][chunks[2]]

    def move_data_by_key_to_deletion(self, key):
        bottom_level = self.get_bottom_level_from_key(key)
        if key in bottom_level:
            self.move_data_list_to_deletion(bottom_level[key])
            del bottom_level[key]

    def move_expired_data_to_deletion_list(self, since, until):
        """ 
        take old data and move it to the deletion list, but don't do the deletion 

        Parameters
        ----------
        since -- unix time 
        until -- unix time
        """

        deletion_list = list(self.sorted_list_by_time_and_serial_number[self.sorted_list_by_time_and_serial_number.bisect_left((since, 0)):
                                                                        self.sorted_list_by_time_and_serial_number.bisect_left((until, 0))])
        self.move_data_list_to_deletion(deletion_list)

    def move_data_list_to_deletion(self, deletion_list):
        for item in deletion_list:
            self.sorted_list_by_time_and_serial_number.remove(item)
            file_path = self.time_and_serial_number_to_file_path_map[item]
            logger.info("moving {file_path} to deletion list", file_path=file_path)
            file_name = file_path.split('/')[-1]
            (key, floating_seconds, serial_number) = FSBackedThreeLevelDict._get_parts_from_file_name(file_name)
            bottom_level = self.get_bottom_level_from_key(key)
            bottom_level[key].remove(item)
            self.file_paths_to_delete.append(file_path)
            del self.time_and_serial_number_to_file_path_map[item]
            self.item_count -= 1
        return

    @staticmethod
    def get_file_path_from_file_name(file_name):
        components = file_name.split(':')
        chunks, dir_name = FSBackedThreeLevelDict.get_directory_name_and_chunks(components[0])
        return "%s/%s" % (dir_name, file_name)

    def update(self, updating_token, updates, now, serial_number):
        """
        Look for an entry matching updating_token, add a new one after modifying with updates

        :param updating_token: folded hash 16 character string
        :param updates:      { update_token, replaces, status }
        :param now:          unix time
        :param serial_number int
        :return:             True if succeeded
        """
        file_name = self.update_index.get(updating_token)
        if file_name:
            blob = self.retrieve_json_from_file_name(file_name)
            blob.update(updates)
            self.insert(None, blob, now, serial_number)
            return True
        else:
            return False


class ContactDict(FSBackedThreeLevelDict):

    def __init__(self, directory):
        directory = directory + '/contact_dict'
        super().__init__(directory)

    def _insert_disk(self, key):
        """
        Subclass dependent part of _insert, nothing to do
        """
        # logger.info('ignoring _insert_disk for ContactDict')
        return

    def _map_over_matching_contacts(self, prefix, ids, since, now, start_pos=0):
        logger.info('_map_over_matching_contacts called with {prefix}, {keys}', prefix=prefix, keys=ids.keys())
        if start_pos < 6:
            this_prefix = prefix[start_pos:]
            if len(this_prefix) >= 2:
                ids = ids.get(this_prefix[0:2])
                if ids:
                    yield from self._map_over_matching_contacts(prefix, ids, since, now, start_pos + 2)
            else:
                if 0 == len(this_prefix):
                    prefixes = [('%02x' % i).upper() for i in range(0, 256)]
                else:
                    hex_char = this_prefix[0]
                    prefixes = [('%s%01x' % (hex_char, i)).upper() for i in range(0, 16)]
                for this_prefix in prefixes:
                    these_ids = ids.get(this_prefix)
                    if these_ids:
                        yield from self._map_over_matching_contacts(prefix, these_ids, since, now, start_pos + 2)
        else:
            for contact_id in filter(lambda x: x.startswith(prefix), ids.keys()):
                for (floating_time, serial_number) in ids[contact_id]:
                    if _good_date(floating_time, since, now):
                        file_name = '%s:%f:%d.data' % (contact_id, floating_time, serial_number)
                        yield FSBackedThreeLevelDict.get_file_path_from_file_name(file_name)
        return

    def map_over_matching_data(self, key, since, now):
        """
        Return relative file paths that match the prefix and are between the times
        """
        yield from self._map_over_matching_contacts(key, self.items, since, now)
        return

    def _key_string_from_blob(self, blob):
        return blob.get('id')


class SpatialDict(FSBackedThreeLevelDict):

    def __init__(self, directory):
        directory = directory + '/spatial_dict'
        self.spatial_index = rtree.index.Index()  # Geospatial index to key_string
        self.keys = {}  # Maps key_tuple to key_QQ1
        self.coords = {}  # Maps key_QQ1 to key_tuple
        super().__init__(directory)
        return

    @staticmethod
    def _key_tuple_from_blob(blob):
        return float(blob['lat']), float(blob['long'])

    def _load_key(self, key_string, blob):
        key_tuple = SpatialDict._key_tuple_from_blob(blob)
        self.keys[key_tuple] = key_string
        self.coords[key_string] = key_tuple
        return

    # _remove_key is unnecessary, there might be other data at this point, and doesnt hurt to leave extra points in place

    def _make_key(self, key_tuple):
        """ 
        Return key string from lat,long

        returns a unique string (should be over 6 characters, but long enough to avoid collisions)
        that can be used to index into the three level filesystem

        Parameters:
        ----------
        self      -- FSBackedThreeLevelDict
        key_tuple -- (float lat, float long)

        """
        key_string = self.keys.get(key_tuple)
        if not key_string:
            key_string = random_ascii(10).upper()
            self.coords[key_string] = key_tuple
        return key_string

    def _insert_disk(self, key_string):
        """
        Subclass dependent part of _insert, add to indexes
        """
        (lat, long) = coords = self.coords[key_string]

        # only insert if coords not currently in keys
        if coords not in self.keys:
            # we can always use the 0 for the id, duplicates are allowed
            self.spatial_index.insert(0, (lat, long, lat, long), obj=key_string)
            self.keys[coords] = key_string
        return

    @property
    def bounds(self):
        return self.spatial_index.bounds

    # key is a bounding box tuple (min_lat, min_long, max_lat, max_long) as floats
    # return relative file paths to data inside that bounding box
    def map_over_matching_data(self, key, since, now):
        for obj in self.spatial_index.intersection(key, objects=True):  # [ object: [ obj, ob], object: [ obj, obj]]
            key = obj.object
            for (floating_time, serial_number) in self.get_bottom_level_from_key(key)[key]:
                if _good_date(floating_time, since, now):
                    file_name = '%s:%f:%d.data' % (key, floating_time, serial_number)
                    yield FSBackedThreeLevelDict.get_file_path_from_file_name(file_name)
        return

    def _key_string_from_blob(self, blob):
        key_tuple = SpatialDict._key_tuple_from_blob(blob)
        return self._make_key(key_tuple)


class SimpleFSBackedDict(FSBackedThreeLevelDict):
    """
    Define a simple file-system backed dictionary that (currently) supports the functionality of ....
    dict("/top-level-dir", /subdir)   Create a new dictionary backed into this directory
    x in dict   True if an object has been added
    dict[x]     Returns [blob*] objects added
    del dict[x] Delete all objects at x
    dict.insert(key, val, floating_seconds, serial_number)
    dict.map_over_matching_data
    """

    def __init__(self, directory, subdir):
        directory = directory + subdir
        super().__init__(directory)

    def _insert_disk(self, key): # Not required
        return

    def map_over_matching_data(self, key, since, now):
        """
        returns: [file_path]
        """
        bottom_level = self.get_bottom_level_from_key(key)
        chunks, dir_name = FSBackedThreeLevelDict.get_directory_name_and_chunks(key)  # e.g. ['ab','cd','12'], 'ab/cd/12'
        yield from ["%s/%s:%s:%s.data" % (dir_name, key, floating_seconds, serial_number) for floating_seconds, serial_number in bottom_level[key] if _good_date(floating_seconds, since, now)]
        return

    # Key string will be in the file name, but not in the blob
    def _key_string_from_blob(self, blob):
        raise NotImplementedError

    def __contains__(self, key):
        chunks, dir_name = FSBackedThreeLevelDict.get_directory_name_and_chunks(key)
        return key in self.items[chunks[0]][chunks[1]][chunks[2]]

    def __getitem__(self, key):
        for file_path in self.map_over_matching_data(key, None, None):
            yield self.retrieve_json_from_file_path(file_path)

    def __delitem__(self, key):
        self.move_data_by_key_to_deletion(key)


class UpdatesDict(SimpleFSBackedDict):
    # key is update_token
    # Blob is { status, ... }

    def __init__(self, directory):
        super().__init__(directory, '/updates_dict')

# contains both the code for the in memory and on disk version of the database
# The in memory is a four deep hash table where the leaves of the hash are:
#   list of dates (as integers for since compares) of when contact data# has come in.


# for an id "DEADBEEF", the in memory version is stored in self.ids in the element
# self.ids['DE']['AD']['BE']["DEADBEEF']  for the disk version is is store in a four
# level directory structure rooted at config['directory'] in 'DE/AD/BE/DEADBEEF.[FLOATING_TIME].[SERIAL_NUMBER].data'
# [FLOATING_TIME] is the floating number of seconds since the epoch 
# [SERIAL_NUMBER] is a unique serial number for multiple items that might come in at the same time
# (accuracy is to minutes).  The date strings are 'YYYYMMDDHHmm'

registry = {}


def register_method(_func=None, *, route):
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

    def __init__(self, config_top):
        config = config_top['DEFAULT']
        self.config = config
        self.directory_root = config['directory']
        self.testing = ('True' == config.get('testing', ''))
        self.spatial_dict = SpatialDict(self.directory_root)
        self.contact_dict = ContactDict(self.directory_root)
        self.bb_min_dp = config.getint('bounding_box_minimum_dp', 2)
        self.bb_max_size = config.getfloat('bounding_box_maximum_size', 4)
        self.location_resolution = config.getint('location_resolution', 4)
        self.unused_update_tokens = UpdatesDict(self.directory_root)
        # self.config_apps = config_top['APPS'] # Not used yet as not doing app versioning in config
        self.statistics = {}
        for k in init_statistics_fields:
            self.statistics[k] = 0
        return

    def execute_route(self, name, *args):
        return registry[name](self, *args)

    def close(self):
        return

    def _insert_blob_with_optional_replacement(self, table, blob, floating_seconds, serial_number):
        table.insert(None, blob, floating_seconds, serial_number)
        ut = blob.get('update_token')
        if ut and ut in self.unused_update_tokens:
            blob_copy = copy.deepcopy(blob)  # Do not trust the insert to make a copy
            updates = self.unused_update_tokens[ut]
            # Should only ever be one, but this would actually handle multiple ones with last being dominant
            for u in updates:
                blob_copy.update(u)
            table.insert(None, blob_copy, floating_seconds, serial_number + 1)
            del self.unused_update_tokens[ut]

    # send_status POST
    # { locations: [ { min_lat,update_token,...}], contacts: [{id,update_token,...} ], memo, replaces, status, ... ]
    # Note this method is also called from server.py/get_data_from_neighbours > sync_response > sync_body
    # so do not assume this is just called by client !
    @register_method(route='/status/send')
    def send_status(self, data, args):
        logger.info('in send_status')
        # These are fields allowed in the send_status, and just copied from top level into each data point
        # Note memo is not supported yet and is a placeholder
        repeated_fields = {k: data.get(k) for k in ['memo', 'replaces', 'status'] if data.get(k)}
        return self.send_or_sync(data, repeated_fields)

    # Common part of both /status/send and
    def send_or_sync(self, data, repeated_fields):
        floating_seconds = current_time()
        serial_number = 0
        # first process contacts, then process geocode
        for contact in data.get('contact_ids', []):
            contact.update(repeated_fields)
            self._insert_blob_with_optional_replacement(self.contact_dict, contact, floating_seconds, serial_number)
            # increase by two each time to deal with potential second insert
            serial_number += 2
        for location in data.get('locations', []):
            location.update(repeated_fields)
            self._insert_blob_with_optional_replacement(self.spatial_dict, location, floating_seconds, serial_number)
            # increase by two each time to deal with potential second insert
            serial_number += 2
        return {"status": "ok"}

    def _update(self, update_token, updates, floating_time, serial_number):
        for this_dict in self.map_over_dicts():
            this_dict.update(update_token, updates, floating_time, serial_number)

    # status_update POST
    # { locations: [{min_lat,update_token,...}], contacts:[{id,update_token, ... }], memo, replaces, status, ... ]
    @register_method(route='/status/update')
    def status_update(self, data, args):
        logger.info('in status_update')
        # floating_seconds = current_time()
        length = data.get('length')  # This is how many to replace
        floating_seconds = current_time()
        serial_number = 0
        if length:
            update_tokens = data.get('update_tokens', [])
            for i in range(length):
                rt = replacement_token(data.get('replaces'), i)
                ut = get_update_token(rt)
                updates = {
                    'replaces': rt,
                    'status': data.get('status'),
                    'update_token': update_tokens[i]
                }  # SEE-OTHER-ADD-FIELDS
                # If some of the update_tokens are not found, it might be a sync issue,
                # hold the update tokens till sync comes in
                if not self._update(ut, updates, floating_seconds, serial_number):
                    self.unused_update_tokens.insert(ut, updates, floating_seconds, serial_number)
                serial_number += 1
        return {"status": "ok"}

    # scan_status post
    @register_method(route='/status/scan')
    def scan_status(self, data, args):
        since = data.get('since')
        now = current_time()
        req_locations = data.get('locations', [])
        if not self.check_bounding_box(req_locations):
            return {
                'status': 302,
                'error': "bounding boxes should be a maximum of %s sq km and specified to a resolution of %s decimal places" % (
                    self.bb_max_size, self.bb_min_dp)
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
            logger.info('contact file_paths = {file_paths}', file_paths=contact_file_paths)

            def get_contact_id_data():
                return list(self.contact_dict.retrieve_json_from_file_paths(contact_file_paths))

            ret['contact_ids'] = get_contact_id_data

        # Find any reported locations, inside the requests bounding box.
        # { locations: [ { min_lat...} ] }
        req_locations = data.get('locations')
        if req_locations:
            spatial_file_paths = []
            for bounding_box in req_locations:
                spatial_file_paths += self.spatial_dict.map_over_matching_data((bounding_box['min_lat'],
                                                                                bounding_box['min_long'],
                                                                                bounding_box['max_lat'],
                                                                                bounding_box['max_long']), since, now)

            logger.info('spatial file_paths = {file_paths}', file_paths=spatial_file_paths)

            def get_location_id_data():
                return list(self.spatial_dict.retrieve_json_from_file_paths(spatial_file_paths))

            ret['locations'] = get_location_id_data
        ret['until'] = iso_time_from_seconds_since_epoch(now)
        return ret

    # sync get
    @register_method(route='/sync')
    def sync(self, data, args):
        # Note that any replaced items will be sent as new items, so there is no need for a separate list of update_tokens.
        # Do this at the start of the process, we want to guarantee have all before this time (even if multi-threading)
        now = current_time()
        since_string = args.get('since')
        if since_string:
            since_string = since_string[0].decode()
        else:
            since_string = "1970-01-01T01:01Z"

        since = unix_time_from_iso(since_string)

        # correlate the two dictionaries
        # list of (timecode, serial_number, listL_type) between since and until
        data = sortedlist(key=lambda k: k[0])
        for the_dict in self.map_over_dicts():
            current_list = the_dict.sorted_list_by_time_and_serial_number
            data.update(map(lambda item: (item[0], item[1], the_dict), current_list[current_list.bisect_left((since, 0)):current_list.bisect_left((now, 0))]))
            
        length = len(data)
        number_to_return = int(self.config.get('MAX_SYNC_COUNT', 1000))

        # create a dict index by either contact_dict or spatial_dict
        lists_to_return = {self.contact_dict: [],
                           self.spatial_dict: []}
        # truncate the list

        truncated_data = data[0:min(length, number_to_return)]
        for datum in truncated_data:
            the_dict = datum[2]
            lists_to_return[datum[2]].append(the_dict.time_and_serial_number_to_file_path_map[(datum[0], datum[1])])
            
        contacts = lists_to_return[self.contact_dict]
        locations = lists_to_return[self.spatial_dict]
        
        ret = {'since': since_string,
               'more_data': number_to_return < length}
        if ret['more_data']:
            latest_time = data[number_to_return][0]
        else:
            latest_time = now
        ret['until'] = iso_time_from_seconds_since_epoch(latest_time)
               
        if 0 != len(contacts):
            def get_contact_id_data():
                return list(self.contact_dict.retrieve_json_from_file_paths(contacts))

            ret['contact_ids'] = get_contact_id_data
        if 0 != len(locations):
            def get_location_id_data():
                return list(self.spatial_dict.retrieve_json_from_file_paths(locations))

            ret['locations'] = get_location_id_data
        return ret

    # admin_config get
    @register_method(route='/admin/config')
    def admin_config(self, data, args):
        ret = {
            'directory': self.directory_root,
            'testing': self.testing
        }
        return ret

    # admin_status get
    @register_method(route='/admin/status')
    def admin_status(self, data, args):
        ret = {
            'bounding_box': self.spatial_dict.bounds,
            'geo_points': len(self.spatial_dict),
            'contacts_count': len(self.contact_dict)
        }
        return ret

    # init post
    @register_method(route='/init')
    def init(self, data, args):
        for k in init_statistics_fields:
            self.statistics[k] += 1
        # TODO-83
        # app_name = data.get('application_name')
        # app_current_version = self.config_apps.getfloat(app_name + "_VERSION")
        ret = {
            # "messaging_url": "", "messaging_version": 1, # TODO-84 - delayed till clients capable
            "bounding_box_minimum_dp": self.bb_min_dp,
            "bounding_box_maximum_size": self.bb_max_size,
            "location_resolution": self.location_resolution,  # ~10 meters at the equator
            "prefix_bits": 20,  # TODO-34 will need to calculate this
        }
        # if app_current_version:
        #    ret["application_current_version"] = app_current_version  # TODO-83
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
                if round(v, self.bb_min_dp) != v:
                    return False
            if (abs(bb.get('max_long') - bb.get('min_long')) * abs(
                    bb.get('max_lat') - bb.get('min_lat'))) > self.bb_max_size:
                return False
        return True

    def map_over_dicts(self):
        for the_dict in [self.contact_dict, self.spatial_dict]:
            yield the_dict
        return

    def move_expired_data_to_deletion_list(self):
        until = current_time() - self.config.getint('expire_data', 45) * 24 * 60 * 60
        for the_dict in self.map_over_dicts():
            the_dict.move_expired_data_to_deletion_list(0, until)
        return

    def delete_from_deletion_list(self):
        for the_dict in self.map_over_dicts():
            the_dict.delete_from_deletion_list()
        return
