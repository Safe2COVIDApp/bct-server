# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import datetime
import rtree
import string
import random

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time(dt):
    return int((dt - epoch).total_seconds())

os.umask(0o007)


logger = logging.getLogger(__name__)


# like a dict but the values can be auto extended, is if you want to set a[B][C] you don't have to initialize B apriori
class ContactDict(dict):

    def __missing__(self, key):
        self[key] = ContactDict()
        return self[key]
    
# Rtree flush isn't working, so we are overwriting
class Index(rtree.index.Index):
    def __init__(self, file_name):
        self.file_name = file_name
        super().__init__(file_name)
        return

    def flush(self):
        # slow...
        self.close()
        super().__init__(self.file_name)
        return
    

# contains both the code for the in memory and on disk version of the database
# The in memory is a four deep hash table where the leaves of the hash are:
#   list of dates (as integersfor since compares) of when contact data# has come in.


# for an id "DEADBEEF", the in memory version is stored in self.ids in the element
# self.ids['DE']['AD']['BE']["DEADBEEF']  for the disk version is is store in a four
# level directory structure rooted at config['directory'] in 'DE/AD/BE/DEADBEEF.[DATE].[PSEUDORANDOM].data'
# [DATE] is the date it gets entered in the system and [PSEUDORANDOM] is used to differential contacts with the same ID that come in at the same time
# (accuracy is to minutes).  The date strings are 'YYYYMMDDHHmm'

class Contacts:

    def __init__(self, config):
        self.directory_root = config['directory']
        self.rtree = Index('%s/rtree' % self.directory_root)
        self.testing = ('True' == config.get('testing', ''))
        self.ids = ContactDict()
        return


    def _load_ids_from_filesystem(self):
        for root, sub_dirs, files in os.walk(self.directory_root):
            for file_name in files:
                if file_name.endswith('.data'):
                    (code, date, ignore, extension) = file_name.split('.')
                    dirs = root.split('/')[-3:]
                    contact_dates = self.ids[dirs[0]][dirs[1]][dirs[2]]
                    date = int(date)
                    dates = [date]
                    if code in contact_dates:
                        dates = contact_dates[code]
                        dates.append(date)
                    self.ids[dirs[0]][dirs[1]][dirs[2]][code] = dates
        return
    
    def close(self):
        logging.info('closing rtree index file')
        self.rtree.close()
        return

    # used to start JSON_DATA at NOW for CONTACT_ID, if CONTACT_ID has other unique JSON_DATA then a new one will be stored
    def _store_id(self, contact_id, json_data, now):
        first_level, second_level, third_level = self._return_contact_keys(contact_id)
        dir_name = "%s/%s/%s/%s" % (self.directory_root, first_level, second_level, third_level)
        os.makedirs(dir_name, 0o770, exist_ok = True)

        # we add some randomness to the name so we deal with the case of the same contact_id coming in within a minute (which is
        # the resolution of now
        
        random_string = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(8)])
        file_name = '%s/%s.%s.%d.data' % (dir_name, contact_id, random_string, now)
        logger.info('writing %s to %s' % (json_data, file_name))
        with open(file_name, 'w') as file:
            json.dump(json_data, file)
        try:
            dates = self.ids[first_level][second_level][third_level].get(contact_id, [])
        except KeyError:
            dates = []
        dates.append(now)
        self.ids[first_level][second_level][third_level][contact_id] = dates
        return

    # get the three levels for both the memory and directory structure
    def _return_contact_keys(self, contact_id):
        return (contact_id[0:2].upper(), contact_id[2:4].upper(), contact_id[4:6].upper())
    

    # return all contact json contants since SINCE for CONTACT_ID
    def _get_json_blobs(self, contact_id, since = None):
        first_level, second_level, third_level = self._return_contact_keys(contact_id)
        dir_name = "%s/%s/%s/%s" % (self.directory_root, first_level, second_level, third_level)
        blobs = []
        if os.path.isdir(dir_name):
            for file_name in os.listdir(dir_name):
                if file_name.endswith('data'):
                    (code, date, ignore, extension) = file_name.split('.')
                    if code == contact_id:
                        if (not since) or (since <= int(date)):
                            blobs.append(json.load(open(('%s/%s/%s/%s/%s' % (self.directory_root, first_level, second_level, third_level, file_name)))))
        return blobs


    # send_status POST
    # { locations: [ { minLat, ...} ], contacts: [ { id, ... } ], memo, updatetoken, replaces, status, ... ]
    def send_status(self, data, args):
        logger.info('in send_statusa')
        now = int(time.time())

        repeated_fields = {}
        # These are fields allowed in the send_status, and just copied from top level into each data point
        for key in ['memo', 'updatetoken', 'replaces', 'status']:
            val = data.get(key);
            if (val):
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
            lat = float(location['lat'])
            long = float(location['long'])
            location['date'] = now
            location.update(repeated_fields)
            # make a unique id
            logger.info('inserting %s at lat: %f, long: %f' % (location, lat, long))
            self.rtree.insert(int(lat * long),  (lat, long, lat, long), obj = location)
            self.rtree.flush()
        return {"status": "ok"}

    # return all ids that match the prefix
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
    def scan_status(self, data, args):
        since = data.get('since')
        ret = {}
        if since:
            ret['since'] = since
            since = int(unix_time(datetime.datetime.strptime(since, "%Y%m%d%H%M")))
        else:
            ret['since'] = "197001010000"

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
                for obj in self.rtree.intersection((bounding_box['minLat'], bounding_box['minLong'], bounding_box['maxLat'], bounding_box['maxLong']), objects = True):
                    location = obj.object
                    if (not since) or (since <= location['date']):
                        locations.append(location)
            ret['locations'] = locations

        ret['now'] = time.strftime("%Y%m%d%H%M", time.gmtime())
        return ret

    # sync get
    def sync(self, data, args):
        since = args.get('since')
        if since:
            since = since[0].decode()
        else:
            since = "197001010000"

        since = int(unix_time(datetime.datetime.strptime(since,
                                                         "%Y%m%d%H%M")))
        contacts = []
        for key1, value1 in self.ids.items():
            for key2, value2 in self.ids[key1].items():
                for key3, value3 in self.ids[key1][key2].items():
                    for contact_id in self.ids[key1][key2][key3].keys():
                        contacts = contacts + self._get_json_blobs(contact_id, since)

        locations = []
        if 0 != self.rtree.get_size():
            for obj in self.rtree.intersection(self.rtree.bounds, objects = True):
                if (not since) or (since <= obj.object['date']):
                    locations.append(obj.object)
        
        ret = {'now':time.strftime("%Y%m%d%H%M", time.gmtime()),
               'since':since}

        if 0 != len(contacts):
            ret['contacts'] = contacts
        if 0 != len(locations):
            ret['locations'] = locations
        return ret

    # reset should only be called and allowed if testing
    def reset(self):
        if self.testing:
            logger.info('resetting ids')
            self.ids = ContactDict()
            self.rtree = Index('%s/rtree' % self.directory_root)
        return
        
