# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import datetime
import functools

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time(dt):
    return int((dt - epoch).total_seconds())

os.umask(0o007)


logger = logging.getLogger(__name__)


import pdb

# like a dict but the values can be auto extended, is if you want to set a[B][C] you don't have to initialize B apriori
class ContactDict(dict):

    def __missing__(self, key):
        self[key] = ContactDict()
        return self[key]
    

class Contacts:
    def __init__(self, config):
        self.directory_root = config['directory']
        self.testing = ('True' == config.get('testing', ''))
        self.ids = ContactDict()
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
    

    # Contacts are stored in a 4 level directory structure.  Such that for contact ABCDEFGHxxx, it is stored is AB/CD/EF/ABCDEFGHxxx.  Each contact is a file which contains JSON data.
    def _store_id(self, contact_id, json_data, now):
        first_level, second_level, third_level = self._return_contact_keys(contact_id)
        dir_name = "%s/%s/%s/%s" % (self.directory_root, first_level, second_level, third_level)
        os.makedirs(dir_name, 0o770, exist_ok = True)
        with open('%s/%s.%s.%d.data' % (dir_name, contact_id, id(json_data), now), 'w') as file:
            json.dump(json_data, file)
        try:
            dates = self.ids[first_level][second_level][third_level].get(contact_id, [])
        except KeyError:
            dates = []
        dates.append(now)
        self.ids[first_level][second_level][third_level][contact_id] = dates
        return

    def _return_contact_keys(self, contact_id):
        return (contact_id[0:2].upper(), contact_id[2:4].upper(), contact_id[4:6].upper())
    

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
                            blobs.append(json.load(open(('%s/%s/%s/%s/%s' % (self.directory_root, first_level, second_level, third_level, file_name)).upper())))
        return blobs


    def red(self, data, args):
        logger.info('in red')
        now = int(time.time())
        for contact in data['contacts']:
            contact_id = contact['id']
            if contact in self._get_json_blobs(contact_id):
                logger.info('contact for id: %s already found, not saving' % contact_id)
            else:
                self._store_id(contact_id, contact, now)
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

    def green(self, data, args):
        since = data.get('since')
        ret = {}
        if since:
            ret['since'] = since
            since = int(unix_time(datetime.datetime.strptime(since, "%Y%m%d%H%M")))
        else:
            ret['since'] = "197001010000"

        matched_ids = []
        for prefix in data['prefixes']:
            for contact in self._get_matching_contacts(prefix, self.ids):
                matched_ids = matched_ids + self._get_json_blobs(contact, since)

        ret['now'] = time.strftime("%Y%m%d%H%M", time.gmtime())
        ret['ids'] = matched_ids
        return ret

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
        ret = {'now':time.strftime("%Y%m%d%H%M", time.gmtime()),
               'since':since,
               'contacts':contacts}
        return ret

    # reset should only be called and allowed if testing
    def reset(self):
        if self.testing:
            logger.info('resetting ids')
            self.ids = ContactDict()
        return
        
