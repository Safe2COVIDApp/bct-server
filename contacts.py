# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import datetime

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time(dt):
    return int((dt - epoch).total_seconds())

os.umask(0o007)


logger = logging.getLogger(__name__)


class Contacts:
    def __init__(self, directory_root):
        self.directory_root = directory_root
        self.ids = {}
        for root, sub_dirs, files in os.walk(self.directory_root):
            for file_name in files:
                if file_name.endswith('.data'):
                    (code, date, extension) = file_name.split('.')
                    self.ids[code] = int(date)
        return
    

    # Contacts are stored in a 4 level directory structure.  Such that for contact ABCDEFGHxxx, it is stored is AB/CD/EF/ABCDEFGHxxx.  Each contact is a file which contains JSON data.
    def _store_id(self, hex_string, json_data, now):
        first_level = hex_string[0:2].upper()
        second_level = hex_string[2:4].upper()
        third_level = hex_string[4:6].upper()
        dir_name = "%s/%s/%s/%s" % (self.directory_root, first_level, second_level, third_level)
        file_name = "%s/%s.%s.data" % (dir_name, hex_string, now)
        os.makedirs(dir_name, 0o770, exist_ok = True)
        with open(file_name, 'w') as file:
            json.dump(json_data, file)
        self.ids[hex_string] = now
        return

    def red(self, data):
        logger.info('in red')
        now = int(time.time())
        for contact in data['contacts']:
            contact_id = contact['id']
            if contact_id not in self.ids:
                self._store_id(contact_id, contact, now)
            else:
                logger.info('contact id: %s already in system' % contact_id)
        return {"status": "ok"}

    def green(self, data):
        since = data.get('since')
        ret = {}
        if since:
            ret['since'] = since
            since = int(unix_time(datetime.datetime.strptime(since, "%Y%m%d%H%M")))
        else:
            ret['since'] = "197001010000"

        matched_ids = []
        for prefix in data['prefixes']:
            # this is completely the wrong datastructure, there are no buckets yet, but we'll add them in another feature
            prefix_length = len(prefix)
            for contact_id in self.ids:
                if contact_id[0:prefix_length] == prefix:
                    contact_date = self.ids[contact_id]
                    logger.debug('matched %s, date: %s' % (contact_id, self.ids[contact_id]))
                    if (not since) or (since <= contact_date):
                        matched_ids.append(contact_id)
        ret['now'] = time.strftime("%Y%m%d%H%M", time.gmtime())
        ret['ids'] = matched_ids
        return ret

