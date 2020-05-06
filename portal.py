#import logging
import os
from twisted.logger import Logger
from contacts import FSBackedThreeLevelDict
from lib import current_time, hash_seed, fold_hash


# This file is intended to be run in a separate webserver from contacts.py
# So there is no data sharing,
# BUT Its currently kicked off by the twisted server in server.py
# To allow testing on a single server, all urls start /portal_

logger = Logger()

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

# TODO-114 need to init and pass stuff to this
class PortalDict(FSBackedThreeLevelDict):

    def __init__(self, directory):
        directory = directory + '/portal_dict'
        os.makedirs(directory, 0o770, exist_ok=True)
        super().__init__(directory)

    def _insert_disk(self, key):
        logger.info('ignoring _insert_disk for PortalDict')
        return

    def _map_over_matching_contacts(self, key, ids, since, now, start_pos=0):
        logger.info('_map_over_matching_testid called with hashed id {key}')
        if start_pos < 6:
            this_prefix = key[start_pos:]
            ids = ids.get(this_prefix[0:2])
            if ids:
                yield from self._map_over_matching_contacts(key, ids, since, now, start_pos + 2)
        else:
            for floating_time in ids[key]:
                if _good_date(floating_time, since, now):
                    yield self.get_file_path_from_file_name('%s:%f.data' % (hashed_id, floating_time))
        return

    def map_over_matching_data(self, key, since, now):
        yield from self._map_over_matching_contacts(key, self.items, since, now)
        return

    def _key_string_from_blob(self, blob):
        return blob['hashed_id']

class Portal:

    def __init__(self, config):
        self.directory_root = config['directory']
        self.testing = ('True' == config.get('testing', '')) # Not used yet
        self.portal_dict = PortalDict(self.directory_root)
        return

    def execute_route(self, name, *args):
        return registry[name](self, *args)

    def close(self):
        return

    # TODO-114 should deliver base file (client.html)
    @register_method(route='/portal/user')
    def portal_user(self, data, args):
        logger.info("portal_user");
        return {"NOT":"DONE"}

    @register_method(route='/portal/register')
    def portal_register(self, data, args):
        # data: { provider, testid, replaces }
        logger.info("portal_register");
        floating_seconds_now = current_time()
        //TODO-114 move hashing to the Javascript
        hashed_id = fold_hash(hash_seed("%s:%s" % (data.get('provider'), data.get('testid'))))
        data_stored = {"hashed_id": hashed_id, "replaces": data.get('replaces')}
        self.portal_dict.insert(data_stored, floating_seconds_now)
        return { "hashed_id": hashed_id } # The client should use this for its poll

    @register_method(route='/portal/result')
    def portal_result(self, data, args):
        # data: { provider, testid, replaces }
        logger.info("portal_result");
        floating_seconds_now = current_time()
        //TODO-114 move hashing to the Javascript
        hashed_id = fold_hash(hash_seed("%s:%s" % (data.get('provider'), data.get('testid'))))
        file_paths = this.portal_dict.map_over_matching_data(hashed_id, 0, floating_seconds_now):
        data_to_further_process = self.portal_dict.retrieve_json_from_file_paths(file_paths)
        //TODO-114 need to submit this to main server 
        //TODO-114 make sure status is flowing through
        //TODO-114 think about result to client
        return data_to_further_process

