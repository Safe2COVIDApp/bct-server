# the module contains the client and server process to manage ids

import hashlib
import string
import random
import time
import logging
import datetime
logger = logging.getLogger(__name__)

# The hash_nonce and verify_nonce functions are a pair that may be changed as the function changes.
# verify(nonce, hashupdates(nonce)) == true;

def hash_nonce(nonce):
    return hashlib.sha1(nonce.encode()).hexdigest()

def fold_hash(hash40):
    return "%X" % (int(hash40[:20], 16) ^ int(hash40[20:],16))

def random_ascii(length):
    # noinspection PyUnusedLocal
    return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(length)])

def new_nonce(seed = None):
    if not seed:
        seed = random_ascii(8)
    return hash_nonce(seed)  # first hash_nonce is to get size same as updates

# This group of functions centralize the process and cryptography for nonce -> replacement_token -> update_token

# Generate Replacement token from nonce + n (which should increment)
# Requirements - none reversible, cannot be used to find the nonce, or any other replacement_token
def replacement_token(nonce, n):
    return hash_nonce(nonce + str(n))

# Generate Update Token from Replacement Token
# Requirements: Not reversible, confirmable i.e. updateToken(rt) == ut shows that you possess the original rt used to create ut
def update_token(rt):
    return fold_hash(hash_nonce(rt))

# Check that the rt is a correct rt for the ut.
def confirm_update_token(ut, rt):
    return update_token(rt) == ut

# current_time is a variable that CAN be set in testing mode (useful in testing so we remove randomness and allow tests
# without sleeping)
override_time_for_testing = False

def current_time():
    if override_time_for_testing:
        logging
        return override_time_for_testing
    else:
        return time.time()

def set_current_time_for_testing(time):
    global override_time_for_testing
    override_time_for_testing = time
    return

def unix_time_from_iso(iso_string):
    """ connvert an iso 8601 time to floating seconds since epoch """
    return datetime.datetime.fromisoformat(iso_string.replace("Z", "+00:00")).timestamp()

def iso_time_from_seconds_since_epoch(seconds_since_epoch):
    return datetime.datetime.utcfromtimestamp(seconds_since_epoch).isoformat() + 'Z'
