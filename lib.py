# the module contains the client and server process to manage ids

import hashlib
import string
import random
import time
import logging
import datetime

logger = logging.getLogger(__name__)


# The hash_nonce and verify_nonce functions are a pair that may be changed as the function changes.
# verify(seed, hashupdates(seed)) == true;

def hash_nonce(seed):
    return hashlib.sha1(seed.encode()).hexdigest()


def fold_hash(hash40):
    return "%X" % (int(hash40[:20], 16) ^ int(hash40[20:], 16))


def random_ascii(length):
    # noinspection PyUnusedLocal
    return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(length)])


def new_seed(seed_string=None):
    if not seed_string:
        seed_string = random_ascii(8)
    return hash_nonce(seed_string)  # first hash_nonce is to get size same as updates


# This group of functions centralize the process and cryptography for seed -> replacement_token -> update_token

# Generate Replacement token from seed + n (which should increment)
# Requirements - none reversible, cannot be used to find the seed, or any other replacement_token
def replacement_token(seed, n):
    return hash_nonce(seed + str(n))


# Generate Update Token from Replacement Token
# Requirements: Not reversible, confirmable
# i.e. updateToken(rt) == ut shows that you possess the original rt used to create ut
def update_token(rt):
    return fold_hash(hash_nonce(rt))


# Check that the rt is a correct rt for the ut.
def confirm_update_token(ut, rt):
    return update_token(rt) == ut


# current_time is a variable that CAN be set in testing mode (useful in testing so we remove randomness and allow tests
# without sleeping)
override_time_for_testing = False

last_current_time = 0;
def current_time():
    global override_time_for_testing
    global last_current_time
    if override_time_for_testing:
        return override_time_for_testing
    else:
        t = time.time()
        if t <= last_current_time + 0.00001:
            t = t+0.00001
        last_current_time = t
        return t



def set_current_time_for_testing(floating_seconds):
    global override_time_for_testing
    override_time_for_testing = floating_seconds
    return


def unix_time_from_iso(iso_string):
    """ convert an iso 8601 time to floating seconds since epoch """
    return datetime.datetime.fromisoformat(iso_string.replace("Z", "+00:00")).timestamp()


def iso_time_from_seconds_since_epoch(seconds_since_epoch):
    return datetime.datetime.utcfromtimestamp(seconds_since_epoch).isoformat() + 'Z'
